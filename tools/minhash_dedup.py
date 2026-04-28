"""MinHash near-duplicate dedup for FineWeb-style scored parquet.

Pipeline (5 阶段，每阶段都把中间结果物化到 lustre 让 planner 重新规划):

  1. read parquet（递归扫嵌套子目录）+ 加 priority + 加 uid
  2. exact dedup with priority：相同 text 的 group 内只保留 priority 最高的，
     记录原 group 大小到 __exact_cnt（后面 cluster size 要乘进去）
  3. chukonu minhash：对 normalize 后的 text 算 sha1 minhash signatures，
     LSH banding 找出 jaccard >= threshold 的候选边
  4. WCC：把候选边聚成连通分量，每条边的两个节点属于同一 cluster
  5. cluster 内按 priority 选 keeper + 算 duplicate_count，写最终 parquet

Priority 公式与 datatrove QualityPriority 等价：
    score_rank * 20000 + min(5000, len(text) // 20),  clamp [1, 65535]
其中 stage3_score = "0"/"1"/"2" → score_rank = 1/2/3，其他 0。
"""

import argparse
import hashlib
import re
import string
import time
from itertools import tee
from struct import unpack as byrunpack
from typing import List, Text, Tuple
from unicodedata import normalize as unorm

import numpy as np
from chukonu import invoke
from chukonu.config import find_chukonu_lib_path
from pyspark import SparkConf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from pyspark.sql.window import Window
from scipy.integrate import quad as integrate

SEED = 42
NON_ALPHA = re.compile(r"\W", re.UNICODE)
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)


def parse_args():
    p = argparse.ArgumentParser(description="MinHash dedup with priority-aware keeper")
    p.add_argument("--input_path", required=True)
    p.add_argument(
        "--output_path",
        required=True,
        help="输出根目录；下面会写 exact/, wcc/, result/ 三个子目录",
    )
    p.add_argument("--text_key", default="text")
    p.add_argument("--score_key", default="stage3_score")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--num_perm", type=int, default=128)
    p.add_argument("--ngram_size", type=int, default=5)
    p.add_argument("--min_length", type=int, default=2)
    p.add_argument(
        "--b",
        type=int,
        default=None,
        help="LSH bands；不传按 (threshold, num_perm) 自动求最优",
    )
    p.add_argument("--r", type=int, default=None, help="LSH rows per band；同上")
    p.add_argument("--num_parallel", type=int, default=3200, help="WCC 并行度")
    return p.parse_args()


# 优先级公式：与 datatrove QualityPriority 完全等价
def build_priority(score_col, text_col):
    score_rank = (
        F.when(score_col == "2", F.lit(3))
         .when(score_col == "1", F.lit(2))
         .when(score_col == "0", F.lit(1))
         .otherwise(F.lit(0))
    )
    length_bonus = F.least((F.length(text_col) / 20).cast("int"), F.lit(5000))
    raw = score_rank * 20000 + length_bonus
    return F.greatest(F.lit(1), F.least(F.lit(65535), raw))


# 求 MinHash LSH 的最优 (b, r)，最小化 fp + fn 加权和（datasketch 同款）
def optimal_param(threshold: float, num_perm: int):
    def fp_area(t, b, r):
        a, _ = integrate(lambda s: 1 - (1 - s ** float(r)) ** float(b), 0.0, t)
        return a

    def fn_area(t, b, r):
        a, _ = integrate(lambda s: 1 - (1 - (1 - s ** float(r)) ** float(b)), t, 1.0)
        return a

    min_err = float("inf")
    opt = (0, 0)
    for b in range(1, num_perm + 1):
        for r in range(1, num_perm // b + 1):
            err = fp_area(threshold, b, r) * 0.5 + fn_area(threshold, b, r) * 0.5
            if err < min_err:
                min_err = err
                opt = (b, r)
    return opt


# 文本归一化：lowercase + NFKC + 把空白和标点折叠成单空格
PUNC = (
    "！？｡。＂＃＄％＆＇（）＊＋，－／：；＜＝＞＠［＼］＾＿｀｛｜｝～｟｠｢｣､、〃》「」『』"
    "【】〔〕〖〗〘〙〚〛〜〝〞〟〰〾〿–—‘’‛“”„‟…‧﹏" + string.punctuation
)
_NORM_PATTERN = re.compile(r"[\s%s]+" % re.escape(PUNC))


def normalize_text(content):
    if content is None:
        return None
    return _NORM_PATTERN.sub(" ", unorm("NFKC", content.lower()))


def main():
    args = parse_args()

    if args.b is None or args.r is None:
        B, R = optimal_param(args.threshold, args.num_perm)
    else:
        B, R = args.b, args.r
    print(f"MinHash params: threshold={args.threshold} num_perm={args.num_perm} B={B} R={R}")

    PERMUTATIONS = np.array(
        [
            (
                RNG.randint(1, MERSENNE_PRIME, dtype=np.uint64),
                RNG.randint(0, MERSENNE_PRIME, dtype=np.uint64),
            )
            for _ in range(args.num_perm)
        ],
        dtype=np.uint64,
    ).T

    conf = SparkConf()
    conf.set("spark.app.name", "fineweb_minhash_dedup")
    conf.set("spark.debug.maxToStringFields", "100")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    text_col = F.col(args.text_key)
    score_col = F.col(args.score_key)

    exact_path = args.output_path + "/exact"
    wcc_path = args.output_path + "/wcc"
    result_path = args.output_path + "/result"

    # ============================================================
    # 阶段 1: 读 parquet + 加 priority、uid
    # ============================================================
    raw = (
        spark.read
        .option("recursiveFileLookup", "true")
        .parquet(args.input_path)
        .withColumn("__pri", build_priority(score_col, text_col))
        .withColumn("uid", F.monotonically_increasing_id())
    )

    # ============================================================
    # 阶段 2: 文本完全相同的 group 先压成一条（priority-aware）
    # 同 text 内：__exact_cnt 记录原始数量，选 priority 最高的留下
    # ============================================================
    same_text_w = Window.partitionBy(args.text_key)
    rank_w = same_text_w.orderBy(
        F.desc("__pri"), F.desc(F.length(text_col)), F.col("uid")
    )
    exact_deduped = (
        raw
        .withColumn("__exact_cnt", F.count("*").over(same_text_w))
        .withColumn("__exact_rk", F.row_number().over(rank_w))
        .filter(F.col("__exact_rk") == 1)
        .drop("__exact_rk")
    )
    t0 = time.time()
    exact_deduped.write.mode("overwrite").parquet(exact_path)
    print(f"[stage 2] exact dedup written in {time.time() - t0:.1f}s")

    uid_df = spark.read.parquet(exact_path)

    # ============================================================
    # 阶段 3: chukonu minhash → 候选相似边 (src, dst)
    # ============================================================
    normal_udf = F.udf(normalize_text, StringType())
    records = uid_df.select(F.col("uid"), normal_udf(text_col).alias("content"))

    minhash_schema = StructType([
        StructField("src", LongType(), False),
        StructField("dst", LongType(), False),
    ])
    perm0 = ",".join(str(it) for it in PERMUTATIONS[0])
    perm1 = ",".join(str(it) for it in PERMUTATIONS[1])

    components = invoke(
        spark,
        f"{find_chukonu_lib_path()}/libminhash_chukonu_mod.so",
        [records],
        [
            str(B),
            str(R),
            str(args.num_perm),
            str(args.ngram_size),
            str(args.min_length),
            perm0,
            perm1,
        ],
        minhash_schema,
    )

    # ============================================================
    # 阶段 4: WCC → 每个非孤立节点 (vid, component)
    # ============================================================
    wcc_schema = StructType([
        StructField("vid", LongType(), False),
        StructField("component", LongType(), False),
    ])
    t0 = time.time()
    wcc = invoke(
        spark,
        f"{find_chukonu_lib_path()}/wcc.so",
        [components],
        [str(args.num_parallel)],
        wcc_schema,
    )
    wcc.write.mode("overwrite").parquet(wcc_path)
    print(f"[stage 4] WCC written in {time.time() - t0:.1f}s")
    wcc = spark.read.parquet(wcc_path)

    # ============================================================
    # 阶段 5: cluster 内按 priority 选 keeper + 算 duplicate_count
    #   - 没在 wcc 里的 doc 是孤立点，自己一个 cluster
    #   - duplicate_count = sum(__exact_cnt) over cluster
    # ============================================================
    labeled = (
        uid_df.alias("d")
        .join(wcc.alias("w"), F.col("d.uid") == F.col("w.vid"), "left")
        .withColumn("__cluster", F.coalesce(F.col("w.component"), F.col("d.uid")))
        .drop("vid", "component")
    )

    cluster_w = Window.partitionBy("__cluster")
    keep_w = cluster_w.orderBy(
        F.desc("__pri"), F.desc(F.length(text_col)), F.col("uid")
    )

    deduped = (
        labeled
        .withColumn("duplicate_count", F.sum("__exact_cnt").over(cluster_w))
        .withColumn("__rk", F.row_number().over(keep_w))
        .filter(F.col("__rk") == 1)
        .drop("__pri", "__rk", "uid", "__cluster", "__exact_cnt")
    )

    t0 = time.time()
    deduped.write.mode("overwrite").parquet(result_path)
    print(f"[stage 5] result written in {time.time() - t0:.1f}s")

    spark.stop()


if __name__ == "__main__":
    main()
