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
import logging
import re
import string
import sys
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
from scipy.integrate import quad as integrate

SEED = 42
NON_ALPHA = re.compile(r"\W", re.UNICODE)
RNG = np.random.RandomState(SEED)
MAX_HASH = np.uint64((1 << 32) - 1)
MERSENNE_PRIME = np.uint64((1 << 61) - 1)


# ----------------------------------------------------------------------
# logging：所有阶段都打时间戳 + 关键计数；driver stdout 走 tee 进 LOG_FILE
# ----------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("minhash_dedup")


def fmt_dur(secs: float) -> str:
    if secs < 60:
        return f"{secs:.1f}s"
    m, s = divmod(secs, 60)
    if m < 60:
        return f"{int(m)}m{s:.0f}s"
    h, m = divmod(m, 60)
    return f"{int(h)}h{int(m)}m{s:.0f}s"


def banner(title: str) -> None:
    log.info("=" * 64)
    log.info(title)
    log.info("=" * 64)


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
    p.add_argument(
        "--skip_exact_dedup",
        action="store_true",
        help="跳过 stage 2 全局 exact dedup；更快但短文本完全重复不会在 exact 阶段合并",
    )
    p.add_argument(
        "--reuse_exact",
        action="store_true",
        help="如果 output_path/exact 已存在，直接复用并跳过 stage 1/2",
    )
    p.add_argument(
        "--reuse_wcc",
        action="store_true",
        help="如果 output_path/wcc 已存在，直接复用并跳过 stage 3/4",
    )
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


def struct_payload(cols):
    return F.struct(*(F.col(c).alias(c) for c in cols))


def keeper_order(text_key: str):
    return F.struct(
        F.col("__pri").alias("__pri"),
        F.length(F.col(text_key)).alias("__text_len"),
        (-F.col("uid")).alias("__neg_uid"),
    )


def keeper_order_from_len():
    return F.struct(
        F.col("__pri").alias("__pri"),
        F.col("__text_len").alias("__text_len"),
        (-F.col("uid")).alias("__neg_uid"),
    )


def path_exists(spark: SparkSession, path: str) -> bool:
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
    return fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path))


def output_complete(spark: SparkSession, path: str) -> bool:
    return path_exists(spark, path + "/_SUCCESS")


def main():
    args = parse_args()
    log.info("argv: %s", " ".join(sys.argv))
    log.info("args: %s", vars(args))

    if args.b is None or args.r is None:
        B, R = optimal_param(args.threshold, args.num_perm)
        log.info("B/R from optimal_param(threshold=%s, num_perm=%s)",
                 args.threshold, args.num_perm)
    else:
        B, R = args.b, args.r
    log.info("MinHash params : threshold=%s num_perm=%s B=%s R=%s ngram=%s min_len=%s",
             args.threshold, args.num_perm, B, R, args.ngram_size, args.min_length)

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
    sc = spark.sparkContext

    cfg = sc.getConf()
    log.info("Spark version  : %s", spark.version)
    log.info("master         : %s", sc.master)
    log.info("applicationId  : %s", sc.applicationId)
    log.info("driver web UI  : %s", sc.uiWebUrl)
    for k in [
        "spark.default.parallelism",
        "spark.sql.shuffle.partitions",
        "spark.sql.adaptive.enabled",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.executor.memoryOverhead",
        "spark.local.dir",
        "spark.plugins",
    ]:
        log.info("  %-50s = %s", k, cfg.get(k, "<unset>"))

    text_col = F.col(args.text_key)
    score_col = F.col(args.score_key)

    exact_path = args.output_path + "/exact"
    wcc_path = args.output_path + "/wcc"
    result_path = args.output_path + "/result"
    log.info("input  path    : %s", args.input_path)
    log.info("output path    : %s  (writes to {exact,wcc,result})", args.output_path)

    pipeline_t0 = time.time()

    if args.reuse_exact and output_complete(spark, exact_path):
        banner("Stage 1/2: reuse exact dedup output")
        log.info("reusing -> %s", exact_path)
    else:
        # ============================================================
        # 阶段 1: 读 parquet + 加 priority、uid
        # ============================================================
        banner("Stage 1: read parquet + build priority/uid")
        t0 = time.time()
        raw = (
            spark.read
            .option("recursiveFileLookup", "true")
            .parquet(args.input_path)
            .withColumn("__pri", build_priority(score_col, text_col))
            # 加 sha256(text) 做 shuffle key，避免完整 text 作为 partition key
            # 在 17TB 规模下产生数 TB 级的 shuffle 数据
            .withColumn("__text_h", F.sha2(text_col, 256))
            .withColumn("uid", F.monotonically_increasing_id())
        )
        log.info("input partitions : %d", raw.rdd.getNumPartitions())
        log.info("input schema:")
        for f in raw.schema.fields:
            log.info("  %-30s %s", f.name, f.dataType.simpleString())
        log.info("[stage 1] plan built in %s (lazy, no scan yet)",
                 fmt_dur(time.time() - t0))

        # ============================================================
        # 阶段 2: 文本完全相同的 group 先压成一条（priority-aware）
        # 用 sha256 hash 作 partition key（比完整 text 小几个数量级），
        # 同 hash 内：__exact_cnt 记录原始数量，选 priority 最高的留下
        # ============================================================
        banner("Stage 2: exact dedup (priority-aware, partitioned by sha256(text))")
        t0 = time.time()
        if args.skip_exact_dedup:
            log.warning("skip_exact_dedup enabled: writing one row per input doc with __exact_cnt=1")
            exact_deduped = raw.drop("__text_h").withColumn("__exact_cnt", F.lit(1))
        else:
            # Keep the shuffle narrow: only move fixed-width keys while choosing the
            # keeper uid, then join back to the wide input row after aggregation.
            exact_keeper = (
                raw
                .select(
                    "__text_h",
                    "__pri",
                    F.length(text_col).alias("__text_len"),
                    "uid",
                )
                .groupBy("__text_h")
                .agg(
                    F.count(F.lit(1)).alias("__exact_cnt"),
                    F.max_by(
                        F.struct(F.col("uid").alias("uid")),
                        keeper_order_from_len(),
                    ).alias("__keeper"),
                )
                .select(
                    F.col("__keeper.uid").alias("uid"),
                    "__exact_cnt",
                )
            )
            exact_deduped = (
                raw
                .drop("__text_h")
                .join(exact_keeper, "uid", "inner")
            )
        log.info("writing -> %s", exact_path)
        exact_deduped.write.mode("overwrite").parquet(exact_path)
        log.info("[stage 2] exact dedup written in %s", fmt_dur(time.time() - t0))

    # 读回来做统计：count() 走 parquet footer 几乎免费；
    # sum(__exact_cnt) 一次扫描就能拿到原始 input 行数（不用重读输入）
    uid_df = spark.read.parquet(exact_path)
    t0 = time.time()
    s2 = uid_df.agg(
        F.count(F.lit(1)).alias("n_exact"),
        F.sum("__exact_cnt").alias("n_input"),
    ).collect()[0]
    n_exact = int(s2["n_exact"])
    n_input = int(s2["n_input"] or 0)
    log.info("[stage 2] stats scan in %s", fmt_dur(time.time() - t0))
    log.info("  input rows         : %s", f"{n_input:,}")
    log.info("  after exact dedup  : %s", f"{n_exact:,}")
    log.info("  exact dedup ratio  : %.2f%%",
             100.0 * (1 - n_exact / max(n_input, 1)))

    if args.reuse_wcc and output_complete(spark, wcc_path):
        banner("Stage 3/4: reuse WCC output")
        log.info("reusing -> %s", wcc_path)
    else:
        # ============================================================
        # 阶段 3: chukonu minhash → 候选相似边 (src, dst)
        # ============================================================
        banner("Stage 3: chukonu minhash (LSH banding)")
        normal_udf = F.udf(normalize_text, StringType())
        records = uid_df.select(F.col("uid"), normal_udf(text_col).alias("content"))
        log.info("records partitions : %d", records.rdd.getNumPartitions())

        minhash_schema = StructType([
            StructField("src", LongType(), False),
            StructField("dst", LongType(), False),
        ])
        perm0 = ",".join(str(it) for it in PERMUTATIONS[0])
        perm1 = ",".join(str(it) for it in PERMUTATIONS[1])

        t0 = time.time()
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
        log.info("[stage 3] minhash plan built in %s "
                 "(chukonu .so loaded; compute fused into stage 4)",
                 fmt_dur(time.time() - t0))

        # ============================================================
        # 阶段 4: WCC → 每个非孤立节点 (vid, component)
        # ============================================================
        banner("Stage 4: WCC (weakly connected components)")
        wcc_schema = StructType([
            StructField("vid", LongType(), False),
            StructField("component", LongType(), False),
        ])
        t0 = time.time()
        wcc_df = invoke(
            spark,
            f"{find_chukonu_lib_path()}/wcc.so",
            [components],
            [str(args.num_parallel)],
            wcc_schema,
        )
        log.info("writing -> %s", wcc_path)
        wcc_df.write.mode("overwrite").parquet(wcc_path)
        log.info("[stage 4] minhash + WCC written in %s", fmt_dur(time.time() - t0))

    wcc_df = spark.read.parquet(wcc_path)
    t0 = time.time()
    s4 = wcc_df.agg(
        F.count(F.lit(1)).alias("n_nodes"),
        F.approx_count_distinct("component").alias("n_clusters_approx"),
    ).collect()[0]
    n_wcc_nodes = int(s4["n_nodes"])
    n_clusters_approx = int(s4["n_clusters_approx"])
    log.info("[stage 4] stats scan in %s", fmt_dur(time.time() - t0))
    log.info("  near-dup nodes        : %s", f"{n_wcc_nodes:,}")
    log.info("  clusters (approx)     : %s", f"{n_clusters_approx:,}")
    if n_clusters_approx > 0:
        log.info("  avg cluster size      : %.2f",
                 n_wcc_nodes / n_clusters_approx)
    log.info("  near-dup coverage     : %.2f%% of exact-deduped set",
             100.0 * n_wcc_nodes / max(n_exact, 1))

    # ============================================================
    # 阶段 5: cluster 内按 priority 选 keeper + 算 duplicate_count
    #   - 没在 wcc 里的 doc 是孤立点，自己一个 cluster
    #   - duplicate_count = sum(__exact_cnt) over cluster
    # ============================================================
    banner("Stage 5: pick keeper per cluster + duplicate_count")
    t0 = time.time()
    output_cols = [c for c in uid_df.columns if c not in {"__pri", "uid", "__exact_cnt"}]
    wcc_nodes = wcc_df.select("vid")
    near_docs = (
        uid_df
        .join(wcc_df, F.col("uid") == F.col("vid"), "inner")
        .drop("vid")
    )
    near_deduped = (
        near_docs
        .groupBy("component")
        .agg(
            F.sum("__exact_cnt").alias("duplicate_count"),
            F.max_by(
                struct_payload(output_cols),
                keeper_order(args.text_key),
            ).alias("__keeper"),
        )
        .select("__keeper.*", "duplicate_count")
    )
    isolated_deduped = (
        uid_df
        .join(wcc_nodes, F.col("uid") == F.col("vid"), "left_anti")
        .withColumn("duplicate_count", F.col("__exact_cnt"))
        .select(*output_cols, "duplicate_count")
    )
    deduped = near_deduped.unionByName(isolated_deduped)

    log.info("writing -> %s", result_path)
    deduped.write.mode("overwrite").parquet(result_path)
    log.info("[stage 5] result written in %s", fmt_dur(time.time() - t0))

    # ============================================================
    # 最终 sanity check：sum(duplicate_count) 必须 == 原始 input 行数
    # ============================================================
    banner("Sanity check")
    res = spark.read.parquet(result_path)
    t0 = time.time()
    s5 = res.agg(
        F.count(F.lit(1)).alias("n_kept"),
        F.sum("duplicate_count").alias("n_sum"),
        F.max("duplicate_count").alias("max_cluster"),
        F.expr("percentile_approx(duplicate_count, 0.99)").alias("p99_cluster"),
        F.expr("percentile_approx(duplicate_count, 0.5)").alias("p50_cluster"),
    ).collect()[0]
    n_kept = int(s5["n_kept"])
    n_sum = int(s5["n_sum"] or 0)
    log.info("sanity scan in %s", fmt_dur(time.time() - t0))
    log.info("  input rows           : %s", f"{n_input:,}")
    log.info("  after exact dedup    : %s", f"{n_exact:,}")
    log.info("  final kept           : %s", f"{n_kept:,}")
    log.info("  sum(dup_count)       : %s   (must equal input)", f"{n_sum:,}")
    log.info("  sum == input ?       : %s", n_sum == n_input)
    log.info("  exact   dedup ratio  : %.2f%%", 100.0 * (1 - n_exact / max(n_input, 1)))
    log.info("  minhash dedup ratio  : %.2f%%", 100.0 * (1 - n_kept / max(n_exact, 1)))
    log.info("  total   dedup ratio  : %.2f%%", 100.0 * (1 - n_kept / max(n_input, 1)))
    log.info("  cluster size  p50    : %s", f"{int(s5['p50_cluster'] or 0):,}")
    log.info("  cluster size  p99    : %s", f"{int(s5['p99_cluster'] or 0):,}")
    log.info("  cluster size  max    : %s", f"{int(s5['max_cluster'] or 0):,}")
    log.info("=" * 64)
    log.info("ALL DONE. total elapsed: %s", fmt_dur(time.time() - pipeline_t0))
    log.info("=" * 64)

    spark.stop()


if __name__ == "__main__":
    main()
