"""MinHash near-duplicate dedup for FineWeb-style scored parquet.

每个有意义的中间产物都独立落盘，挂在哪续在哪。下一次 sbatch 同样命令再跑：
看到 <output>/<stage>/_SUCCESS 就直接跳过该阶段。

  exact/        uid + 全列 + __exact_cnt        skip 模式 = uid checkpoint
  normalized/   (uid, normalized_text)          固化 Python UDF 结果
  edges/        (src, dst)                      chukonu LSH 候选边
  wcc/          (vid, component)                weakly connected components
  keepers/      (component, keeper_uid, dup_cnt) 窄列 max_by(uid)
  near/         cluster keeper 的 wide row + duplicate_count
  isolated/     没在 cluster 里的 doc 的 wide row + duplicate_count
  result/       near ∪ isolated  ← 最终输出

Priority 公式与 datatrove QualityPriority 等价：
    score_rank * 20000 + min(5000, len(text) // 20),  clamp [1, 65535]
其中 stage3_score = "0"/"1"/"2" → score_rank = 1/2/3，其他 0。
"""

import argparse
import logging
import re
import string
import sys
import time
from unicodedata import normalize as unorm

import numpy as np
from chukonu import invoke
from chukonu.config import find_chukonu_lib_path
from pyspark import SparkConf
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType
from scipy.integrate import quad as integrate

SEED = 42
RNG = np.random.RandomState(SEED)
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
        help="输出根目录；下面会写 exact/ normalized/ edges/ wcc/ keepers/ near/ isolated/ result/",
    )
    p.add_argument("--text_key", default="text")
    p.add_argument("--score_key", default="stage3_score")
    p.add_argument("--threshold", type=float, default=0.85)
    p.add_argument("--num_perm", type=int, default=128)
    p.add_argument("--ngram_size", type=int, default=5)
    p.add_argument("--min_length", type=int, default=2)
    p.add_argument(
        "--weight_key",
        default="duplicate_count",
        help="输入已是上一轮 dedup 结果时，用该列作为每行代表的原始 doc 数；列不存在则按 1 处理",
    )
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
        help="跳过 stage 2 全局 exact dedup；后半段会把 exact/ 用作 uid checkpoint",
    )
    p.add_argument(
        "--minhash_input_partitions",
        type=int,
        default=0,
        help="Stage 3b 读 normalized/ 后最多 coalesce 到这个分区数；0 表示不处理",
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


def keeper_order_from_len():
    """priority desc, text_len desc, uid asc 的 tie-break key（窄列 max_by 用）"""
    return F.struct(
        F.col("__pri").alias("__pri"),
        F.col("__text_len").alias("__text_len"),
        (-F.col("uid")).alias("__neg_uid"),
    )


def hadoop_path(spark: SparkSession, path: str):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)


def hadoop_fs(spark: SparkSession, path: str):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    return hadoop_path(spark, path).getFileSystem(hadoop_conf)


def path_exists(spark: SparkSession, path: str) -> bool:
    return hadoop_fs(spark, path).exists(hadoop_path(spark, path))


def output_complete(spark: SparkSession, path: str) -> bool:
    return path_exists(spark, path + "/_SUCCESS")


def delete_path(spark: SparkSession, path: str) -> None:
    if path_exists(spark, path):
        fs = hadoop_fs(spark, path)
        if not fs.delete(hadoop_path(spark, path), True):
            raise RuntimeError(f"failed to delete path: {path}")


def mkdirs_path(spark: SparkSession, path: str) -> None:
    if path_exists(spark, path):
        return
    fs = hadoop_fs(spark, path)
    if not fs.mkdirs(hadoop_path(spark, path)):
        raise RuntimeError(f"failed to create path: {path}")


def rename_path(spark: SparkSession, src: str, dst: str) -> None:
    fs = hadoop_fs(spark, src)
    if not fs.rename(hadoop_path(spark, src), hadoop_path(spark, dst)):
        raise RuntimeError(f"failed to rename checkpoint {src} -> {dst}")


def row_weight(df, args):
    if args.weight_key and args.weight_key in df.columns:
        return F.coalesce(F.col(args.weight_key).cast("long"), F.lit(1).cast("long"))
    return F.lit(1).cast("long")


def split_input_paths(input_path: str):
    return [p.strip() for p in input_path.split(",") if p.strip()]


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

    # 落盘点；每个阶段开始前 check _SUCCESS 自动 reuse，挂哪续哪。
    # skip 模式下 exact/ 是 uid checkpoint，不再执行全局 exact dedup。
    paths = {
        "exact":      args.output_path + "/exact",       # 非 skip：exact dedup；skip：uid checkpoint
        "normalized": args.output_path + "/normalized",  # (uid, normalized_text) ← 把 Python UDF 固化
        "edges":      args.output_path + "/edges",       # (src, dst) chukonu LSH 候选边
        "wcc":        args.output_path + "/wcc",         # (vid, component)
        "keepers":    args.output_path + "/keepers",     # (component, keeper_uid, duplicate_count)
        "near":       args.output_path + "/near",        # cluster keeper 的 wide row + duplicate_count
        "isolated":   args.output_path + "/isolated",    # 孤立 doc 的 wide row + duplicate_count
        "result":     args.output_path + "/result",      # near ∪ isolated（最终输出）
    }
    input_paths = split_input_paths(args.input_path)
    log.info("input  path(s) : %s", input_paths)
    log.info("output path    : %s", args.output_path)
    log.info("checkpoints (auto-reuse on _SUCCESS):")
    for k, p in paths.items():
        mark = "[reuse]" if output_complete(spark, p) else "[build]"
        label = "exact(uid checkpoint)" if k == "exact" and args.skip_exact_dedup else k
        log.info("  %s %-21s -> %s", mark, label, p)

    pipeline_t0 = time.time()
    rebuilt_stages = set()

    def raw_input_df():
        return (
            spark.read
            .option("ignoreCorruptFiles", "false")
            .option("recursiveFileLookup", "true")
            .option("pathGlobFilter", "*.parquet")
            .parquet(*input_paths)
        )

    def add_uid_columns(df):
        return (
            df
            .withColumn("__pri", build_priority(score_col, text_col))
            .withColumn("uid", F.monotonically_increasing_id())
            .withColumn("__exact_cnt", row_weight(df, args))
        )

    def log_input_shape(df):
        log.info("input partitions : %d", df.rdd.getNumPartitions())
        log.info("input schema:")
        for f in df.schema.fields:
            log.info("  %-30s %s", f.name, f.dataType.simpleString())

    def cleanup_stage_staging(stage: str) -> None:
        staging_root = args.output_path + "/_staging"
        if not path_exists(spark, staging_root):
            return
        fs = hadoop_fs(spark, staging_root)
        prefix = stage + "-"
        for status in fs.listStatus(hadoop_path(spark, staging_root)):
            name = status.getPath().getName()
            if name.startswith(prefix):
                stale_path = status.getPath().toString()
                log.warning("[checkpoint] deleting stale staging dir: %s", stale_path)
                if not fs.delete(status.getPath(), True):
                    raise RuntimeError(f"failed to delete stale staging dir: {stale_path}")

    def checkpoint_reuse(stage: str, path: str, upstreams=()):
        stale_upstreams = [s for s in upstreams if s in rebuilt_stages]
        if output_complete(spark, path) and not stale_upstreams:
            return True, False
        if output_complete(spark, path) and stale_upstreams:
            log.warning(
                "[checkpoint] %s/ is complete but upstream rebuilt in this run (%s); "
                "will rebuild after staging a fresh copy",
                stage,
                ", ".join(stale_upstreams),
            )
            return False, True
        return False, False

    def write_parquet_checkpoint(df, stage: str, path: str, force: bool = False) -> None:
        """Write to output/_staging first, then commit the complete directory."""
        if output_complete(spark, path) and not force:
            log.info("[checkpoint] reuse complete %s/: %s", stage, path)
            return

        cleanup_stage_staging(stage)
        app_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", sc.applicationId or "no-app")
        ts_ms = int(time.time() * 1000)
        tmp_path = f"{args.output_path}/_staging/{stage}-{app_id}-{ts_ms}"

        delete_path(spark, tmp_path)
        log.info("[checkpoint] writing %s/ via staging: %s", stage, tmp_path)
        df.write.mode("overwrite").parquet(tmp_path)
        if not output_complete(spark, tmp_path):
            raise RuntimeError(f"staging checkpoint missing _SUCCESS: {tmp_path}")

        if output_complete(spark, path) and not force:
            log.warning(
                "[checkpoint] final path became complete while staging %s; deleting temp %s",
                path,
                tmp_path,
            )
            delete_path(spark, tmp_path)
            return

        backup_path = None
        if path_exists(spark, path):
            if output_complete(spark, path):
                backup_root = args.output_path + "/_backup"
                mkdirs_path(spark, backup_root)
                backup_path = f"{backup_root}/{stage}-{app_id}-{ts_ms}"
                delete_path(spark, backup_path)
                log.warning(
                    "[checkpoint] moving existing complete %s/ to backup before commit: %s",
                    stage,
                    backup_path,
                )
                rename_path(spark, path, backup_path)
            else:
                log.warning("[checkpoint] deleting incomplete final path before commit: %s", path)
                delete_path(spark, path)

        try:
            rename_path(spark, tmp_path, path)
            if not output_complete(spark, path):
                if backup_path and path_exists(spark, backup_path):
                    delete_path(spark, path)
                    rename_path(spark, backup_path, path)
                raise RuntimeError(f"committed checkpoint missing _SUCCESS: {path}")
        except Exception:
            if backup_path and not path_exists(spark, path) and path_exists(spark, backup_path):
                log.error("[checkpoint] commit failed; restoring backup for %s/: %s", stage, path)
                rename_path(spark, backup_path, path)
            raise

        if backup_path and path_exists(spark, backup_path):
            delete_path(spark, backup_path)
        rebuilt_stages.add(stage)
        log.info("[checkpoint] committed %s/: %s", stage, path)

    def materialize_uid_checkpoint(reason: str):
        """For skip-exact mode, exact/ is a durable uid -> wide-row checkpoint."""
        if output_complete(spark, paths["exact"]):
            return
        banner(reason)
        if output_complete(spark, paths["normalized"]):
            log.warning(
                "normalized/ already exists but exact/ uid checkpoint does not; "
                "this run assumes the input files and Spark file partitioning are unchanged "
                "from the run that produced normalized/."
            )
        t0 = time.time()
        raw = raw_input_df()
        uid_checkpoint = add_uid_columns(raw)
        log_input_shape(uid_checkpoint)
        write_parquet_checkpoint(uid_checkpoint, "exact", paths["exact"])
        log.info("[stage 1+2] uid checkpoint exact/ written in %s",
                 fmt_dur(time.time() - t0))

    def read_uid_checkpoint():
        return spark.read.parquet(paths["exact"])

    def uid_stats(df):
        row = df.agg(
            F.count(F.lit(1)).alias("n_exact"),
            F.sum("__exact_cnt").alias("n_input"),
        ).collect()[0]
        return int(row["n_exact"]), int(row["n_input"] or 0)

    # ============================================================
    # Stage 1+2:
    #   skip 模式  -> 优先复用 exact/ 作为 uid checkpoint；如果 exact/ 不存在，
    #                 前半段可继续用 lazy(raw + uid)，后半段需要 wide row 前会落盘。
    #                 uid 稳定性依赖 ignoreCorruptFiles=false + 静态输入；
    #                 file listing 顺序 + FilePartition 划分跨 stage 一致就稳。
    #   非 skip 模式 -> 写 exact/（真做了 exact dedup，行数变了，必须落盘）
    # ============================================================
    normalized_rows = None
    if args.skip_exact_dedup:
        spark.conf.set("spark.sql.files.ignoreCorruptFiles", "false")
        t0 = time.time()
        if output_complete(spark, paths["exact"]):
            banner("Stage 1+2: reuse exact/ uid checkpoint")
            uid_df = read_uid_checkpoint()
            n_exact, n_input = uid_stats(uid_df)
            log.info("[stage 1+2] checkpoint stats in %s", fmt_dur(time.time() - t0))
        else:
            banner("Stage 1+2: skip exact dedup, uid_df 先用 lazy raw")
            raw = raw_input_df()
            uid_df = add_uid_columns(raw)
            log_input_shape(uid_df)
            if output_complete(spark, paths["normalized"]):
                normalized_rows = spark.read.parquet(paths["normalized"]).count()
                n_exact = normalized_rows
                n_input = normalized_rows
                if args.weight_key and args.weight_key in raw.schema.names:
                    log.info(
                        "  %s exists in input; weighted input rows will be recomputed "
                        "after exact/ uid checkpoint is materialized",
                        args.weight_key,
                    )
                log.info("[stage 1+2] lazy plan + normalized count in %s",
                         fmt_dur(time.time() - t0))
            else:
                n_exact, n_input = uid_stats(uid_df)
                log.info("[stage 1+2] lazy plan + raw stats in %s",
                         fmt_dur(time.time() - t0))
        log.info("  input rows         : %s   (= weighted rows if %s exists)",
                 f"{n_input:,}", args.weight_key)
        log.info("  rows entering LSH  : %s   (exact dedup skipped here)", f"{n_exact:,}")
    else:
        if output_complete(spark, paths["exact"]):
            banner("Stage 1+2: reuse exact/")
        else:
            banner("Stage 1+2: build exact/  (global exact dedup)")
            t0 = time.time()
            raw0 = raw_input_df()
            raw = (
                add_uid_columns(raw0)
                .withColumn("__text_h", F.sha2(text_col, 256))
            )
            log_input_shape(raw)
            # 窄列 shuffle：只搬 (sha256, pri, text_len, uid) 选出 keeper uid，
            # 再 join 回 wide row 写盘
            exact_keeper = (
                raw.select(
                    "__text_h",
                    "__pri",
                    F.length(text_col).alias("__text_len"),
                    "uid",
                    "__exact_cnt",
                )
                .groupBy("__text_h")
                .agg(
                    F.sum("__exact_cnt").alias("__exact_cnt"),
                    F.max_by(
                        F.struct(F.col("uid").alias("uid")),
                        keeper_order_from_len(),
                    ).alias("__keeper"),
                )
                .select(F.col("__keeper.uid").alias("uid"), "__exact_cnt")
            )
            write_parquet_checkpoint(
                raw.drop("__text_h", "__exact_cnt").join(exact_keeper, "uid", "inner"),
                "exact",
                paths["exact"],
            )
            log.info("[stage 1+2] exact/ written in %s", fmt_dur(time.time() - t0))
        uid_df = spark.read.parquet(paths["exact"])

        t0 = time.time()
        s12 = uid_df.agg(
            F.count(F.lit(1)).alias("n_exact"),
            F.sum("__exact_cnt").alias("n_input"),
        ).collect()[0]
        n_exact = int(s12["n_exact"])
        n_input = int(s12["n_input"] or 0)
        log.info("[stage 1+2] stats in %s", fmt_dur(time.time() - t0))
        log.info("  input rows         : %s", f"{n_input:,}")
        log.info("  after exact dedup  : %s", f"{n_exact:,}")
        log.info("  exact dedup ratio  : %.2f%%",
                 100.0 * (1 - n_exact / max(n_input, 1)))

    # ============================================================
    # Stage 3a: normalized/  (uid, normalized_text)
    #   把 Python UDF 的归一化结果固化，stage 3b chukonu 重跑不再吃一次 UDF
    # ============================================================
    reuse_normalized, force_normalized = checkpoint_reuse(
        "normalized",
        paths["normalized"],
        upstreams=("exact",),
    )
    if reuse_normalized:
        banner("Stage 3a: reuse normalized/")
    else:
        if args.skip_exact_dedup and not output_complete(spark, paths["exact"]):
            materialize_uid_checkpoint("Stage 1+2: materialize exact/ uid checkpoint before normalized/")
            uid_df = read_uid_checkpoint()
            n_exact, n_input = uid_stats(uid_df)
            if normalized_rows is not None and n_exact != normalized_rows:
                raise RuntimeError(
                    f"uid checkpoint row count {n_exact:,} does not match "
                    f"existing normalized/ row count {normalized_rows:,}"
                )
            log.info("  input rows         : %s   (= weighted rows if %s exists)",
                     f"{n_input:,}", args.weight_key)
            log.info("  rows entering LSH  : %s   (exact dedup skipped here)", f"{n_exact:,}")
        banner("Stage 3a: normalize text -> normalized/")
        t0 = time.time()
        normal_udf = F.udf(normalize_text, StringType())
        write_parquet_checkpoint(
            uid_df.select(F.col("uid"), normal_udf(text_col).alias("content")),
            "normalized",
            paths["normalized"],
            force=force_normalized,
        )
        log.info("[stage 3a] normalized/ written in %s", fmt_dur(time.time() - t0))
    records = spark.read.parquet(paths["normalized"])
    if args.skip_exact_dedup and output_complete(spark, paths["exact"]):
        if normalized_rows is None:
            normalized_rows = records.count()
        if n_exact != normalized_rows:
            raise RuntimeError(
                f"exact/ uid checkpoint row count {n_exact:,} does not match "
                f"normalized/ row count {normalized_rows:,}; remove exact/ or the "
                "affected downstream checkpoints and rerun with unchanged input."
            )
        log.info("  exact/ and normalized/ row counts match: %s", f"{n_exact:,}")
    records_partitions = records.rdd.getNumPartitions()
    if args.minhash_input_partitions and records_partitions > args.minhash_input_partitions:
        log.info(
            "coalesce normalized/ for Stage 3b: %s -> %s partitions",
            f"{records_partitions:,}",
            f"{args.minhash_input_partitions:,}",
        )
        records = records.coalesce(args.minhash_input_partitions)
    else:
        log.info("Stage 3b input partitions: %s", f"{records_partitions:,}")

    # ============================================================
    # Stage 3b: edges/  ——  chukonu minhash LSH 候选边 (src, dst)
    #   从 stage 4 fuse 里拆出来单独落盘，minhash 挂了不连累 wcc，反之亦然
    # ============================================================
    reuse_edges, force_edges = checkpoint_reuse(
        "edges",
        paths["edges"],
        upstreams=("normalized",),
    )
    if reuse_edges:
        banner("Stage 3b: reuse edges/")
    else:
        banner("Stage 3b: chukonu minhash -> edges/")
        t0 = time.time()
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
        write_parquet_checkpoint(components, "edges", paths["edges"], force=force_edges)
        log.info("[stage 3b] edges/ written in %s", fmt_dur(time.time() - t0))
    edges_df = spark.read.parquet(paths["edges"])

    t0 = time.time()
    n_edges = edges_df.count()
    log.info("[stage 3b] stats in %s", fmt_dur(time.time() - t0))
    log.info("  candidate edges    : %s", f"{n_edges:,}")

    # ============================================================
    # Stage 4: wcc/  ——  WCC 聚 cluster (vid, component)
    # ============================================================
    reuse_wcc, force_wcc = checkpoint_reuse(
        "wcc",
        paths["wcc"],
        upstreams=("edges",),
    )
    if reuse_wcc:
        banner("Stage 4: reuse wcc/")
    else:
        banner("Stage 4: chukonu WCC -> wcc/")
        t0 = time.time()
        wcc_schema = StructType([
            StructField("vid", LongType(), False),
            StructField("component", LongType(), False),
        ])
        wcc = invoke(
            spark,
            f"{find_chukonu_lib_path()}/wcc.so",
            [edges_df],
            [str(args.num_parallel)],
            wcc_schema,
        )
        write_parquet_checkpoint(wcc, "wcc", paths["wcc"], force=force_wcc)
        log.info("[stage 4] wcc/ written in %s", fmt_dur(time.time() - t0))
    wcc_df = spark.read.parquet(paths["wcc"])

    t0 = time.time()
    s4 = wcc_df.agg(
        F.count(F.lit(1)).alias("n_nodes"),
        F.approx_count_distinct("component").alias("n_clusters_approx"),
    ).collect()[0]
    n_wcc_nodes = int(s4["n_nodes"])
    n_clusters_approx = int(s4["n_clusters_approx"])
    log.info("[stage 4] stats in %s", fmt_dur(time.time() - t0))
    log.info("  near-dup nodes        : %s", f"{n_wcc_nodes:,}")
    log.info("  clusters (approx)     : %s", f"{n_clusters_approx:,}")
    if n_clusters_approx > 0:
        log.info("  avg cluster size      : %.2f",
                 n_wcc_nodes / n_clusters_approx)
    log.info("  near-dup coverage     : %.2f%% of exact-deduped set",
             100.0 * n_wcc_nodes / max(n_exact, 1))

    if args.skip_exact_dedup and not output_complete(spark, paths["exact"]):
        materialize_uid_checkpoint("Stage 1+2: materialize exact/ uid checkpoint for wide-row stages")
        uid_df = read_uid_checkpoint()
        n_exact, n_input = uid_stats(uid_df)
        if normalized_rows is not None and n_exact != normalized_rows:
            raise RuntimeError(
                f"uid checkpoint row count {n_exact:,} does not match "
                f"existing normalized/ row count {normalized_rows:,}"
            )
        log.info("  input rows         : %s   (= weighted rows if %s exists)",
                 f"{n_input:,}", args.weight_key)
        log.info("  rows entering LSH  : %s   (exact dedup skipped here)", f"{n_exact:,}")

    # ============================================================
    # Stage 5a: keepers/  (component, keeper_uid, duplicate_count)
    #   只搬窄列 (uid, __pri, text_len, __exact_cnt) 进 max_by(uid)；
    #   不再把 wide row struct 灌进 partial aggregation buffer，避免 OOM。
    # ============================================================
    reuse_keepers, force_keepers = checkpoint_reuse(
        "keepers",
        paths["keepers"],
        upstreams=("exact", "wcc"),
    )
    if reuse_keepers:
        banner("Stage 5a: reuse keepers/")
    else:
        banner("Stage 5a: pick keeper_uid per cluster -> keepers/")
        t0 = time.time()
        keepers = (
            uid_df.select(
                "uid",
                "__pri",
                F.length(text_col).alias("__text_len"),
                "__exact_cnt",
            )
            .join(wcc_df, F.col("uid") == F.col("vid"), "inner")
            .groupBy("component")
            .agg(
                F.sum("__exact_cnt").alias("duplicate_count"),
                F.max_by(F.col("uid"), keeper_order_from_len()).alias("keeper_uid"),
            )
        )
        write_parquet_checkpoint(keepers, "keepers", paths["keepers"], force=force_keepers)
        log.info("[stage 5a] keepers/ written in %s", fmt_dur(time.time() - t0))
    keepers_df = spark.read.parquet(paths["keepers"])

    t0 = time.time()
    s5a = keepers_df.agg(
        F.count(F.lit(1)).alias("n_clusters"),
        F.sum("duplicate_count").alias("n_in_clusters"),
    ).collect()[0]
    n_clusters = int(s5a["n_clusters"])
    n_in_clusters = int(s5a["n_in_clusters"] or 0)
    log.info("[stage 5a] stats in %s", fmt_dur(time.time() - t0))
    log.info("  clusters              : %s", f"{n_clusters:,}")
    log.info("  exact-cnt in clusters : %s", f"{n_in_clusters:,}")

    internal_cols = {"__pri", "uid", "__exact_cnt"}
    if args.weight_key:
        internal_cols.add(args.weight_key)
    output_cols = [c for c in uid_df.columns if c not in internal_cols]

    # ============================================================
    # Stage 5b: near/  ——  把 keeper_uid join 回 wide row
    # ============================================================
    reuse_near, force_near = checkpoint_reuse(
        "near",
        paths["near"],
        upstreams=("exact", "keepers"),
    )
    if reuse_near:
        banner("Stage 5b: reuse near/")
    else:
        banner("Stage 5b: keeper wide row -> near/")
        t0 = time.time()
        near_deduped = (
            uid_df.join(keepers_df, F.col("uid") == F.col("keeper_uid"), "inner")
                  .select(*output_cols, "duplicate_count")
        )
        write_parquet_checkpoint(near_deduped, "near", paths["near"], force=force_near)
        log.info("[stage 5b] near/ written in %s", fmt_dur(time.time() - t0))

    # ============================================================
    # Stage 5c: isolated/  ——  没在任何 cluster 里的 doc，wide row
    # ============================================================
    reuse_isolated, force_isolated = checkpoint_reuse(
        "isolated",
        paths["isolated"],
        upstreams=("exact", "wcc"),
    )
    if reuse_isolated:
        banner("Stage 5c: reuse isolated/")
    else:
        banner("Stage 5c: isolated docs -> isolated/")
        t0 = time.time()
        isolated_deduped = (
            uid_df.join(wcc_df.select("vid"), F.col("uid") == F.col("vid"), "left_anti")
                  .withColumn("duplicate_count", F.col("__exact_cnt"))
                  .select(*output_cols, "duplicate_count")
        )
        write_parquet_checkpoint(
            isolated_deduped,
            "isolated",
            paths["isolated"],
            force=force_isolated,
        )
        log.info("[stage 5c] isolated/ written in %s", fmt_dur(time.time() - t0))

    # ============================================================
    # Stage 5d: result/ = near/ ∪ isolated/
    # ============================================================
    reuse_result, force_result = checkpoint_reuse(
        "result",
        paths["result"],
        upstreams=("near", "isolated"),
    )
    if reuse_result:
        banner("Stage 5d: reuse result/")
    else:
        banner("Stage 5d: union near + isolated -> result/")
        t0 = time.time()
        near = spark.read.parquet(paths["near"])
        isolated = spark.read.parquet(paths["isolated"])
        write_parquet_checkpoint(
            near.unionByName(isolated),
            "result",
            paths["result"],
            force=force_result,
        )
        log.info("[stage 5d] result/ written in %s", fmt_dur(time.time() - t0))

    # ============================================================
    # Sanity check: sum(duplicate_count) 必须 == 原始 input 行数
    # ============================================================
    banner("Sanity check")
    res = spark.read.parquet(paths["result"])
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
    if n_sum != n_input:
        raise RuntimeError(
            f"sanity check failed: sum(duplicate_count)={n_sum:,} "
            f"does not equal input rows={n_input:,}"
        )
    log.info("  exact   dedup ratio  : %.2f%%",
             100.0 * (1 - n_exact / max(n_input, 1)))
    log.info("  minhash dedup ratio  : %.2f%%",
             100.0 * (1 - n_kept / max(n_exact, 1)))
    log.info("  total   dedup ratio  : %.2f%%",
             100.0 * (1 - n_kept / max(n_input, 1)))
    log.info("  cluster size  p50    : %s", f"{int(s5['p50_cluster'] or 0):,}")
    log.info("  cluster size  p99    : %s", f"{int(s5['p99_cluster'] or 0):,}")
    log.info("  cluster size  max    : %s", f"{int(s5['max_cluster'] or 0):,}")
    log.info("=" * 64)
    log.info("ALL DONE. total elapsed: %s", fmt_dur(time.time() - pipeline_t0))
    log.info("=" * 64)

    spark.stop()


if __name__ == "__main__":
    main()
