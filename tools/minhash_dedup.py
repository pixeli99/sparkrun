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
from pyspark.sql import DataFrame, SparkSession, functions as F
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


def validate_id_sample(
    self_df: DataFrame,
    self_col: str,
    other_df: DataFrame,
    other_col: str,
    self_label: str,
    other_label: str,
    sample_size: int = 4096,
    sample_fraction: float = 1e-3,
) -> None:
    """抽样校验 self_df.self_col 的值都落在 other_df.other_col 里。

    用来在 reuse checkpoint 时尽早挡掉 ID-space 错位（注意：硬不变量
    invariant 仍在 stage 5 末尾，这里只是 fast-fail）。

    注意 ``F.monotonically_increasing_id()`` 把 partition_id 放在高位、行号放
    在低位，partition 0 的 uid 是 0..n0-1。``.limit(N)`` 在 parquet 上等价于
    "拿第一个 file 的前 N 行"，全是 partition 0 的低位 uid，两个不同 run
    的 partition 0 大概率重合 → 抽样会假阳性通过。所以这里走
    ``.sample(False, fraction)``：在所有 partition 上做 Bernoulli 采样，能拿
    到高位 partition 的 uid，错位就能被采到。

    Args:
        sample_size: 实际拿来 join 的最大 distinct id 数。
        sample_fraction: Bernoulli 采样率；对 1.2B 行约采 1.2M 候选，再 limit。
    """
    t0 = time.time()
    sample = (
        self_df.select(F.col(self_col).cast("long").alias("__probe"))
               .sample(withReplacement=False, fraction=sample_fraction, seed=42)
               .distinct()
               .limit(sample_size)
    )
    sample_count = sample.count()
    if sample_count == 0:
        log.info(
            "[id-check] %s.%s sample is empty (fraction=%g); skipping uid-space "
            "check vs %s.%s",
            self_label, self_col, sample_fraction, other_label, other_col,
        )
        return
    matched = sample.join(
        other_df.select(F.col(other_col).cast("long").alias("__probe")),
        "__probe",
        "inner",
    ).count()
    missing = sample_count - matched
    log.info(
        "[id-check] %s.%s vs %s.%s in %s: sampled=%d matched=%d missing=%d",
        self_label, self_col, other_label, other_col,
        fmt_dur(time.time() - t0), sample_count, matched, missing,
    )
    if missing == 0:
        return
    raise RuntimeError(
        f"uid-space mismatch: sampled {sample_count} distinct "
        f"{self_label}.{self_col} values but {missing} of them are NOT in "
        f"{other_label}.{other_col}. This usually means {self_label}/ was "
        "written by an earlier run whose uid generation no longer matches "
        "the current exact/ checkpoint. monotonically_increasing_id() is "
        "non-deterministic across runs, so the only safe recovery is to "
        f"delete {self_label}/ and every checkpoint that depends on it "
        "(any of: edges/ wcc/ keepers/ near/ isolated/ result/) and rerun."
    )


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
        """For skip-exact mode, exact/ is a durable uid -> wide-row checkpoint.

        Must be called BEFORE any downstream stage runs or reuses — uid
        comes from `monotonically_increasing_id`, which is non-deterministic
        across runs, so every downstream stage has to pin to this
        checkpoint's uid space. The Stage 1+2 wrapper refuses to proceed
        if any downstream checkpoint already exists on disk without exact/.
        """
        if output_complete(spark, paths["exact"]):
            return
        banner(reason)
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
    #   skip 模式  -> exact/ 是 uid checkpoint，开局就 materialize 落盘
    #                 (不能 lazy 到 stage 3a/4 —— monotonically_increasing_id
    #                 跨 run 不稳定，后写的 exact/ 会跟前面写的 normalized/wcc
    #                 不在一个 uid space，stage 5 join 会大面积错配)。
    #                 如果 exact/ 缺失但下游 checkpoint 已存在，直接 RuntimeError
    #                 (没法稳定还原下游当时用的 uid，安全恢复办法是删下游重跑)。
    #   非 skip 模式 -> 写 exact/（真做了 exact dedup，行数变了，必须落盘）
    # ============================================================
    if args.skip_exact_dedup:
        spark.conf.set("spark.sql.files.ignoreCorruptFiles", "false")
        t0 = time.time()
        if output_complete(spark, paths["exact"]):
            banner("Stage 1+2: reuse exact/ uid checkpoint")
        else:
            # exact/ is the uid checkpoint that anchors every downstream
            # stage. monotonically_increasing_id() is NOT stable across
            # runs, so if any downstream checkpoint already exists we
            # cannot safely re-materialize exact/ — the new uid space
            # would silently disagree with normalized/wcc/keepers, and
            # the stage 5 joins would mostly miss (the historical
            # near_rows=48,998 vs keepers_rows=402M bug).
            stale_downstream = [
                stage for stage in (
                    "normalized", "edges", "wcc", "keepers",
                    "near", "isolated", "result",
                )
                if output_complete(spark, paths[stage])
            ]
            if stale_downstream:
                raise RuntimeError(
                    "exact/ uid checkpoint is missing but downstream "
                    f"checkpoint(s) {stale_downstream} already exist. In "
                    "skip_exact_dedup mode, exact/ anchors the uid space "
                    "used by every downstream stage; rebuilding it now "
                    "would produce a different uid space "
                    "(monotonically_increasing_id is non-deterministic "
                    "across runs). Delete the listed checkpoints and "
                    "rerun, or restore the matching exact/ from backup. "
                    f"Output root: {args.output_path}"
                )
            materialize_uid_checkpoint(
                "Stage 1+2: materialize exact/ uid checkpoint up front"
            )
        uid_df = read_uid_checkpoint()
        n_exact, n_input = uid_stats(uid_df)
        log.info("[stage 1+2] uid stats in %s", fmt_dur(time.time() - t0))
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
    if reuse_normalized:
        # normalized/ on disk may come from an earlier run whose uid
        # generation no longer lines up with the active exact/. Sample its
        # uid values against exact/.uid before anything downstream joins
        # on them — this is the check that catches the historical
        # ID-space-mismatch bug.
        validate_id_sample(
            records, "uid", uid_df, "uid",
            self_label="normalized", other_label="exact",
        )
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

    if reuse_wcc:
        # wcc/ on disk may come from an earlier run whose uid generation
        # no longer lines up with the active exact/. Sample wcc/.vid
        # against exact/.uid before stage 5 joins on uid==vid — this is
        # exactly where the historical bug surfaced (near_rows=48,998
        # vs keepers_rows=402M).
        validate_id_sample(
            wcc_df, "vid", uid_df, "uid",
            self_label="wcc", other_label="exact",
        )

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

    if reuse_keepers:
        # keepers/.keeper_uid was picked via max_by(uid) over the
        # (uid_df ⋈ wcc) inner join when keepers/ was first written. If
        # that earlier run used a different uid generation, the stage 5b
        # join uid==keeper_uid silently almost-misses — exactly the
        # historical near_rows=48,998 vs keepers_rows=402M symptom.
        validate_id_sample(
            keepers_df, "keeper_uid", uid_df, "uid",
            self_label="keepers", other_label="exact",
        )

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
    near_df = spark.read.parquet(paths["near"])

    t0 = time.time()
    s5b = near_df.agg(
        F.count(F.lit(1)).alias("n_near"),
        F.sum("duplicate_count").alias("near_sum"),
    ).collect()[0]
    n_near = int(s5b["n_near"])
    near_sum = int(s5b["near_sum"] or 0)
    log.info("[stage 5b] stats in %s", fmt_dur(time.time() - t0))
    log.info("  near rows             : %s", f"{n_near:,}")
    log.info("  near sum              : %s", f"{near_sum:,}")

    # Hard invariants — must hold whether near/ was freshly written or
    # reused. Catches ID-space drift before any "sum ≈ input" smoke test
    # accidentally lets a broken result/ through (historical near=48,998
    # vs keepers=402M bug).
    if n_near != n_clusters:
        raise RuntimeError(
            f"[stage 5b] invariant violated: near.count={n_near:,} != "
            f"keepers.count={n_clusters:,}. Every keeper_uid must resolve "
            f"to exactly one wide row in near/; a deficit means uid_df.uid "
            f"does not contain all of keepers.keeper_uid — typically an "
            f"ID-space mismatch. Inspect: tools/debug_minhash_dedup.py "
            f"--output_path {args.output_path} --quick"
        )
    if near_sum != n_in_clusters:
        raise RuntimeError(
            f"[stage 5b] invariant violated: "
            f"near.sum(duplicate_count)={near_sum:,} != "
            f"keepers.sum(duplicate_count)={n_in_clusters:,}. Cluster "
            f"weights must be preserved across the keeper_uid join."
        )

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
    isolated_df = spark.read.parquet(paths["isolated"])

    t0 = time.time()
    s5c = isolated_df.agg(
        F.count(F.lit(1)).alias("n_isolated"),
        F.sum("duplicate_count").alias("isolated_sum"),
    ).collect()[0]
    n_isolated = int(s5c["n_isolated"])
    isolated_sum = int(s5c["isolated_sum"] or 0)
    log.info("[stage 5c] stats in %s", fmt_dur(time.time() - t0))
    log.info("  isolated rows         : %s", f"{n_isolated:,}")
    log.info("  isolated sum          : %s", f"{isolated_sum:,}")

    # Hard invariants. n_wcc_nodes is wcc/.count() and equals the count of
    # distinct vids (chukonu WCC emits each vertex exactly once); so any
    # exact uid not in wcc.vid must land in isolated/. Conservation of
    # weight: each input row's __exact_cnt is in exactly one of (some
    # cluster's keeper, isolated).
    expected_isolated_rows = n_exact - n_wcc_nodes
    if n_isolated != expected_isolated_rows:
        raise RuntimeError(
            f"[stage 5c] invariant violated: isolated.count={n_isolated:,} "
            f"!= exact.count - wcc.count = "
            f"{n_exact:,} - {n_wcc_nodes:,} = {expected_isolated_rows:,}. "
            f"Every exact uid not in wcc.vid should appear in isolated/; a "
            f"discrepancy means the uid==vid left_anti dropped the wrong "
            f"rows (typical of an ID-space mismatch between exact/.uid and "
            f"wcc/.vid)."
        )
    if isolated_sum + n_in_clusters != n_input:
        raise RuntimeError(
            f"[stage 5c] weight conservation violated: "
            f"isolated_sum={isolated_sum:,} + keepers_sum={n_in_clusters:,} "
            f"= {isolated_sum + n_in_clusters:,} != input rows={n_input:,}. "
            f"Every input row's __exact_cnt must be accounted for by "
            f"exactly one of (a cluster's keeper, an isolated row)."
        )

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
        write_parquet_checkpoint(
            near_df.unionByName(isolated_df),
            "result",
            paths["result"],
            force=force_result,
        )
        log.info("[stage 5d] result/ written in %s", fmt_dur(time.time() - t0))
    result_df = spark.read.parquet(paths["result"])

    t0 = time.time()
    s5d = result_df.agg(
        F.count(F.lit(1)).alias("n_kept"),
        F.sum("duplicate_count").alias("n_sum"),
        F.max("duplicate_count").alias("max_cluster"),
        F.expr("percentile_approx(duplicate_count, 0.99)").alias("p99_cluster"),
        F.expr("percentile_approx(duplicate_count, 0.5)").alias("p50_cluster"),
    ).collect()[0]
    n_kept = int(s5d["n_kept"])
    n_sum = int(s5d["n_sum"] or 0)
    log.info("[stage 5d] stats in %s", fmt_dur(time.time() - t0))
    log.info("  result rows           : %s", f"{n_kept:,}")
    log.info("  result sum            : %s", f"{n_sum:,}")

    # Hard invariants. unionByName preserves row count and per-row weight,
    # so result must equal near + isolated exactly (both rows and sum).
    if n_kept != n_near + n_isolated:
        raise RuntimeError(
            f"[stage 5d] invariant violated: result.count={n_kept:,} != "
            f"near({n_near:,}) + isolated({n_isolated:,}) = "
            f"{n_near + n_isolated:,}."
        )
    if n_sum != near_sum + isolated_sum:
        raise RuntimeError(
            f"[stage 5d] invariant violated: "
            f"result.sum(duplicate_count)={n_sum:,} != "
            f"near.sum({near_sum:,}) + isolated.sum({isolated_sum:,}) = "
            f"{near_sum + isolated_sum:,}."
        )

    # ============================================================
    # Sanity check: 全链路总账。result.sum 必须 == 原始 input 行数。
    # 这条等式数学上已经被 stage 5c + 5d 的两条 invariant 推出来了，
    # 但保留显式校验，万一上游 stats 算错也能拦住。
    # ============================================================
    banner("Sanity check")
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
    log.info("  cluster size  p50    : %s", f"{int(s5d['p50_cluster'] or 0):,}")
    log.info("  cluster size  p99    : %s", f"{int(s5d['p99_cluster'] or 0):,}")
    log.info("  cluster size  max    : %s", f"{int(s5d['max_cluster'] or 0):,}")
    log.info("=" * 64)
    log.info("ALL DONE. total elapsed: %s", fmt_dur(time.time() - pipeline_t0))
    log.info("=" * 64)

    spark.stop()


if __name__ == "__main__":
    main()
