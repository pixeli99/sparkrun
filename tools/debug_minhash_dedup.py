"""Debug checkpoint consistency for tools/minhash_dedup.py outputs.

This script reads an existing minhash_dedup output directory and checks whether
the persisted checkpoints are mutually consistent. It is intentionally read-only.
"""

import argparse
import logging
import sys
import time
from typing import Dict, Optional

from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession, functions as F


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("debug_minhash_dedup")


def fmt_dur(secs: float) -> str:
    if secs < 60:
        return f"{secs:.1f}s"
    mins, secs = divmod(secs, 60)
    if mins < 60:
        return f"{int(mins)}m{secs:.0f}s"
    hours, mins = divmod(mins, 60)
    return f"{int(hours)}h{int(mins)}m{secs:.0f}s"


def pct(value: Optional[float]) -> str:
    if value is None:
        return "n/a"
    return f"{100.0 * value:.6f}%"


def parse_args():
    p = argparse.ArgumentParser(
        description="Debug minhash_dedup checkpoint counts and consistency."
    )
    p.add_argument("--output_path", required=True, help="Root output path from minhash_dedup.py")
    p.add_argument(
        "--weight_key",
        default="duplicate_count",
        help="Final output weight column name.",
    )
    p.add_argument(
        "--quick",
        action="store_true",
        help="Only count persisted checkpoints; skip join-based recomputation and duplicate samples.",
    )
    p.add_argument(
        "--sample_limit",
        type=int,
        default=20,
        help="Rows to print for duplicate-key samples. Use 0 to disable samples.",
    )
    return p.parse_args()


def hadoop_path(spark: SparkSession, path: str):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)


def hadoop_fs(spark: SparkSession, path: str):
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    return hadoop_path(spark, path).getFileSystem(hadoop_conf)


def output_complete(spark: SparkSession, path: str) -> bool:
    fs = hadoop_fs(spark, path)
    return fs.exists(hadoop_path(spark, path + "/_SUCCESS"))


def read_stage(spark: SparkSession, paths: Dict[str, str], stage: str) -> Optional[DataFrame]:
    path = paths[stage]
    if not output_complete(spark, path):
        log.warning("%-10s missing or incomplete: %s", stage + "/", path)
        return None
    df = spark.read.parquet(path)
    log.info("%-10s loaded: %s", stage + "/", path)
    return df


def collect_one(label: str, df: DataFrame, exprs) -> Dict[str, int]:
    t0 = time.time()
    row = df.agg(*exprs).collect()[0].asDict()
    out = {k: int(v or 0) for k, v in row.items()}
    log.info("%-34s in %s", label, fmt_dur(time.time() - t0))
    for key, value in out.items():
        log.info("  %-30s %s", key + ":", f"{value:,}")
    return out


def sum_col(df: DataFrame, col: str, alias: str):
    if col in df.columns:
        return F.sum(F.col(col).cast("long")).alias(alias)
    return F.lit(0).cast("long").alias(alias)


def show_duplicate_sample(df: DataFrame, keys, label: str, limit: int) -> None:
    if limit <= 0:
        return
    t0 = time.time()
    sample = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .orderBy(F.desc("count"))
        .limit(limit)
        .collect()
    )
    log.info("%s duplicate sample in %s", label, fmt_dur(time.time() - t0))
    if not sample:
        log.info("  no duplicate sample")
        return
    for row in sample:
        vals = ", ".join(f"{k}={row[k]}" for k in keys)
        log.info("  %s, count=%s", vals, f"{int(row['count']):,}")


def main() -> int:
    args = parse_args()
    conf = SparkConf()
    conf.set("spark.app.name", "debug_minhash_dedup")
    conf.set("spark.debug.maxToStringFields", "100")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    log.info("argv: %s", " ".join(sys.argv))
    log.info("Spark version  : %s", spark.version)
    log.info("master         : %s", sc.master)
    log.info("applicationId  : %s", sc.applicationId)
    log.info("driver web UI  : %s", sc.uiWebUrl)
    log.info("output path    : %s", args.output_path)
    log.info("quick mode     : %s", args.quick)

    paths = {
        "exact": args.output_path + "/exact",
        "normalized": args.output_path + "/normalized",
        "edges": args.output_path + "/edges",
        "wcc": args.output_path + "/wcc",
        "keepers": args.output_path + "/keepers",
        "near": args.output_path + "/near",
        "isolated": args.output_path + "/isolated",
        "result": args.output_path + "/result",
    }

    dfs = {stage: read_stage(spark, paths, stage) for stage in paths}
    exact = dfs["exact"]
    wcc = dfs["wcc"]
    keepers = dfs["keepers"]
    near = dfs["near"]
    isolated = dfs["isolated"]
    result = dfs["result"]

    stats: Dict[str, Dict[str, int]] = {}

    if exact is not None:
        stats["exact"] = collect_one(
            "exact stats",
            exact,
            [
                F.count(F.lit(1)).alias("exact_rows"),
                F.countDistinct("uid").alias("exact_distinct_uid"),
                F.min("uid").alias("exact_min_uid"),
                F.max("uid").alias("exact_max_uid"),
                sum_col(exact, "__exact_cnt", "exact_sum_exact_cnt"),
            ],
        )

    if wcc is not None:
        stats["wcc"] = collect_one(
            "wcc stats",
            wcc,
            [
                F.count(F.lit(1)).alias("wcc_rows"),
                F.countDistinct("vid").alias("wcc_distinct_vid"),
                F.countDistinct("component").alias("wcc_distinct_component"),
                F.min("vid").alias("wcc_min_vid"),
                F.max("vid").alias("wcc_max_vid"),
            ],
        )

    if keepers is not None:
        stats["keepers"] = collect_one(
            "keepers stats",
            keepers,
            [
                F.count(F.lit(1)).alias("keepers_rows"),
                F.countDistinct("component").alias("keepers_distinct_component"),
                F.countDistinct("keeper_uid").alias("keepers_distinct_keeper_uid"),
                F.min("keeper_uid").alias("keepers_min_keeper_uid"),
                F.max("keeper_uid").alias("keepers_max_keeper_uid"),
                sum_col(keepers, args.weight_key, "keepers_sum_weight"),
            ],
        )

    if near is not None:
        stats["near"] = collect_one(
            "near stats",
            near,
            [
                F.count(F.lit(1)).alias("near_rows"),
                sum_col(near, args.weight_key, "near_sum_weight"),
            ],
        )

    if isolated is not None:
        stats["isolated"] = collect_one(
            "isolated stats",
            isolated,
            [
                F.count(F.lit(1)).alias("isolated_rows"),
                sum_col(isolated, args.weight_key, "isolated_sum_weight"),
            ],
        )

    if result is not None:
        stats["result"] = collect_one(
            "result stats",
            result,
            [
                F.count(F.lit(1)).alias("result_rows"),
                sum_col(result, args.weight_key, "result_sum_weight"),
            ],
        )

    log.info("=" * 72)
    log.info("Derived checks from persisted checkpoint stats")
    log.info("=" * 72)

    exact_rows = stats.get("exact", {}).get("exact_rows")
    exact_sum = stats.get("exact", {}).get("exact_sum_exact_cnt")
    wcc_rows = stats.get("wcc", {}).get("wcc_rows")
    wcc_distinct_vid = stats.get("wcc", {}).get("wcc_distinct_vid")
    keepers_rows = stats.get("keepers", {}).get("keepers_rows")
    keepers_sum = stats.get("keepers", {}).get("keepers_sum_weight")
    near_rows = stats.get("near", {}).get("near_rows")
    near_sum = stats.get("near", {}).get("near_sum_weight")
    isolated_rows = stats.get("isolated", {}).get("isolated_rows")
    isolated_sum = stats.get("isolated", {}).get("isolated_sum_weight")
    result_rows = stats.get("result", {}).get("result_rows")
    result_sum = stats.get("result", {}).get("result_sum_weight")

    def report(label: str, observed: Optional[int], expected: Optional[int]) -> None:
        if observed is None or expected is None:
            log.info("%-36s skipped", label)
            return
        delta = observed - expected
        status = "OK" if delta == 0 else "MISMATCH"
        log.info(
            "%-36s observed=%s expected=%s delta=%s [%s]",
            label,
            f"{observed:,}",
            f"{expected:,}",
            f"{delta:,}",
            status,
        )

    report("exact distinct uid", stats.get("exact", {}).get("exact_distinct_uid"), exact_rows)
    report("wcc distinct vid", wcc_distinct_vid, wcc_rows)
    report(
        "keepers distinct component",
        stats.get("keepers", {}).get("keepers_distinct_component"),
        keepers_rows,
    )
    report(
        "keepers distinct keeper_uid",
        stats.get("keepers", {}).get("keepers_distinct_keeper_uid"),
        keepers_rows,
    )
    report("near rows vs keepers rows", near_rows, keepers_rows)
    report("near sum vs keepers sum", near_sum, keepers_sum)
    if near_rows is not None and isolated_rows is not None:
        report("result rows vs near+isolated", result_rows, near_rows + isolated_rows)
    if near_sum is not None and isolated_sum is not None:
        report("result sum vs near+isolated", result_sum, near_sum + isolated_sum)
    if wcc_distinct_vid is not None and isolated_rows is not None:
        report("exact rows vs wcc distinct+isolated", exact_rows, wcc_distinct_vid + isolated_rows)
    if exact_rows is not None and wcc_distinct_vid is not None:
        report("isolated rows vs exact-wcc", isolated_rows, exact_rows - wcc_distinct_vid)
    report("result sum vs exact sum", result_sum, exact_sum)

    if keepers_rows is not None and exact_rows is not None and wcc_distinct_vid is not None:
        expected_isolated = exact_rows - wcc_distinct_vid
        expected_kept = keepers_rows + expected_isolated
        log.info("expected isolated from exact-wcc  : %s", f"{expected_isolated:,}")
        log.info("expected kept from wcc+keepers    : %s", f"{expected_kept:,}")
        log.info("expected dedup ratio from wcc     : %s", pct(1.0 - expected_kept / exact_rows))
        if result_rows is not None:
            log.info("result row overage vs wcc expected: %s", f"{result_rows - expected_kept:,}")
    if keepers_rows is not None and isolated_rows is not None and exact_rows:
        expected_kept = keepers_rows + isolated_rows
        log.info("persisted kept from keepers+isolated: %s", f"{expected_kept:,}")
        log.info("persisted checkpoint dedup ratio   : %s", pct(1.0 - expected_kept / exact_rows))
    if result_rows is not None and exact_rows:
        log.info("actual result dedup ratio          : %s", pct(1.0 - result_rows / exact_rows))
    if result_rows is not None and keepers_rows is not None and isolated_rows is not None:
        log.info(
            "result row overage vs expected      : %s",
            f"{result_rows - keepers_rows - isolated_rows:,}",
        )
    if result_sum is not None and exact_sum is not None:
        log.info("result sum overage vs exact sum    : %s", f"{result_sum - exact_sum:,}")
    if (
        near_rows is not None
        and keepers_rows is not None
        and isolated_rows is not None
        and exact_rows is not None
        and near_rows != keepers_rows
        and isolated_rows > exact_rows * 0.9
    ):
        log.warning(
            "likely id-space mismatch: wcc/keepers ids barely join exact.uid; "
            "do not trust near/, isolated/, or result/."
        )

    if args.quick:
        return 0

    log.info("=" * 72)
    log.info("Join-based recomputation checks")
    log.info("=" * 72)

    if exact is not None and keepers is not None:
        expected_near = exact.join(
            keepers.select("keeper_uid", F.col(args.weight_key).alias("__keeper_weight")),
            F.col("uid") == F.col("keeper_uid"),
            "inner",
        )
        expected_near_stats = collect_one(
            "recomputed near from exact+keepers",
            expected_near,
            [
                F.count(F.lit(1)).alias("expected_near_rows"),
                F.sum("__keeper_weight").alias("expected_near_sum_weight"),
            ],
        )
        report(
            "near rows vs recomputed near",
            near_rows,
            expected_near_stats["expected_near_rows"],
        )
        report(
            "near sum vs recomputed near",
            near_sum,
            expected_near_stats["expected_near_sum_weight"],
        )

    if exact is not None and wcc is not None:
        expected_isolated = (
            exact.join(wcc.select("vid").distinct(), F.col("uid") == F.col("vid"), "left_anti")
            .withColumn("__isolated_weight", F.col("__exact_cnt").cast("long"))
        )
        expected_isolated_stats = collect_one(
            "recomputed isolated from exact-wcc",
            expected_isolated,
            [
                F.count(F.lit(1)).alias("expected_isolated_rows"),
                F.sum("__isolated_weight").alias("expected_isolated_sum_weight"),
            ],
        )
        report(
            "isolated rows vs recomputed",
            isolated_rows,
            expected_isolated_stats["expected_isolated_rows"],
        )
        report(
            "isolated sum vs recomputed",
            isolated_sum,
            expected_isolated_stats["expected_isolated_sum_weight"],
        )

    log.info("=" * 72)
    log.info("Duplicate-key samples")
    log.info("=" * 72)
    if exact is not None:
        show_duplicate_sample(exact, ["uid"], "exact.uid", args.sample_limit)
    if wcc is not None:
        show_duplicate_sample(wcc, ["vid"], "wcc.vid", args.sample_limit)
        show_duplicate_sample(wcc, ["vid", "component"], "wcc.(vid,component)", args.sample_limit)
    if keepers is not None:
        show_duplicate_sample(keepers, ["component"], "keepers.component", args.sample_limit)
        show_duplicate_sample(keepers, ["keeper_uid"], "keepers.keeper_uid", args.sample_limit)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
