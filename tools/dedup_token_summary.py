"""Summarize or export remaining dedup rows by year with Spark.

Input layout:
  <base>/<dataset>/year_YYYY/*.parquet

Filter output layout:
  <filtered_output>/<dataset>/year_YYYY/*.parquet

Rows whose score casts to one of --keep-scores are kept. The default keeps
stage3_score values 1 and 2, which filters out all score-0 cases.
"""

import argparse
import csv
import glob
import os
import re
from pathlib import Path

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession


def parse_args():
    parser = argparse.ArgumentParser(description="Summarize or export dedup rows by year.")
    parser.add_argument(
        "--base",
        default="/lustre/projects/polyullm/lipengxiang_tmp/minhash_simple/dedup",
        help="Dedup base directory containing <dataset>/year_YYYY outputs.",
    )
    parser.add_argument(
        "--mode",
        choices=("summary", "filter", "both"),
        default="filter",
        help="summary writes the TSV only; filter writes kept rows only; both does both.",
    )
    parser.add_argument(
        "--output",
        default="/lustre/projects/polyullm/lipengxiang_tmp/dedup_tokens_by_year_spark.tsv",
        help="Output TSV path written by the Spark driver.",
    )
    parser.add_argument(
        "--filtered-output",
        default="/work/projects/polyullm/infra/sync_to_b3/0518",
        help="Output root for score-filtered parquet, preserving <dataset>/year_YYYY layout.",
    )
    parser.add_argument(
        "--filtered-output-mode",
        choices=("error", "overwrite", "append", "ignore"),
        default="overwrite",
        help="Spark write mode for each filtered <dataset>/year_YYYY directory.",
    )
    parser.add_argument(
        "--keep-scores",
        default="1,2",
        help="Comma-separated score values to keep after casting the score column to double.",
    )
    parser.add_argument("--token-key", default="token_count")
    parser.add_argument("--score-key", default="stage3_score")
    return parser.parse_args()


def parse_keep_scores(scores: str):
    values = []
    for raw_score in scores.split(","):
        raw_score = raw_score.strip()
        if not raw_score:
            continue
        values.append(float(raw_score))
    if not values:
        raise ValueError("--keep-scores must include at least one numeric score")
    return values


def discover_year_paths(base: str):
    pattern = os.path.join(base.rstrip("/"), "*", "year_[0-9][0-9][0-9][0-9]")
    return sorted(path for path in glob.glob(pattern) if os.path.isdir(path))


def parse_dataset_year(path: str):
    match = re.search(r"/([^/]+)/year_([0-9]{4})$", path.rstrip("/"))
    if not match:
        raise ValueError(f"Could not parse <dataset>/year_YYYY from path: {path}")
    return match.group(1), match.group(2)


def count_parquet_files(year_paths):
    counts = {}
    for path in year_paths:
        dataset, year = parse_dataset_year(path)
        files = glob.glob(os.path.join(path, "*.parquet"))
        counts[(dataset, year)] = len(files)
    return counts


def init_spark(app_name: str):
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def keep_score_filter(score_key: str, keep_scores):
    score = F.col(score_key).cast("double")
    return score.isNotNull() & (~F.isnan(score)) & score.isin(keep_scores)


def write_summary(spark, args, year_paths, keep_scores):
    file_counts = count_parquet_files(year_paths)
    df = spark.read.parquet(*year_paths)
    required = {"meta", args.score_key}
    missing = sorted(required - set(df.columns))
    if missing:
        raise ValueError(f"Missing required columns: {missing}; available columns: {df.columns}")

    file_name = F.input_file_name()
    score = F.col(args.score_key).cast("double")
    token_count = F.col("meta.others").getItem(args.token_key).cast("long")

    prepared = df.select(
        F.regexp_extract(file_name, r"/([^/]+)/year_[0-9]{4}/", 1).alias("dataset"),
        F.regexp_extract(file_name, r"/year_([0-9]{4})/", 1).alias("year"),
        score.alias("score"),
        token_count.alias("token_count"),
    )
    valid_score = F.col("score").isNotNull() & (~F.isnan(F.col("score")))
    keep_row = valid_score & F.col("score").isin(keep_scores)

    summary = (
        prepared.groupBy("dataset", "year")
        .agg(
            F.count(F.lit(1)).alias("total_documents"),
            F.sum(F.when(valid_score & (F.col("score") == F.lit(0.0)), 1).otherwise(0)).alias(
                "score_zero_rows"
            ),
            F.sum(F.when(~valid_score, 1).otherwise(0)).alias(
                "missing_or_bad_score_rows"
            ),
            F.sum(
                F.when(valid_score & (~F.col("score").isin(keep_scores)), 1).otherwise(0)
            ).alias("dropped_score_rows"),
            F.sum(F.when(keep_row, 1).otherwise(0)).alias("kept_documents"),
            F.sum(
                F.when(
                    keep_row,
                    F.coalesce(F.col("token_count"), F.lit(0)),
                ).otherwise(0)
            ).alias("tokens"),
            F.sum(
                F.when(
                    keep_row & F.col("token_count").isNull(),
                    1,
                ).otherwise(0)
            ).alias("missing_token_rows"),
        )
        .withColumn("tokens_billion", F.col("tokens") / F.lit(1_000_000_000.0))
        .orderBy("dataset", "year")
    )

    rows = [row.asDict() for row in summary.collect()]

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "dataset",
        "year",
        "parquet_files",
        "total_documents",
        "kept_documents",
        "tokens",
        "tokens_billion",
        "score_zero_rows",
        "dropped_score_rows",
        "missing_or_bad_score_rows",
        "missing_token_rows",
    ]
    with output.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter="\t")
        writer.writeheader()
        for row in rows:
            row["parquet_files"] = file_counts.get((row["dataset"], row["year"]), 0)
            row["tokens_billion"] = f"{float(row['tokens_billion'] or 0):.6f}"
            writer.writerow(row)

    total_docs = sum(int(row["total_documents"] or 0) for row in rows)
    kept_docs = sum(int(row["kept_documents"] or 0) for row in rows)
    total_tokens = sum(int(row["tokens"] or 0) for row in rows)
    print(f"wrote: {output}")
    print(f"total_documents: {total_docs}")
    print(f"kept_documents: {kept_docs}")
    print(f"total_tokens: {total_tokens}")
    print(f"total_tokens_billion: {total_tokens / 1_000_000_000:.6f}")


def write_filtered_outputs(spark, args, year_paths, keep_scores):
    output_root = args.filtered_output.rstrip("/")
    if not output_root:
        raise ValueError("--filtered-output must not be empty when mode includes filter")

    for input_path in year_paths:
        dataset, year = parse_dataset_year(input_path)
        output_path = os.path.join(output_root, dataset, f"year_{year}")
        print(f"Filtering {input_path} -> {output_path}")

        df = spark.read.parquet(input_path)
        if args.score_key not in df.columns:
            raise ValueError(
                f"Missing required column {args.score_key!r} in {input_path}; "
                f"available columns: {df.columns}"
            )

        filtered = df.where(keep_score_filter(args.score_key, keep_scores))
        filtered.write.mode(args.filtered_output_mode).parquet(output_path)
        print(f"wrote filtered parquet: {output_path}")


def main():
    args = parse_args()
    keep_scores = parse_keep_scores(args.keep_scores)
    spark = init_spark("dedup_token_summary")

    year_paths = discover_year_paths(args.base)
    if not year_paths:
        raise FileNotFoundError(f"No year_YYYY directories found under {args.base}")

    print(f"Discovered {len(year_paths)} year directories")
    for path in year_paths:
        print(f"  {path}")
    print(f"Keeping scores: {keep_scores}")

    if args.mode in ("summary", "both"):
        write_summary(spark, args, year_paths, keep_scores)

    if args.mode in ("filter", "both"):
        write_filtered_outputs(spark, args, year_paths, keep_scores)

    spark.stop()


if __name__ == "__main__":
    main()
