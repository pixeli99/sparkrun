"""跨年完全文本重复 ID 修补集 —— 让 Path B (per-year filter) 和 Path A 严格等价。

per-year minhash dedup 之后，跨年完全相同的 doc 在 stage 2 distinct (band_hashes, id)
会被折成一行 → 在 groupBy(band_hashes).min(id) 里自己就是 min → 不进 remove_ids。
结果 path B 按年 filter 时这些 doc 会被多个年份各保留一份；path A 因为 df_inc_text
做了 drop_duplicates(["id"])，只保留一份。差距估算 0.5%–5%。

本脚本读每年的 hash/.../global_2013_2025/year_YYYY/band_idx=N（任一 band 都有完整 id 集合，
因为每个 doc 32 band 都生成一行），找跨年重复的 id，挑 min(year) 当 canonical，
其他年份的实例写到 extras（按 year 分区），下游 apply_global_remove 第二次 leftanti 用。

YAML:
  band_idx: 0   # 哪个 band 读 id（任意，0 最方便）
  hash_inputs:
    - {year: 2013, hash_oss_path: ".../hash/.../global_2013_2025/year_2013"}
    - {year: 2014, hash_oss_path: ".../hash/.../global_2013_2025/year_2014"}
    ...
  output_path: ".../dedup/.../global_2013_2025_cross_year_extras"
"""

import sys

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession

from utils import load_yaml_file


def hadoop_path(spark: SparkSession, path: str):
    return spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)


def hadoop_fs(spark: SparkSession, path: str):
    conf = spark.sparkContext._jsc.hadoopConfiguration()
    return hadoop_path(spark, path).getFileSystem(conf)


def path_exists(spark: SparkSession, path: str) -> bool:
    return hadoop_fs(spark, path).exists(hadoop_path(spark, path))


def output_complete(spark: SparkSession, path: str) -> bool:
    return path_exists(spark, path + "/_SUCCESS")


def init_spark(app_name: str) -> SparkSession:
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def main(spark: SparkSession, yaml_config: dict):
    band_idx = int(yaml_config.get("band_idx", 0))
    output_path = yaml_config["output_path"]

    if output_complete(spark, output_path):
        print(f"reuse existing cross-year extras: {output_path}")
        return

    df_all = None
    for src in yaml_config["hash_inputs"]:
        year = int(src["year"])
        hash_path = src["hash_oss_path"]
        band_path = f"{hash_path}/band_idx={band_idx}"
        if not path_exists(spark, band_path):
            raise FileNotFoundError(f"missing hash band: {band_path}")
        df = (
            spark.read.parquet(band_path)
            .select("id")
            .drop_duplicates(["id"])
            .withColumn("year", F.lit(year))
        )
        df_all = df if df_all is None else df_all.unionByName(df)

    # 每个 id 在每个 year 至多一行（已 distinct）；跨年重复 ⇔ count("*") > 1
    dup_summary = (
        df_all.groupBy("id")
        .agg(F.min("year").alias("keep_year"), F.count("*").alias("n_years"))
        .where("n_years > 1")
        .select("id", "keep_year")
    )

    # 非 keep_year 的 (id, year) → 该年份需要额外删除的 doc
    extras = (
        df_all.join(dup_summary, on="id")
        .where(F.col("year") != F.col("keep_year"))
        .select("id", "year")
    )

    extras.write.mode("overwrite").partitionBy("year").parquet(output_path)
    print(f"wrote cross-year extras to {output_path}")


if __name__ == "__main__":
    print("sys.argv:", sys.argv)
    config_path = sys.argv[1]

    spark = init_spark(f"build_cross_year_extras:{config_path}")
    yaml_config = load_yaml_file(config_path)
    print("yaml_config:", yaml_config)

    main(spark, yaml_config)
