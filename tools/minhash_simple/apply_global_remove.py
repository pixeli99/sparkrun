"""按年应用 global remove_ids —— 替代 inc_data.py 末尾那步 14T text leftanti。

inc_data.py:133-138 把 13 年 text 当一个整体做 leftanti + write，单 stage shuffle
~150TB 撑爆磁盘（参考 job 81527/81718）。这里把 13 年拆开，每年独立 read → 算
global id → leftanti remove_ids（+ 可选 cross-year extras）→ 写本年 output。
每年 shuffle 量降到 ~1–2T，单节点 /raid 完全扛得住。失败可按年 _SUCCESS 续跑。

输出 schema 跟 path A 一致：text + meta + stage1_category + stage3_score + stage3_reason + id(global)。

YAML:
  remove_id_path: ".../dedup/.../global_2013_2025_remove_ids"
  cross_year_extras_path: ".../dedup/.../global_2013_2025_cross_year_extras"  # 可选
  global_id_key: "fineweb_012_global_2013_2025_"
  output_root: ".../dedup/.../global_2013_2025"
  years:
    - {year: 2013, text_input: ".../dedup/.../year_2013"}
    - ...
"""

import sys

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from utils import load_yaml_file, process_md5_udf


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


def load_remove_ids(spark: SparkSession, remove_id_path: str):
    # 32 个 band 落盘的 remove_id 集合，distinct 一次给所有年份共用
    return (
        spark.read.parquet(remove_id_path)
        .select("id")
        .drop_duplicates(["id"])
    )


def apply_one_year(
    spark: SparkSession,
    year: int,
    text_input: str,
    remove_df,
    extras_df,
    global_id_key: str,
    output_path: str,
):
    if output_complete(spark, output_path):
        print(f"[year {year}] reuse existing output: {output_path}")
        return

    print(f"[year {year}] start: read {text_input}")
    df = spark.read.parquet(text_input)
    # year-level id 列用的是 per-year key，丢掉；重新算 global id
    if "id" in df.columns:
        df = df.drop("id")
    df = df.withColumn("id", process_md5_udf("text", lit(global_id_key)))

    df = df.join(remove_df, on="id", how="leftanti")

    if extras_df is not None:
        df = df.join(extras_df, on="id", how="leftanti")

    df.write.mode("overwrite").parquet(output_path)
    print(f"[year {year}] wrote {output_path}")


def main(spark: SparkSession, yaml_config: dict):
    remove_id_path = yaml_config["remove_id_path"]
    global_id_key = yaml_config["global_id_key"]
    output_root = yaml_config["output_root"].rstrip("/")
    cross_year_extras_path = yaml_config.get("cross_year_extras_path")

    remove_df = load_remove_ids(spark, remove_id_path).cache()
    n_remove = remove_df.count()
    print(f"distinct remove ids: {n_remove}")

    for entry in yaml_config["years"]:
        year = int(entry["year"])
        text_input = entry["text_input"]
        output_path = f"{output_root}/year_{year}"

        extras_df = None
        if cross_year_extras_path:
            extras_year_path = f"{cross_year_extras_path}/year={year}"
            if path_exists(spark, extras_year_path):
                extras_df = (
                    spark.read.parquet(extras_year_path)
                    .select("id")
                    .drop_duplicates(["id"])
                )
            else:
                print(
                    f"[year {year}] no cross_year_extras partition at {extras_year_path}, "
                    f"skipping cross-year filter (path B will keep cross-year exact dups)"
                )

        apply_one_year(
            spark,
            year=year,
            text_input=text_input,
            remove_df=remove_df,
            extras_df=extras_df,
            global_id_key=global_id_key,
            output_path=output_path,
        )


if __name__ == "__main__":
    print("sys.argv:", sys.argv)
    config_path = sys.argv[1]

    spark = init_spark(f"apply_global_remove:{config_path}")
    yaml_config = load_yaml_file(config_path)
    print("yaml_config:", yaml_config)

    main(spark, yaml_config)
