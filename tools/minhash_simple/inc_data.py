"""Stage 2: 拿 stage 1 落盘的签名，对增量数据做 MinHash 去重。

逻辑：
- source_hash_paths（可选）：历史已保留集合的签名
- inc_paths：本次新增数据的签名（hash_oss_path） + 原始 text (path)
- merge 历史 + 增量签名后，按 band_idx 独立处理；每个 (band_idx, band_hashes)
  桶留 id 最小的，其余 id 进 remove 集合；新增 text 用 leftanti 删除 remove id。

注意：这是「逐 band 局部最小」而不是 WCC 连通分量；语义上比 tools/minhash_dedup.py
更激进（更倾向删）。Stage 2 会把每个 band 的 remove_id 单独落盘，便于失败后续跑。
"""

import sys

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from utils import (
    get_input_path,
    load_yaml_file,
    process_md5_udf,
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


def init_spark(app_name: str) -> SparkSession:
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def read_hash_band(spark: SparkSession, path: str, band_idx: int):
    partition_path = f"{path}/band_idx={band_idx}"
    if path_exists(spark, partition_path):
        return spark.read.parquet(partition_path).select("band_hashes", "id")
    return (
        spark.read.parquet(path)
        .filter(F.col("band_idx") == F.lit(band_idx))
        .select("band_hashes", "id")
    )


def write_band_remove_ids(spark: SparkSession, df_merge_hash, band_idx: int, output_path: str):
    band_path = f"{output_path}/band_idx={band_idx}"
    if output_complete(spark, band_path):
        print(f"reuse remove ids for band {band_idx}: {band_path}")
        return band_path

    # Equivalent to row_number(... order by id) != 1, but avoids sorting each bucket.
    keepers = df_merge_hash.groupBy("band_hashes").agg(F.min("id").alias("id"))
    remove_ids = (
        df_merge_hash.join(keepers, on=["band_hashes", "id"], how="leftanti")
        .select("id")
        .drop_duplicates(["id"])
    )
    remove_ids.write.mode("overwrite").parquet(band_path)
    return band_path


def main(spark: SparkSession, yaml_config: dict):
    df_inc_text = None
    for inc_path in yaml_config["inc_paths"]:
        input_list = get_input_path(inc_path["path"])
        print("input_list:", input_list)
        df_text = (
            spark.read.option("recursiveFileLookup", "true")
            .parquet(*input_list)
            .withColumn("id", process_md5_udf("text", lit(inc_path["key"])))
            .select("meta", "text", "id")
        )
        df_inc_text = df_text if not df_inc_text else df_inc_text.unionByName(df_text)

    df_inc_count = df_inc_text.count()
    print("df_inc_count:", df_inc_count)

    # 不同 part 里可能有完全相同的 text（同 id），不去掉的话被后续判定为"有相似邻居"全删
    df_inc_text = df_inc_text.drop_duplicates(["id"])

    num_buckets = int(yaml_config.get("num_buckets", 32))
    remove_id_path = yaml_config.get("remove_id_path", yaml_config["output_path"] + "_remove_ids")
    remove_paths = []

    for band_idx in range(num_buckets):
        print(f"processing band_idx={band_idx}")
        df_source_hash = None
        if "source_hash_paths" in yaml_config:
            for source_path in yaml_config["source_hash_paths"]:
                df = read_hash_band(spark, source_path, band_idx)
                df_source_hash = df if df_source_hash is None else df_source_hash.unionByName(df)

        df_inc_hash = None
        for inc_path in yaml_config["inc_paths"]:
            df = read_hash_band(spark, inc_path["hash_oss_path"], band_idx)
            df_inc_hash = df if df_inc_hash is None else df_inc_hash.unionByName(df)

        df_merge_hash = (
            df_source_hash.unionByName(df_inc_hash) if df_source_hash is not None else df_inc_hash
        )
        if yaml_config.get("distinct_id", False):
            df_merge_hash = df_merge_hash.drop_duplicates(["band_hashes", "id"])

        remove_paths.append(
            write_band_remove_ids(
                spark,
                df_merge_hash,
                band_idx,
                remove_id_path,
            )
        )

    df_remove_id = spark.read.parquet(*remove_paths).drop_duplicates(["id"])

    df_keep_text = df_inc_text.join(df_remove_id, on="id", how="leftanti")
    df_keep_text.write.mode("overwrite").parquet(yaml_config["output_path"])

    df_keep_text = spark.read.parquet(yaml_config["output_path"])
    df_keep_count = df_keep_text.count()

    print("=" * 10, "结果统计")
    output_path = yaml_config["output_path"]
    print(
        f"output: {output_path}, inc ori count: {df_inc_count}, "
        f"inc output count: {df_keep_count}, ratio: {df_keep_count / df_inc_count}"
    )


if __name__ == "__main__":
    print("sys.argv:", sys.argv)
    config_path = sys.argv[1]

    spark = init_spark(config_path)
    yaml_config = load_yaml_file(config_path)
    print("yaml_config:", yaml_config)

    # multi_inc：一个 YAML 里串多组 source/inc 配置，逐组跑
    if "multi_inc" in yaml_config:
        for inc_conf in yaml_config["multi_inc"]:
            main(spark, inc_conf)
    else:
        main(spark, yaml_config)
