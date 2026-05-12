"""Stage 1: 算 MinHash 签名落盘。

读 YAML 里 hash_source 下每个 source 的 input parquet，给每条 text 算 id (md5)
和 (band_idx, band_hashes, id) 行，写到 hash_oss_path。这一步不做去重判断，
只是把签名固化下来，便于 stage 2 (inc_data.py) 拿来跟历史/增量对齐。

Cluster sizing、master url 等从 spark-submit 命令行传，不在脚本里硬编码。
"""

import sys

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

from utils import (
    build_generate_hash_values_function,
    get_input_path,
    load_yaml_file,
    normalize_text,
    process_md5_udf,
)


def init_spark(app_name: str) -> SparkSession:
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def main(spark: SparkSession, yaml_config: dict, num_buckets: int, num_hashes_per_bucket: int, ngram: int):
    for source in yaml_config["hash_source"]:
        input_list = get_input_path(source["path"])
        output_path = source["hash_oss_path"]
        print("input_list:", input_list)
        print("output_path:", output_path)

        df = (
            spark.read.option("recursiveFileLookup", "true")
            .parquet(*input_list)
            .select("meta", "text")
            .withColumn("stripped_text", normalize_text("text"))
            .withColumn("id", process_md5_udf("text", lit(source["key"])))
        )

        generate_hash_values = build_generate_hash_values_function(
            num_buckets=num_buckets,
            num_hashes_per_bucket=num_hashes_per_bucket,
        )

        # 核心：normalize 后的 text 过 jieba n-gram → numpy minhash → 每条 doc 拆成 num_buckets 行
        records = df.select("id", "stripped_text").rdd
        hash_values = records.flatMap(
            lambda x: generate_hash_values(
                content=x[1],
                idx=x[0],
                ngram_size=ngram,
            )
        )
        df_hash_values = hash_values.toDF().select(
            F.col("_1").alias("band_idx"),
            F.col("_2").alias("band_hashes"),
            F.col("_3").alias("id"),
        )
        # Partition by band so Stage 2 can process each band independently.
        df_hash_values.write.mode("overwrite").partitionBy("band_idx").parquet(output_path)


if __name__ == "__main__":
    print("sys.argv:", sys.argv)
    config_path = sys.argv[1]

    yaml_config = load_yaml_file(config_path)
    print("yaml_config:", yaml_config)

    # 32 * 10 = 320 个 perm；比 minhash_dedup.py 默认的 num_perm=128 重，
    # 召回更激进。降下来对应改 num_buckets/num_hashes_per_bucket。
    num_buckets = int(yaml_config.get("num_buckets", 32))
    num_hashes_per_bucket = int(yaml_config.get("num_hashes_per_bucket", 10))
    ngram = int(yaml_config.get("ngram", 5))

    spark = init_spark(config_path)

    main(spark, yaml_config, num_buckets, num_hashes_per_bucket, ngram)
