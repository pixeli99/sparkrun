"""Stage 2: 拿 stage 1 落盘的签名，对增量数据做 MinHash 去重。

逻辑：
- source_hash_paths（可选）：历史已保留集合的签名
- inc_paths：本次新增数据的签名（hash_oss_path） + 原始 text (path)
- merge 历史 + 增量签名后，按 (band_idx, band_hashes) 分桶，每桶留 id 最小的，
  其余 id 进 remove 集合；新增 text 用 leftanti 把 remove 中的 id 剔掉。

注意：这是「逐 band 局部最小」而不是 WCC 连通分量；语义上比 tools/minhash_dedup.py
更激进（更倾向删），并且 partitionBy=(band_idx, band_hashes) 下的 row_number
在数据倾斜时会单 reducer 卡顿，TB 量级需要先按 (lang/source/year) 切片再跑。
"""

import sys

import pyspark.sql.functions as F
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.window import Window

from utils import (
    get_input_path,
    load_yaml_file,
    process_md5_udf,
)


def init_spark(app_name: str) -> SparkSession:
    conf = SparkConf()
    conf.set("spark.app.name", app_name)
    conf.set("spark.debug.maxToStringFields", "100")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def main(spark: SparkSession, yaml_config: dict):
    df_source_hash = None
    if "source_hash_paths" in yaml_config:
        for source_path in yaml_config["source_hash_paths"]:
            df = spark.read.parquet(source_path)
            df_source_hash = df if not df_source_hash else df_source_hash.unionByName(df)

    df_inc_hash = None
    df_inc_text = None
    for inc_path in yaml_config["inc_paths"]:
        df = spark.read.parquet(inc_path["hash_oss_path"])
        df_inc_hash = df if not df_inc_hash else df_inc_hash.unionByName(df)

        input_list = get_input_path(inc_path)
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

    df_merge_hash = (
        df_source_hash.unionByName(df_inc_hash) if df_source_hash is not None else df_inc_hash
    )

    if yaml_config.get("distinct_id", False):
        df_merge_hash = df_merge_hash.drop_duplicates(["band_idx", "band_hashes", "id"])

    # 每个 (band_idx, band_hashes) 桶里 id 最小的留下，其余进 remove
    df_remove_id = (
        df_merge_hash.withColumn(
            "rank",
            F.row_number().over(
                Window.partitionBy(["band_idx", "band_hashes"]).orderBy(F.col("id"))
            ),
        )
        .filter(F.col("rank") != 1)
        .select("id")
        .drop_duplicates(["id"])
    )

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
