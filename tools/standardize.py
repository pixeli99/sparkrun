import argparse
import math
import os
import sys

from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
from pyspark.sql.types import MapType, StringType, StructField, StructType

TOOLS_DIR = os.path.dirname(os.path.abspath(__file__))
if TOOLS_DIR not in sys.path:
    sys.path.insert(0, TOOLS_DIR)

from data_schema.adapters import FineWebAdapter, FineWebEduAdapter, DCLMAdapter
from data_schema.adapters import Dolma3MixCCAdapter, Dolma3MixCodeAdapter, Dolma3MixOCRAdapter, Dolma3MixMathAdapter, Dolma3MixWikiAdapter


ADAPTERS = {
    "fineweb": FineWebAdapter,
    "fineweb_edu": FineWebEduAdapter,
    "dclm": DCLMAdapter,
    "dolma3_mix_cc": Dolma3MixCCAdapter,
    "dolma3_mix_code": Dolma3MixCodeAdapter,
    "dolma3_mix_ocr": Dolma3MixOCRAdapter,
    "dolma3_mix_math": Dolma3MixMathAdapter,
    "dolma3_mix_wiki": Dolma3MixWikiAdapter,
}


def parse_args():
    parser = argparse.ArgumentParser(description="Standardize dataset records")
    parser.add_argument("--adapter", type=str, required=True, choices=ADAPTERS.keys())
    parser.add_argument("--input_path", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--file_type", type=str, default="parquet", choices=["json", "parquet"])
    parser.add_argument("--num_partitions", type=int, default=0)
    parser.add_argument("--partition_size_mb", type=int, default=0)
    return parser.parse_args()


def build_spark():
    conf = SparkConf()
    conf.set("spark.app.name", "data_standardize")
    conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    return SparkSession.builder.config(conf=conf).getOrCreate()


def convert_to_standard(df, adapter_name):
    adapter_cls = ADAPTERS[adapter_name]
    adapter = adapter_cls()

    def to_standard(iterator):
        for row in iterator:
            raw = row.asDict(recursive=False)
            doc = adapter.adapt_and_validate(raw)
            yield Row(
                text=doc.text,
                meta={
                    "source": doc.meta.source,
                    "doc_id": doc.meta.doc_id,
                    "chunk_id": doc.meta.chunk_id,
                    "others": doc.meta.others,
                },
            )

    meta_type = StructType(
        [
            StructField("source", StringType(), False),
            StructField("doc_id", StringType(), False),
            StructField("chunk_id", StringType(), False),
            StructField("others", MapType(StringType(), StringType()), False),
        ]
    )
    schema = StructType(
        [
            StructField("text", StringType(), False),
            StructField("meta", meta_type, False),
        ]
    )

    standardized_rdd = df.rdd.mapPartitions(to_standard)
    return df.sparkSession.createDataFrame(standardized_rdd, schema=schema)


def get_input_bytes(spark, input_path):
    jvm = spark._jvm
    conf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(conf)
    path = jvm.org.apache.hadoop.fs.Path(input_path)
    statuses = fs.globStatus(path)
    if statuses is None:
        return 0
    total = 0
    for status in statuses:
        p = status.getPath()
        if status.isFile():
            total += status.getLen()
        else:
            total += fs.getContentSummary(p).getLength()
    return total


def main():
    args = parse_args()
    spark = build_spark()
    if args.file_type == "json":
        df = spark.read.json(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)

    standardized_df = convert_to_standard(df, args.adapter)
    if args.num_partitions > 0:
        standardized_df = standardized_df.repartition(args.num_partitions)
    elif args.partition_size_mb > 0:
        total_bytes = get_input_bytes(spark, args.input_path)
        if total_bytes <= 0:
            print("Input size is 0 bytes; skip repartitioning")
        else:
            target_bytes = args.partition_size_mb * 1024 * 1024
            partitions = max(1, int(math.ceil(total_bytes / target_bytes)))
            print(f"Total input size: {total_bytes} bytes")
            print(f"Repartitioning to {partitions} partitions (~{args.partition_size_mb} MB each)")
            standardized_df = standardized_df.repartition(partitions)
    elif args.num_partitions < 0:
        count = df.count()
        print(f"Total number of documents: {count}")
        print(f"Repartitioning to {int(count / 500000)} partitions")
        standardized_df = standardized_df.repartition(int(count / 500000))

    standardized_df.write.mode("overwrite").parquet(args.output_path)
    spark.stop()


if __name__ == "__main__":
    main()
