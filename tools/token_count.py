import argparse
import json
import time
import os
import sys
from typing import Iterator, Any

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from transformers import AutoTokenizer

def format_number(num: int) -> str:
    if num < 1_000: return str(num)
    if num < 1_000_000: return f"{num / 1_000:.2f}K"
    if num < 1_000_000_000: return f"{num / 1_000_000:.2f}M"
    return f"{num / 1_000_000_000:.2f}B"

def count_tokens_partition(iterator: Iterator[Any], model_path: str, text_col: str) -> Iterator[int]:
    # 强制禁用 HuggingFace tokenizers 的内部多线程并行
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True, use_fast=True)
    except Exception as e:
        print(f"Error loading tokenizer on executor: {str(e)}", file=sys.stderr)
        yield 0
        return

    # 3. 批量处理
    local_total = 0
    batch_texts = []
    batch_size = 100 

    for row in iterator:
        val = row[text_col]

        if val: 
            batch_texts.append(str(val))
        
        if len(batch_texts) >= batch_size:
            enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
            if 'length' in enc:
                local_total += sum(enc['length'])
            else:
                local_total += sum(len(x) for x in enc['input_ids'])
    
    if batch_texts:
        enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
        if 'length' in enc:
            local_total += sum(enc['length'])
    
    yield local_total


def run_spark_analysis(spark: SparkSession, args):
    # 读取 Parquet 数据
    input_path = args.input_path
    print(f"Reading parquet data from: {input_path}")

    try:
        df = spark.read.parquet(input_path)
        print(f"Successfully loaded parquet file(s)")
    except Exception as e:
        print(f"Error reading parquet file: {e}", file=sys.stderr)
        raise
    
    # 提取文本列
    cols = df.columns
    target = args.text_key
    if args.text_key not in cols:
        candidates = [c for c in cols if "text" in c.lower() or "content" in c.lower()]
        target = candidates[0] if candidates else cols[0]
        if target != args.text_key:
            print(f"Using column '{target}' instead of '{args.text_key}'")
    
    df = df.select(F.col(target).alias("text").cast("string"))
    df = df.cache()
    total_docs = df.count()
    print(f"Total documents loaded: {total_docs}")

    results = {
        "meta": vars(args),
        "total_docs": total_docs,
        "details": {}
    }

    if "count_tokens" in args.tools:
        # 使用 collect 来调试查看每个分区的结果
        partition_results = df.rdd.mapPartitions(
            lambda iter: count_tokens_partition(iter, args.model_path, "text")
        ).collect()

        print(f"DEBUG: Partition results: {partition_results}")
        total_tokens = sum(partition_results)
        
        results["details"]["total_tokens"] = total_tokens
        results["details"]["total_tokens_human"] = format_number(total_tokens)
        print(f"Total Tokens: {format_number(total_tokens)} ({total_tokens})")

    if "sample" in args.tools:
        samples = df.limit(args.sample_n).collect()
        results["details"]["samples"] = [{"text": row["text"]} for row in samples]

    output_file = args.output_path
    if not output_file.startswith("hdfs://") and not output_file.startswith("s3://"):
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    start_time = time.time()

    parser = argparse.ArgumentParser(description="Spark Text Analysis Tool")
    parser.add_argument("--input-path", type=str, required=True, help="Input path (supports glob patterns)")
    parser.add_argument("--output-path", type=str, default="report.json", help="Output JSON path")
    parser.add_argument("--text-key", type=str, default="text", help="Column name")
    parser.add_argument("--model-path", type=str, default=None, help="HF model path")
    parser.add_argument("--tools", nargs="+", default=["sample", "count_tokens"], choices=["sample", "count_tokens"])
    parser.add_argument("--sample-n", type=int, default=3)
    
    args = parser.parse_args()
    
    conf = SparkConf()
    conf.set("spark.app.name", "Spark_Debug_Env")
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    run_spark_analysis(spark, args)
    
    end_time = time.time()
    print(f"Total Time: {end_time - start_time:.4f} sec")