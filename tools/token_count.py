import argparse
import json
import time
import os
import sys
import threading
from typing import Iterator, Any, Optional

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

from transformers import AutoTokenizer


def log(message: str) -> None:
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", flush=True)


def filter_body_lines(text: str, line_id_json: str) -> str:
    """按 line_id JSON 里的 1-indexed 行号抽出正文行,其它行丢弃。"""
    if not text:
        return ""
    if not line_id_json:
        return ""
    try:
        obj = json.loads(line_id_json)
    except (json.JSONDecodeError, TypeError):
        return ""
    ids = obj.get("line_id", []) if isinstance(obj, dict) else []
    if not ids:
        return ""
    lines = text.split("\n")
    n = len(lines)
    selected = [lines[i - 1] for i in ids if isinstance(i, int) and 1 <= i <= n]
    return "\n".join(selected)

def format_number(num: int) -> str:
    if num < 1_000: return str(num)
    if num < 1_000_000: return f"{num / 1_000:.2f}K"
    if num < 1_000_000_000: return f"{num / 1_000_000:.2f}M"
    return f"{num / 1_000_000_000:.2f}B"

def format_docs_progress(processed_docs: int, total_docs: Optional[int]) -> str:
    if total_docs is None:
        return f"docs={format_number(processed_docs)}"
    doc_pct = (processed_docs / total_docs * 100.0) if total_docs else 0.0
    return f"docs={format_number(processed_docs)}/{format_number(total_docs)} ({doc_pct:.1f}%)"

def format_duration(seconds: float) -> str:
    total_seconds = int(seconds)
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:02d}:{secs:02d}"


def monitor_token_count_progress(
    stop_event: threading.Event,
    total_docs: Optional[int],
    total_partitions: int,
    processed_docs_acc: Any,
    processed_tokens_acc: Any,
    completed_partitions_acc: Any,
    poll_interval_sec: int,
) -> None:
    started_at = time.time()
    last_snapshot = None

    while not stop_event.wait(max(1, poll_interval_sec)):
        completed_partitions = int(completed_partitions_acc.value)
        processed_docs = int(processed_docs_acc.value)
        processed_tokens = int(processed_tokens_acc.value)
        snapshot = (completed_partitions, processed_docs, processed_tokens)

        if snapshot == last_snapshot:
            continue
        last_snapshot = snapshot

        log(
            "Token count progress: "
            f"partitions={completed_partitions}/{total_partitions}, "
            f"{format_docs_progress(processed_docs, total_docs)}, "
            f"tokens={format_number(processed_tokens)}, "
            f"elapsed={format_duration(time.time() - started_at)}"
        )


def count_tokens_partition(
    iterator: Iterator[Any],
    model_path: str,
    text_col: str,
    processed_docs_acc: Any,
    processed_tokens_acc: Any,
    completed_partitions_acc: Any,
) -> Iterator[int]:
    # 强制禁用 HuggingFace tokenizers 的内部多线程并行
    os.environ["TOKENIZERS_PARALLELISM"] = "false"

    local_total = 0
    local_docs = 0

    try:
        tokenizer = AutoTokenizer.from_pretrained(model_path, trust_remote_code=True, use_fast=True)
    except Exception as e:
        print(f"Error loading tokenizer on executor: {str(e)}", file=sys.stderr)
        completed_partitions_acc.add(1)
        yield 0
        return

    # 3. 批量处理
    batch_texts = []
    batch_size = 100 

    for row in iterator:
        local_docs += 1
        val = row[text_col]

        if val: 
            batch_texts.append(str(val))
        
        if len(batch_texts) >= batch_size:
            enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
            if 'length' in enc:
                local_total += sum(enc['length'])
            else:
                local_total += sum(len(x) for x in enc['input_ids'])
            batch_texts = []

    if batch_texts:
        enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
        if 'length' in enc:
            local_total += sum(enc['length'])
        else:
            local_total += sum(len(x) for x in enc['input_ids'])

    processed_docs_acc.add(local_docs)
    processed_tokens_acc.add(local_total)
    completed_partitions_acc.add(1)

    yield local_total


def run_spark_analysis(spark: SparkSession, args):
    # 读取 Parquet 数据
    input_path = args.input_path
    log(f"Reading parquet data from: {input_path}")

    try:
        df = spark.read.parquet(input_path)
        log("Successfully loaded parquet file(s)")
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
            log(f"Using column '{target}' instead of '{args.text_key}'")

    if args.line_id_key:
        if args.line_id_key not in cols:
            print(f"Error: --line-id-key '{args.line_id_key}' not found in columns {cols}", file=sys.stderr)
            raise ValueError(f"line_id column '{args.line_id_key}' not found")
        log(f"Filtering body lines by column '{args.line_id_key}' before token counting")
        filter_body_udf = F.udf(filter_body_lines, StringType())
        df = df.select(
            filter_body_udf(
                F.col(target).cast("string"),
                F.col(args.line_id_key).cast("string"),
            ).alias("text")
        )
    else:
        df = df.select(F.col(target).alias("text").cast("string"))
    total_partitions = df.rdd.getNumPartitions()
    log(f"Input partitions: {total_partitions}")

    if args.cache_input:
        log("Caching selected text DataFrame. Use this only when the selected data fits executor memory/disk.")
        df = df.cache()
    else:
        log("Skipping DataFrame cache to avoid executor memory/disk pressure.")

    total_docs = None
    if args.count_total_docs:
        total_docs = df.count()
        log(f"Total documents loaded: {format_number(total_docs)}")
    else:
        log("Skipping total document pre-count; progress will report processed docs without a percentage.")

    results = {
        "meta": vars(args),
        "total_docs": total_docs,
        "details": {}
    }
    results["meta"]["input_partitions"] = total_partitions

    if "count_tokens" in args.tools:
        processed_docs_acc = spark.sparkContext.accumulator(0)
        processed_tokens_acc = spark.sparkContext.accumulator(0)
        completed_partitions_acc = spark.sparkContext.accumulator(0)
        stop_event = threading.Event()
        progress_thread = threading.Thread(
            target=monitor_token_count_progress,
            args=(
                stop_event,
                total_docs,
                total_partitions,
                processed_docs_acc,
                processed_tokens_acc,
                completed_partitions_acc,
                args.progress_interval_sec,
            ),
            daemon=True,
        )
        progress_thread.start()

        try:
            partition_results = df.rdd.mapPartitions(
                lambda iter: count_tokens_partition(
                    iter,
                    args.model_path,
                    "text",
                    processed_docs_acc,
                    processed_tokens_acc,
                    completed_partitions_acc,
                )
            ).collect()
        finally:
            stop_event.set()
            progress_thread.join(timeout=1)

        total_tokens = sum(partition_results)
        processed_docs = int(processed_docs_acc.value)

        if total_docs is None:
            results["total_docs"] = processed_docs
        results["details"]["processed_docs"] = processed_docs
        results["details"]["total_tokens"] = total_tokens
        results["details"]["total_tokens_human"] = format_number(total_tokens)
        log(
            "Token count finished: "
            f"partitions={int(completed_partitions_acc.value)}/{total_partitions}, "
            f"{format_docs_progress(processed_docs, total_docs)}, "
            f"tokens={format_number(total_tokens)} ({total_tokens})"
        )

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
    parser.add_argument("--line-id-key", type=str, default=None,
                        help="若指定,读取该列的 JSON line_id (如 stage2 的 'line_id' 列),仅统计正文行 token")
    parser.add_argument("--model-path", type=str, default=None, help="HF model path")
    parser.add_argument("--tools", nargs="+", default=["sample", "count_tokens"], choices=["sample", "count_tokens"])
    parser.add_argument("--sample-n", type=int, default=3)
    parser.add_argument("--progress-interval-sec", type=int, default=30,
                        help="打印 token count 进度日志的时间间隔（秒）")
    parser.add_argument("--cache-input", action="store_true",
                        help="缓存选出的 text DataFrame。40TB 级别默认不要开启。")
    parser.add_argument("--count-total-docs", action="store_true",
                        help="在 token count 前先全量 count 文档数。40TB 级别默认不要开启。")
    
    args = parser.parse_args()
    
    conf = SparkConf()
    conf.set("spark.app.name", "Spark_Debug_Env")
    conf.set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true")
    conf.set("spark.ui.showConsoleProgress", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    run_spark_analysis(spark, args)
    
    end_time = time.time()
    print(f"Total Time: {end_time - start_time:.4f} sec")
