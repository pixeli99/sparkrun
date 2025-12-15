import argparse
import json
import time
import os
import sys
import socket
from typing import List, Dict, Any, Iterator
from functools import reduce

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

# 尝试导入 transformers，如果没有则标记为 None
try:
    from transformers import AutoTokenizer
except ImportError:
    AutoTokenizer = None

# ==========================================
# 新增：环境调试函数
# ==========================================
def print_driver_env():
    """打印 Driver 端环境信息"""
    print("\n" + "="*50)
    print(f"✅ [DRIVER INFO] Hostname: {socket.gethostname()}")
    print(f"✅ [DRIVER INFO] Python Executable: {sys.executable}")
    print(f"✅ [DRIVER INFO] Working Dir: {os.getcwd()}")
    try:
        import transformers
        print(f"✅ [DRIVER INFO] Transformers version: {transformers.__version__} (Path: {transformers.__file__})")
    except ImportError:
        print("❌ [DRIVER INFO] Transformers NOT installed!")
    print("="*50 + "\n")

def debug_worker_env(iterator):
    """打印 Worker 端环境信息 (只打印一次)"""
    # 获取当前 Worker 的信息
    import sys, os, socket
    worker_info = {
        "hostname": socket.gethostname(),
        "python_path": sys.executable,
        "cwd": os.getcwd(),
        "has_transformers": False,
        "env_vars": dict(os.environ)
    }
    
    try:
        import transformers
        worker_info["has_transformers"] = True
        worker_info["transformers_path"] = transformers.__file__
    except ImportError:
        worker_info["has_transformers"] = False
        
    # 为了防止每个分区都打印，我们只 yield 一个结果出来
    yield worker_info

# ==========================================
# 原有业务逻辑
# ==========================================

def format_number(num: int) -> str:
    if num < 1_000: return str(num)
    if num < 1_000_000: return f"{num / 1_000:.2f}K"
    if num < 1_000_000_000: return f"{num / 1_000_000:.2f}M"
    return f"{num / 1_000_000_000:.2f}B"

def count_tokens_partition(iterator: Iterator[Any], model_path: str, text_col: str) -> Iterator[int]:
    # 强制禁用 HuggingFace 并行
    os.environ["TOKENIZERS_PARALLELISM"] = "false"
    
    # 再次检查 transformers，确保在 mapPartitions 内部也能捕获错误
    if AutoTokenizer is None:
        import sys
        print(f"CRITICAL ERROR: Transformers not found on worker {socket.gethostname()}", file=sys.stderr)
        # 这里不 raise，而是 yield 0，防止整个 Job 挂掉看不到前面的 debug 信息
        yield 0
        return

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
        try:
            val = row[text_col]
        except (AttributeError, KeyError, TypeError):
             val = getattr(row, text_col, "")

        if val: 
            batch_texts.append(str(val))
        
        if len(batch_texts) >= batch_size:
            try:
                enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
                if 'length' in enc:
                    local_total += sum(enc['length'])
                else:
                    local_total += sum(len(x) for x in enc['input_ids'])
            except Exception as e:
                print(f"Error tokenizing batch: {e}", file=sys.stderr)
            finally:
                batch_texts = [] 
    
    if batch_texts:
        try:
            enc = tokenizer(batch_texts, add_special_tokens=False, return_length=True)
            if 'length' in enc:
                local_total += sum(enc['length'])
            else:
                local_total += sum(len(x) for x in enc['input_ids'])
        except Exception as e:
            print(f"Error tokenizing final batch: {e}", file=sys.stderr)
    
    yield local_total


def run_spark_analysis(spark: SparkSession, args):
    # --- 1. 执行环境探测 (关键步骤) ---
    print("\n🔍 Starting Environment Probe on Workers...")
    # 创建一个极小的 RDD (例如 4 个分区)，触发 Worker 运行 debug 函数
    # 我们只取前 2 个结果看看就行
    env_rdd = spark.sparkContext.parallelize(range(4), 4).mapPartitions(debug_worker_env)
    worker_envs = env_rdd.collect()
    
    print("="*50)
    print(f"🔍 Received {len(worker_envs)} worker reports:")
    for i, env in enumerate(worker_envs):
        status = "✅ OK" if env['has_transformers'] else "❌ MISSING TRANSFORMERS"
        print(f"Worker {i+1} [{env['hostname']}]:")
        print(f"   Python: {env['python_path']}")
        print(f"   Transformers: {status}")
        if not env['has_transformers']:
            print("   WARNING: This worker will fail to run tokenization!")
    print("="*50 + "\n")
    # --- 探测结束 ---

    # 读取数据：根据路径自动判断文件类型
    input_path = args.input_path
    print(f"Reading data from: {input_path}")

    # 根据路径自动判断文件类型并读取
    input_path_lower = input_path.lower()
    if "parquet" in input_path_lower:
        print("Detected parquet format")
        df = spark.read.parquet(input_path)
    elif "jsonl.zst" in input_path_lower or ".zst" in input_path_lower:
        # 处理 zstd 压缩的 JSONL 文件
        print("Detected jsonl.zst format (zstd compressed)")
        print("Reading as binary and decompressing...")
        
        try:
            import zstandard as zstd
        except ImportError:
            raise ImportError("zstandard library is required for .jsonl.zst files. Install with: pip install zstandard")
        
        # 使用 binaryFile 格式读取（读取二进制文件）
        def decompress_jsonl_zst(iterator):
            dctx = zstd.ZstdDecompressor()
            for row in iterator:
                # row.content 是二进制内容
                content = row.content if hasattr(row, 'content') else bytes(row)
                try:
                    # 解压数据
                    decompressed = dctx.decompress(content)
                    # 按行分割（JSONL 格式）
                    for line in decompressed.decode('utf-8').split('\n'):
                        if line.strip():
                            yield line
                except Exception as e:
                    print(f"Warning: Failed to decompress file {row.path if hasattr(row, 'path') else 'unknown'}: {e}")
                    continue
        
        # 读取为二进制文件
        binary_df = spark.read.format("binaryFile").load(input_path)
        # 使用 mapPartitions 解压
        json_lines_rdd = binary_df.rdd.mapPartitions(decompress_jsonl_zst)
        # 将解压后的 JSON 行转换为 DataFrame
        df = spark.read.json(json_lines_rdd.map(lambda x: x))
    elif "json" in input_path_lower:
        print("Detected json format")
        df = spark.read.json(input_path)
    else:
        # 默认尝试 parquet
        print("Unknown format, trying parquet...")
        try:
            df = spark.read.parquet(input_path)
        except Exception:
            # 如果 parquet 失败，尝试 json
            print("Parquet failed, trying json...")
            df = spark.read.json(input_path)
    
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

    if "sample" in args.tools:
        samples = df.limit(args.sample_n).collect()
        results["details"]["samples"] = [{"text": row["text"]} for row in samples]

        

    if "count_tokens" in args.tools:
        # 使用 collect 来调试查看每个分区的结果
        partition_results = df.rdd.mapPartitions(
            lambda iter: count_tokens_partition(iter, args.model_path, "text")
        ).collect()

        print(f"DEBUG: Partition results: {partition_results}")
        total_tokens = sum(partition_results)
        
        results["total_tokens"] = total_tokens
        results["total_tokens_human"] = format_number(total_tokens)
        print(f"Total Tokens: {format_number(total_tokens)} ({total_tokens})")

    output_file = args.output_path
    if not output_file.startswith("hdfs://") and not output_file.startswith("s3://"):
        os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(results, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    start_time = time.time()

    # 0. 先打印 Driver 环境
    print_driver_env()

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