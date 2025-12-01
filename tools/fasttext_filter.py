import argparse
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

try:
    import fasttext
except ImportError:
    pass

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_path", type=str, required=True)
    parser.add_argument("--output_path", type=str, required=True)
    parser.add_argument("--file_type", type=str, default="parquet", choices=["json", "parquet"])
    parser.add_argument("--text_key", type=str, default="text")
    parser.add_argument("--fasttext_model_dir", type=str, required=True)
    parser.add_argument("--threshold_fasttext_score", type=float, default=0.4)
    parser.add_argument("--ccnt_min_length_th", type=int, default=200)
    parser.add_argument("--theshold_num_lines", type=int, default=1)
    return parser.parse_args()

def run_cleaning_job(args):
    conf = SparkConf()
    conf.set("spark.app.name", "FasttextFilter")
    # 允许忽略损坏文件
    conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # 1. 读取数据
    if args.file_type == "json":
        df = spark.read.json(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)
        
    # 确保 text 列存在且不为空
    df = df.filter(df[args.text_key].isNotNull() & (df[args.text_key] != ""))
    count = df.count()
    print(f"Total number of documents: {count}")

    # ==============================================================================
    # 核心修改：使用 mapPartitions 在 Executor 端处理
    # ==============================================================================
    
    # 将需要的参数广播出去，避免闭包捕获整个 args 对象带来的序列化风险
    model_path = args.fasttext_model_dir
    text_key = args.text_key
    th_score = args.threshold_fasttext_score
    th_len = args.ccnt_min_length_th
    th_lines = args.theshold_num_lines

    def filter_partition(iterator):
        """
        这个函数会在每个 Executor 的每个 Partition 上运行一次。
        在这里加载模型，可以分摊开销。
        """
        import fasttext # 在 Worker 端导入
        
        # 1. 在当前分区加载一次模型 (初始化开销被成千上万条数据分摊)
        # 注意：因为用了 Lustre，Worker 可以直接通过绝对路径读取模型文件
        ft_model = fasttext.load_model(model_path)
        
        for row in iterator:
            # 获取文本内容
            # Row 对象可以用 row.key 或者 row['key'] 访问
            # 为了兼容性，转成 dict 或者直接 getattr
            if isinstance(row, Row):
                text = row[text_key]
            else:
                continue # 防御性编程
                
            if not text:
                continue

            # --- 原始逻辑复用 ---
            keep = True
            
            # Rule 1: FastText Score
            # fasttext 建议移除换行符进行预测
            text_clean = text.replace('\n', ' ') 
            try:
                # k=1 返回 top 1
                labels, scores = ft_model.predict(text_clean, k=1)
                score = scores[0]
                if score < th_score:
                    keep = False
            except Exception:
                keep = False

            if not keep: 
                continue # 只要有一条规则挂了，就跳过

            # Rule 2: Length
            # 优化：避免多次 replace 带来的内存/CPU开销，直接用 translate 或者简单判断
            # 这里沿用你的逻辑，但稍微优化写法
            length = len(text.strip().replace(" ", "").replace("\n", "").replace("\t", ""))
            if length < th_len:
                continue

            # Rule 3: Lines
            if text.count('\n') + 1 < th_lines:
                continue
            
            # 如果都通过了，yield 这行数据
            yield row

    # 转换为 RDD 进行处理，然后转回 DataFrame
    # 注意：使用 mapPartitions 后，我们需要用 schema 重新构建 DataFrame
    # 如果 schema 很复杂，可以只存 id，后面再 join 回去，但在清洗场景下直接 filter 比较快
    
    result_rdd = df.rdd.mapPartitions(filter_partition)
    
    # 将过滤后的 RDD 转回 DataFrame
    # 这里的 schema 直接复用读取时的 schema
    df_cleaned = spark.createDataFrame(result_rdd, schema=df.schema)

    # 写入结果
    df_cleaned.write.mode("overwrite").parquet(args.output_path)
    count_cleaned = df_cleaned.count()
    print(f"Total number of documents after cleaning: {count_cleaned}")
    print(f"Filtering ratio: {count_cleaned / count}")
    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    run_cleaning_job(args)