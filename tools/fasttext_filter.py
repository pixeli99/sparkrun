import argparse
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import Row

try:
    import fasttext
except ImportError:
    # driver does not need fasttext
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
    # Enable ignoring corrupt files during read operations
    conf.set("spark.sql.files.ignoreCorruptFiles", "true")
    
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    # 1. Read input data
    if args.file_type == "json":
        df = spark.read.json(args.input_path)
    else:
        df = spark.read.parquet(args.input_path)
        
    # Ensure text column exists and is not empty
    df = df.filter(df[args.text_key].isNotNull() & (df[args.text_key] != ""))
    count = df.count()
    print(f"Total number of documents: {count}")

    # ==============================================================================
    # Process data using mapPartitions on Executor side for efficient model loading
    # ==============================================================================
    
    # Extract required parameters to avoid serialization risks from closure 
    # capturing the entire args object
    model_path = args.fasttext_model_dir
    text_key = args.text_key
    th_score = args.threshold_fasttext_score
    th_len = args.ccnt_min_length_th
    th_lines = args.theshold_num_lines

    def filter_partition(iterator):
        """
        This function runs once per partition on each Executor.
        Loading the model here amortizes the initialization cost across
        thousands of records in the partition.
        """
        import fasttext  # Import on Worker side
        
        # Load model once per partition (initialization cost amortized across
        # thousands of records). Note: With Lustre, Workers can directly read
        # model files via absolute paths.
        ft_model = fasttext.load_model(model_path)
        
        for row in iterator:
            # Extract text content from Row object
            # Row objects can be accessed via row.key or row['key']
            # For compatibility, convert to dict or use getattr if needed
            if isinstance(row, Row):
                text = row[text_key]
            else:
                continue  # Defensive programming: skip invalid rows
                
            if not text:
                continue

            # --- Apply filtering rules ---
            keep = True
            
            # Rule 1: FastText Score
            # FastText recommends removing newlines for prediction
            text_clean = text.replace('\n', ' ') 
            try:
                # k=1 returns top-1 prediction
                labels, scores = ft_model.predict(text_clean, k=1)
                score = scores[0]
                if score < th_score:
                    keep = False
            except Exception:
                keep = False

            if not keep: 
                continue  # Skip if any rule fails

            # Rule 2: Minimum length threshold
            length = len(text.strip().replace(" ", "").replace("\n", "").replace("\t", ""))
            if length < th_len:
                continue

            # Rule 3: Minimum number of lines
            if text.count('\n') + 1 < th_lines:
                continue
            
            # If all rules pass, yield this row
            yield row

    # Convert to RDD for processing, then convert back to DataFrame
    # Note: After using mapPartitions, we need to reconstruct DataFrame with schema.
    # For complex schemas, consider storing only IDs and joining back later,
    # but direct filtering is faster for cleaning scenarios.
    
    result_rdd = df.rdd.mapPartitions(filter_partition)
    
    # Convert filtered RDD back to DataFrame
    # Reuse the schema from the original DataFrame
    df_cleaned = spark.createDataFrame(result_rdd, schema=df.schema)

    # Write results to output path
    df_cleaned.write.mode("overwrite").parquet(args.output_path)
    count_cleaned = df_cleaned.count()
    print(f"Total number of documents after cleaning: {count_cleaned}")
    print(f"Filtering ratio: {count_cleaned / count}")
    spark.stop()

if __name__ == "__main__":
    args = parse_args()
    run_cleaning_job(args)