#!/bin/bash

# ====================================================================
# Resource Environment variables
# ===============================================================
# Ensure MASTER_URL is not empty (Assuming running in a cluster/slurm env that provides this)
if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    # 如果是在单机测试，可以取消下面这行的注释
    # MASTER_URL="local[*]" 
    exit 1
fi

set -x

# Read from environment variables, use defaults if not set
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-32G}"
EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-4G}"
DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-16}"

# Log Configuration
LOG_PATH=${LOG_PATH:-/work/projects/polyullm/wenjun/spark/logs/slurm}
mkdir -p ${LOG_PATH} # Ensure log directory exists
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/spark-analysis-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/spark-analysis-${TIMESTAMP}.err"

# ====================================================================
# Application parameters
# ===============================================================

# Calculate SQL_SHUFFLE_PARTITIONS
SQL_SHUFFLE_PARTITIONS=$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 1.5)}")

# Input/Output
INPUT_PATH=${INPUT_PATH:-"/work/projects/polyullm/congkai/pretrain_data/Nemotron-CC-Math-v1/*/*.parquet"}
OUTPUT_PATH=${OUTPUT_PATH:-"/work/projects/polyullm/wtf/data/Nemotraon-CC-Math-v1-analysis/report.json"}

# Model Path (Tokenizer)
# 必须是所有 Worker 节点都能访问的路径
MODEL_PATH=${MODEL_PATH:-"/work/projects/polyullm/models/Qwen/Qwen2.5-7B"}

# Tools configuration
TOOLS=${TOOLS:-"sample count_tokens"}
SAMPLE_N=${SAMPLE_N:-"5"}

# [Critical] Python Environment on Lustre (Must exist!)
# 请修改为你解压后的真实路径
ENV_ROOT="/work/projects/polyullm/wenjun/envs/spark_nlp_1"
ENV_PYTHON="${ENV_ROOT}/bin/python"

set +x

echo "Starting Spark Job..."
echo "Master: ${MASTER_URL}"
echo "Script: ${PYTHON_SCRIPT_PATH}"
echo "Input: ${INPUT_PATH}"
echo "Output: ${OUTPUT_PATH}"

# Build spark-submit command
# 注意：移除了 --conf spark.plugins=org.pacman.chukonu.ChukonuPlugin 及相关配置，因为新脚本不需要
spark-submit \
    --master ${MASTER_URL} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.pyspark.driver.python=${ENV_PYTHON} \
    --conf spark.pyspark.python=${ENV_PYTHON} \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256m \
    --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.pyspark.jvmStacktrace.enabled=true \
    --conf spark.kryo.unsafe=true \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size=1g \
    --conf spark.local.dir="/work/projects/polyullm/wenjun/tmp" \
    tools/token_count.py \
    --input-path "${INPUT_PATH}" \
    --output-path "${OUTPUT_PATH}" \
    --model-path "${MODEL_PATH}" \
    --tools ${TOOLS} \
    --sample-n ${SAMPLE_N} \
    > "${LOG_FILE}" 2> "${ERR_FILE}"

echo "Job completed. Logs saved to:"
echo "  Standard output: ${LOG_FILE}"
echo "  Standard error:  ${ERR_FILE}"