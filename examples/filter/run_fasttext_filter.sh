#!/bin/bash

# ====================================================================
# 1. Environment & Resource Check
# ====================================================================
if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

# Resource defaults (Reads from submit.sh exports)
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-32G}"
# Increase overhead for FastText C++ memory usage (off-heap)
EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-4G}" 
DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-64}"

# Logs
LOG_PATH=${LOG_PATH:-/work/projects/polyullm/wtf/spark/logs/slurm}
mkdir -p ${LOG_PATH}
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/fasttext-filter-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/fasttext-filter-${TIMESTAMP}.err"

# ====================================================================
# 2. Paths & Dependencies configuration
# ====================================================================

# [Critical] Python Environment on Lustre (Must exist!)
# 请修改为你解压后的真实路径
ENV_ROOT="/lustre/projects/polyullm/reallm.xyz/spark_resource/spark_env_20251201_c"
ENV_PYTHON="${ENV_ROOT}/bin/python"

# [Critical] Model File Path on Lustre
# 我们将通过 --files 把这个文件分发到计算节点的本地目录
MODEL_SOURCE="/lustre/projects/polyullm/reallm.xyz/spark_resource/lid.176.bin"
MODEL_FILENAME=$(basename "${MODEL_SOURCE}")

# Python Script
PYTHON_SCRIPT="tools/fasttext_filter.py" 

# Data Paths (Defaults, can be overridden by config.sh)
# INPUT_PATH 和 OUTPUT_PATH 应该在 config.sh 中定义，这里给个默认值防止报错
INPUT_PATH=${INPUT_PATH:-"/lustre/projects/polyullm/reallm.xyz/data/input"}
OUTPUT_PATH=${OUTPUT_PATH:-"/lustre/projects/polyullm/reallm.xyz/data/output"}

# Job Params
THRESHOLD=${THRESHOLD:-"0.4"}
MIN_LENGTH=${MIN_LENGTH:-"200"}

# ====================================================================
# 3. Tuning Parameters
# ====================================================================
# Dynamic partition tuning
SQL_SHUFFLE_PARTITIONS=$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 2)}")

set +x

echo "------------------------------------------------"
echo "Job Config:"
echo "Master: $MASTER_URL"
echo "Python: $ENV_PYTHON"
echo "Model:  $MODEL_SOURCE (Distributed via --files)"
echo "Input:  $INPUT_PATH"
echo "Output: $OUTPUT_PATH"
echo "------------------------------------------------"

# ====================================================================
# 4. Submit Command
# ====================================================================

spark-submit \
    --master ${MASTER_URL} \
    --deploy-mode client \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.pyspark.driver.python=${ENV_PYTHON} \
    --conf spark.pyspark.python=${ENV_PYTHON} \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.files.ignoreCorruptFiles=true \
    --conf spark.network.timeout=300s \
    --conf spark.executor.heartbeatInterval=60s \
    --files "${MODEL_SOURCE}" \
    "${PYTHON_SCRIPT}" \
    --input_path "${INPUT_PATH}" \
    --output_path "${OUTPUT_PATH}" \
    --fasttext_model_dir "./${MODEL_FILENAME}" \
    --threshold_fasttext_score ${THRESHOLD} \
    --ccnt_min_length_th ${MIN_LENGTH} \
    --file_type parquet \
    > "${LOG_FILE}" 2> "${ERR_FILE}"

echo "Job completed."
echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"