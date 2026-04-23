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
OUTPUT_PATH=${OUTPUT_PATH:-"/work/projects/polyullm/wtf/data/Nemotron-CC-Math-v1-analysis/report.json"}

# Model Path (Tokenizer)
# 必须是所有 Worker 节点都能访问的路径
MODEL_PATH=${MODEL_PATH:-"/work/projects/polyullm/models/Qwen/Qwen2.5-0.5B"}

# Tools configuration
TOOLS=${TOOLS:-"sample count_tokens"}
SAMPLE_N=${SAMPLE_N:-"5"}
TEXT_KEY=${TEXT_KEY:-"text"}
# 若统计 stage2 (noborder) 输出,设为 "line_id" 即可按正文行号过滤后再统计
LINE_ID_KEY=${LINE_ID_KEY:-""}
PROGRESS_INTERVAL_SEC=${PROGRESS_INTERVAL_SEC:-"30"}
CACHE_INPUT=${CACHE_INPUT:-"false"}
COUNT_TOTAL_DOCS=${COUNT_TOTAL_DOCS:-"false"}

# [Critical] Python Environment on Lustre (Must exist!)
# 请修改为你解压后的真实路径
ENV_ROOT="/work/projects/polyullm/wenjun/envs/spark_nlp_1"
ENV_PYTHON="${ENV_ROOT}/bin/python"

set +x

echo "Starting Spark Job..."
echo "Master: ${MASTER_URL}"
echo "Input: ${INPUT_PATH}"
echo "Output: ${OUTPUT_PATH}"
echo "TextKey: ${TEXT_KEY}"
echo "LineIdKey: ${LINE_ID_KEY:-<none>}"
echo "ProgressIntervalSec: ${PROGRESS_INTERVAL_SEC}"
echo "CacheInput: ${CACHE_INPUT}"
echo "CountTotalDocs: ${COUNT_TOTAL_DOCS}"

LINE_ID_ARG=()
if [ -n "${LINE_ID_KEY}" ]; then
    LINE_ID_ARG=(--line-id-key "${LINE_ID_KEY}")
fi

CACHE_INPUT_ARG=()
if [[ "${CACHE_INPUT}" =~ ^([Tt][Rr][Uu][Ee]|1|[Yy][Ee][Ss]|[Oo][Nn])$ ]]; then
    CACHE_INPUT_ARG=(--cache-input)
fi

COUNT_TOTAL_DOCS_ARG=()
if [[ "${COUNT_TOTAL_DOCS}" =~ ^([Tt][Rr][Uu][Ee]|1|[Yy][Ee][Ss]|[Oo][Nn])$ ]]; then
    COUNT_TOTAL_DOCS_ARG=(--count-total-docs)
fi

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
    --conf spark.ui.showConsoleProgress=true \
    --conf spark.local.dir="/work/projects/polyullm/wenjun/tmp" \
    tools/token_count.py \
    --input-path "${INPUT_PATH}" \
    --output-path "${OUTPUT_PATH}" \
    --model-path "${MODEL_PATH}" \
    --text-key "${TEXT_KEY}" \
    "${LINE_ID_ARG[@]}" \
    --tools ${TOOLS} \
    --sample-n ${SAMPLE_N} \
    --progress-interval-sec "${PROGRESS_INTERVAL_SEC}" \
    "${CACHE_INPUT_ARG[@]}" \
    "${COUNT_TOTAL_DOCS_ARG[@]}" \
    > >(tee -a "${LOG_FILE}") \
    2> >(tee -a "${ERR_FILE}" >&2)

submit_status=$?

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
