#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-16G}"
EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-2G}"
DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-32}"
SQL_SHUFFLE_PARTITIONS=$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 2)}")

INPUT_PATH=${INPUT_PATH:-"/path/to/fineweb/input"}
OUTPUT_PATH=${OUTPUT_PATH:-"/path/to/fineweb/standard"}
FILE_TYPE=${FILE_TYPE:-"parquet"}
ADAPTER=${ADAPTER:-"fineweb"}
NUM_PARTITIONS=${NUM_PARTITIONS:-"0"}
PARTITION_SIZE_MB=${PARTITION_SIZE_MB:-"0"}

LOG_PATH=${LOG_PATH:-/tmp/spark_logs}
mkdir -p "${LOG_PATH}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/fineweb-standardize-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/fineweb-standardize-${TIMESTAMP}.err"

set +x

export PYTHONPATH="$(pwd)/tools:${PYTHONPATH}"

spark-submit \
    --master ${MASTER_URL} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.executorEnv.PYTHONPATH="$(pwd)/tools" \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
    --conf spark.sql.files.ignoreCorruptFiles=true \
    tools/standardize.py \
    --adapter "${ADAPTER}" \
    --input_path "${INPUT_PATH}" \
    --output_path "${OUTPUT_PATH}" \
    --file_type "${FILE_TYPE}" \
    --num_partitions "${NUM_PARTITIONS}" \
    --partition_size_mb "${PARTITION_SIZE_MB}" \
    > "${LOG_FILE}" 2> "${ERR_FILE}"

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
