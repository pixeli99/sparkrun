#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/dedup-token-summary-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/dedup-token-summary-${TIMESTAMP}.err"

set +x

echo "------------------------------------------------"
echo "Dedup token summary config:"
echo "  MASTER_URL: ${MASTER_URL}"
echo "  DEDUP_BASE: ${DEDUP_BASE}"
echo "  JOB_MODE: ${JOB_MODE}"
echo "  FILTERED_OUTPUT_PATH: ${FILTERED_OUTPUT_PATH}"
echo "  FILTERED_OUTPUT_MODE: ${FILTERED_OUTPUT_MODE}"
echo "  KEEP_SCORES: ${KEEP_SCORES}"
echo "  OUTPUT_PATH: ${OUTPUT_PATH}"
echo "  TOKEN_KEY: ${TOKEN_KEY}"
echo "  STAGE3_SCORE_KEY: ${STAGE3_SCORE_KEY}"
echo "  EXECUTOR_CORES: ${EXECUTOR_CORES}"
echo "  EXECUTOR_MEMORY: ${EXECUTOR_MEMORY}"
echo "  DRIVER_MEMORY: ${DRIVER_MEMORY}"
echo "  DEFAULT_PARALLELISM: ${DEFAULT_PARALLELISM}"
echo "  SQL_SHUFFLE_PARTITIONS: ${SQL_SHUFFLE_PARTITIONS}"
echo "  SPARK_LOCAL_DIR: ${SPARK_LOCAL_DIR}"
echo "  USE_RAW_LOCAL_FS: ${USE_RAW_LOCAL_FS}"
echo "  IGNORE_CORRUPT_FILES: ${IGNORE_CORRUPT_FILES}"
echo "  Driver stdout: ${LOG_FILE}"
echo "  Driver stderr: ${ERR_FILE}"
echo "------------------------------------------------"

EXTRA_SPARK_CONF=()
case "${USE_RAW_LOCAL_FS}" in
    1|true|TRUE|yes|YES)
        EXTRA_SPARK_CONF+=(
            --conf spark.hadoop.fs.file.impl=org.apache.hadoop.fs.RawLocalFileSystem
        )
        ;;
esac

spark-submit \
    --master "${MASTER_URL}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --driver-cores "${DRIVER_CORES}" \
    --executor-cores "${EXECUTOR_CORES}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --conf spark.executor.memoryOverhead="${EXECUTOR_MEMORY_OVERHEAD}" \
    --conf spark.default.parallelism="${DEFAULT_PARALLELISM}" \
    --conf spark.sql.shuffle.partitions="${SQL_SHUFFLE_PARTITIONS}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=512m \
    --conf spark.sql.files.ignoreCorruptFiles="${IGNORE_CORRUPT_FILES}" \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.executor.heartbeatInterval="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL}" \
    --conf spark.network.timeout="${SPARK_NETWORK_TIMEOUT}" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    "${EXTRA_SPARK_CONF[@]}" \
    tools/dedup_token_summary.py \
    --base "${DEDUP_BASE}" \
    --mode "${JOB_MODE}" \
    --output "${OUTPUT_PATH}" \
    --filtered-output "${FILTERED_OUTPUT_PATH}" \
    --filtered-output-mode "${FILTERED_OUTPUT_MODE}" \
    --keep-scores "${KEEP_SCORES}" \
    --token-key "${TOKEN_KEY}" \
    --score-key "${STAGE3_SCORE_KEY}" \
    > >(tee -a "${LOG_FILE}") \
    2> >(tee -a "${ERR_FILE}" >&2)

submit_status=$?

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
