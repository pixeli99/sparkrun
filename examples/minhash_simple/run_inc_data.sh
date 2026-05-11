#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/minhash-simple-inc-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/minhash-simple-inc-${TIMESTAMP}.err"

set +x

spark-submit \
    --master "${MASTER_URL}" \
    --executor-cores "${EXECUTOR_CORES}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --driver-cores "${DRIVER_CORES}" \
    --conf spark.executor.memoryOverhead="${EXECUTOR_MEMORY_OVERHEAD}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256m \
    --conf spark.sql.shuffle.partitions="${SQL_SHUFFLE_PARTITIONS}" \
    --conf spark.default.parallelism="${DEFAULT_PARALLELISM}" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.network.timeout="${SPARK_NETWORK_TIMEOUT}" \
    --conf spark.executor.heartbeatInterval="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL}" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    --py-files tools/minhash_simple/utils.py \
    tools/minhash_simple/inc_data.py \
    "${CONFIG_PATH}" \
    > "${LOG_FILE}" 2> "${ERR_FILE}"

echo "Job completed. Logs:"
echo "  stdout: ${LOG_FILE}"
echo "  stderr: ${ERR_FILE}"
