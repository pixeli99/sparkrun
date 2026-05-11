#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-64G}"
EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-8G}"
DRIVER_MEMORY="${DRIVER_MEMORY:-64G}"
DRIVER_CORES="${DRIVER_CORES:-4}"
DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-256}"
SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 8)}")}"

SPARK_UI_RETAINED_TASKS="${SPARK_UI_RETAINED_TASKS:-1000}"
SPARK_UI_RETAINED_STAGES="${SPARK_UI_RETAINED_STAGES:-50}"
SPARK_UI_RETAINED_JOBS="${SPARK_UI_RETAINED_JOBS:-50}"
SPARK_SQL_UI_RETAINED_EXECUTIONS="${SPARK_SQL_UI_RETAINED_EXECUTIONS:-20}"
SPARK_UI_TIMELINE_TASKS_MAXIMUM="${SPARK_UI_TIMELINE_TASKS_MAXIMUM:-1000}"
SPARK_EXECUTOR_HEARTBEAT_INTERVAL="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL:-60s}"
SPARK_NETWORK_TIMEOUT="${SPARK_NETWORK_TIMEOUT:-600s}"

OUTPUT_PATH=${OUTPUT_PATH:-"/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_minhash"}
WEIGHT_KEY=${WEIGHT_KEY:-"duplicate_count"}
DEBUG_QUICK="${DEBUG_QUICK:-true}"
DEBUG_SAMPLE_LIMIT="${DEBUG_SAMPLE_LIMIT:-20}"

SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-"/lustre/projects/polyullm/lipengxiang_tmp/spark_local/${SLURM_JOB_ID:-default}"}
mkdir -p "${SPARK_LOCAL_DIR}"

LOG_PATH=${LOG_PATH:-/tmp/spark_logs}
mkdir -p "${LOG_PATH}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/debug-minhash-dedup-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/debug-minhash-dedup-${TIMESTAMP}.err"

DEBUG_ARGS=""
case "${DEBUG_QUICK}" in
    1|true|TRUE|yes|YES) DEBUG_ARGS+=" --quick" ;;
esac

set +x

echo "------------------------------------------------"
echo "MinHash dedup debug config:"
echo "  MASTER_URL: ${MASTER_URL}"
echo "  OUTPUT_PATH: ${OUTPUT_PATH}"
echo "  WEIGHT_KEY: ${WEIGHT_KEY}"
echo "  DEBUG_QUICK: ${DEBUG_QUICK}"
echo "  DEBUG_SAMPLE_LIMIT: ${DEBUG_SAMPLE_LIMIT}"
echo "  EXECUTOR_CORES: ${EXECUTOR_CORES}  EXECUTOR_MEMORY: ${EXECUTOR_MEMORY}"
echo "  DRIVER_CORES: ${DRIVER_CORES}  DRIVER_MEMORY: ${DRIVER_MEMORY}"
echo "  DEFAULT_PARALLELISM: ${DEFAULT_PARALLELISM}"
echo "  SQL_SHUFFLE_PARTITIONS: ${SQL_SHUFFLE_PARTITIONS}"
echo "  SPARK_LOCAL_DIR: ${SPARK_LOCAL_DIR}"
echo "  Driver stdout: ${LOG_FILE}"
echo "  Driver stderr: ${ERR_FILE}"
echo "------------------------------------------------"

spark-submit \
    --master ${MASTER_URL} \
    --driver-memory ${DRIVER_MEMORY} \
    --driver-cores ${DRIVER_CORES} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
    --conf spark.ui.retainedTasks=${SPARK_UI_RETAINED_TASKS} \
    --conf spark.ui.retainedStages=${SPARK_UI_RETAINED_STAGES} \
    --conf spark.ui.retainedJobs=${SPARK_UI_RETAINED_JOBS} \
    --conf spark.sql.ui.retainedExecutions=${SPARK_SQL_UI_RETAINED_EXECUTIONS} \
    --conf spark.ui.timeline.tasks.maximum=${SPARK_UI_TIMELINE_TASKS_MAXIMUM} \
    --conf spark.executor.heartbeatInterval=${SPARK_EXECUTOR_HEARTBEAT_INTERVAL} \
    --conf spark.network.timeout=${SPARK_NETWORK_TIMEOUT} \
    --conf spark.shuffle.io.maxRetries="${SPARK_SHUFFLE_IO_MAX_RETRIES:-10}" \
    --conf spark.shuffle.io.retryWait="${SPARK_SHUFFLE_IO_RETRY_WAIT:-30s}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=512m \
    --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionSize=128m \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.unsafe=true \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size="${SPARK_OFFHEAP_SIZE:-16g}" \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.scheduler.outputCommitCoordination.enabled=false \
    --conf spark.task.maxFailures=10 \
    --conf spark.stage.maxConsecutiveAttempts=8 \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=32m -XX:MaxGCPauseMillis=300 -XX:InitiatingHeapOccupancyPercent=35 -XX:+ParallelRefProcEnabled" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    tools/debug_minhash_dedup.py \
    --output_path "${OUTPUT_PATH}" \
    --weight_key "${WEIGHT_KEY}" \
    --sample_limit "${DEBUG_SAMPLE_LIMIT}" \
    ${DEBUG_ARGS} \
    > >(tee -a "${LOG_FILE}") \
    2> >(tee -a "${ERR_FILE}" >&2)

submit_status=$?

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
