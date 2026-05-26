#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/minhash-simple-inc-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/minhash-simple-inc-${TIMESTAMP}.err"

# /raid 上的 spark-submit 日志旁路到 lustre：driver 继续写 NVMe（快），
# tail -F 进程从 NVMe 读、追加到 lustre。lustre 慢只拖 tail 自己，不阻塞 driver。
# job 结束 enroot 拆容器前 trap 收尾，做一次 final cp 保最后几行。
PERSIST_DIR="${SPARKRUN_PERSIST_LOG_DIR:-/lustre/projects/polyullm/lipengxiang_tmp/sparkrun/lpx_log}/${SLURM_JOB_ID:-manual}"
mkdir -p "${PERSIST_DIR}"
PERSIST_LOG="${PERSIST_DIR}/$(basename "${LOG_FILE}")"
PERSIST_ERR="${PERSIST_DIR}/$(basename "${ERR_FILE}")"

: > "${LOG_FILE}"
: > "${ERR_FILE}"
: > "${PERSIST_LOG}"
: > "${PERSIST_ERR}"

tail -n +1 -F "${LOG_FILE}" >> "${PERSIST_LOG}" 2>/dev/null &
TAIL_LOG_PID=$!
tail -n +1 -F "${ERR_FILE}" >> "${PERSIST_ERR}" 2>/dev/null &
TAIL_ERR_PID=$!

cleanup_log_mirror() {
    sleep 3   # 给 driver 最后几行刷盘的时间
    kill "${TAIL_LOG_PID}" "${TAIL_ERR_PID}" 2>/dev/null || true
    wait "${TAIL_LOG_PID}" "${TAIL_ERR_PID}" 2>/dev/null || true
    cp -p "${LOG_FILE}" "${PERSIST_LOG}" 2>/dev/null || true
    cp -p "${ERR_FILE}" "${PERSIST_ERR}" 2>/dev/null || true
}
trap cleanup_log_mirror EXIT

set +x

echo "------------------------------------------------"
echo "MinHash simple inc config:"
echo "  MASTER_URL: ${MASTER_URL}"
echo "  CONFIG_PATH: ${CONFIG_PATH}"
echo "  USE_RAW_LOCAL_FS: ${USE_RAW_LOCAL_FS}"
echo "  IGNORE_CORRUPT_FILES: ${IGNORE_CORRUPT_FILES}"
echo "  LOG_FILE: ${LOG_FILE}  (mirror -> ${PERSIST_LOG})"
echo "  ERR_FILE: ${ERR_FILE}  (mirror -> ${PERSIST_ERR})"
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
    --executor-cores "${EXECUTOR_CORES}" \
    --executor-memory "${EXECUTOR_MEMORY}" \
    --driver-memory "${DRIVER_MEMORY}" \
    --driver-cores "${DRIVER_CORES}" \
    --conf spark.executor.memoryOverhead="${EXECUTOR_MEMORY_OVERHEAD}" \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256m \
    --conf spark.sql.files.ignoreCorruptFiles="${IGNORE_CORRUPT_FILES}" \
    --conf spark.sql.shuffle.partitions="${SQL_SHUFFLE_PARTITIONS}" \
    --conf spark.default.parallelism="${DEFAULT_PARALLELISM}" \
    --conf spark.sql.execution.arrow.pyspark.enabled=true \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.network.timeout="${SPARK_NETWORK_TIMEOUT}" \
    --conf spark.executor.heartbeatInterval="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL}" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    "${EXTRA_SPARK_CONF[@]}" \
    --py-files tools/minhash_simple/utils.py \
    tools/minhash_simple/inc_data.py \
    "${CONFIG_PATH}" \
    > "${LOG_FILE}" 2> "${ERR_FILE}"

submit_status=$?

echo "Job completed. Logs:"
echo "  stdout: ${LOG_FILE}"
echo "  stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
