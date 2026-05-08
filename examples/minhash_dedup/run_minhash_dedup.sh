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
# shuffle 量大（exact dedup + minhash signature + WCC edges），开 8x 默认并行；
# 全量跑时可用 SQL_SHUFFLE_PARTITIONS 单独覆盖，AQE 会自动 coalesce
SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 8)}")}"
WCC_PARALLELISM="${WCC_PARALLELISM:-${DEFAULT_PARALLELISM}}"

# driver 侧的 AppStatusListener 会保留 task/stage metrics；全量输入有几十万 task，
# 默认 Spark UI/status retention 很容易把 driver heap 打爆。
SPARK_UI_RETAINED_TASKS="${SPARK_UI_RETAINED_TASKS:-1000}"
SPARK_UI_RETAINED_STAGES="${SPARK_UI_RETAINED_STAGES:-50}"
SPARK_UI_RETAINED_JOBS="${SPARK_UI_RETAINED_JOBS:-50}"
SPARK_SQL_UI_RETAINED_EXECUTIONS="${SPARK_SQL_UI_RETAINED_EXECUTIONS:-20}"
SPARK_UI_TIMELINE_TASKS_MAXIMUM="${SPARK_UI_TIMELINE_TASKS_MAXIMUM:-1000}"
SPARK_EXECUTOR_HEARTBEAT_INTERVAL="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL:-60s}"
SPARK_NETWORK_TIMEOUT="${SPARK_NETWORK_TIMEOUT:-600s}"

INPUT_PATH=${INPUT_PATH:-"/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012"}
OUTPUT_PATH=${OUTPUT_PATH:-"/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_minhash"}
TEXT_KEY=${TEXT_KEY:-"text"}
SCORE_KEY=${SCORE_KEY:-"stage3_score"}

THRESHOLD=${THRESHOLD:-"0.85"}
NGRAM_SIZE=${NGRAM_SIZE:-"5"}
MIN_LENGTH=${MIN_LENGTH:-"2"}
NUM_PERM=${NUM_PERM:-"128"}
if [[ ! ${B+x} ]]; then
    B="8"
fi
if [[ ! ${R+x} ]]; then
    R="16"
fi
SKIP_EXACT_DEDUP="${SKIP_EXACT_DEDUP:-false}"

# spark.local.dir 隔离到 per-job 子目录，避免并发 job 互相覆盖 shuffle spill
SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR:-"/lustre/projects/polyullm/lipengxiang_tmp/spark_local/${SLURM_JOB_ID:-default}"}
mkdir -p "${SPARK_LOCAL_DIR}"

LOG_PATH=${LOG_PATH:-/tmp/spark_logs}
mkdir -p "${LOG_PATH}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_PATH}/minhash-dedup-${TIMESTAMP}.log"
ERR_FILE="${LOG_PATH}/minhash-dedup-${TIMESTAMP}.err"

# 只在非空时把 --b / --r 传给 python，否则让脚本走 optimal_param 自动求解
LSH_ARGS=""
[[ -n "${B}" ]] && LSH_ARGS+=" --b ${B}"
[[ -n "${R}" ]] && LSH_ARGS+=" --r ${R}"

EXACT_ARGS=""
case "${SKIP_EXACT_DEDUP}" in
    1|true|TRUE|yes|YES) EXACT_ARGS+=" --skip_exact_dedup" ;;
esac

set +x

echo "------------------------------------------------"
echo "MinHash dedup config:"
echo "  MASTER_URL: ${MASTER_URL}"
echo "  INPUT_PATH: ${INPUT_PATH}"
echo "  OUTPUT_PATH: ${OUTPUT_PATH}"
echo "  TEXT_KEY: ${TEXT_KEY}  SCORE_KEY: ${SCORE_KEY}"
echo "  THRESHOLD: ${THRESHOLD}  NUM_PERM: ${NUM_PERM}  B: ${B:-auto}  R: ${R:-auto}"
echo "  EXECUTOR_CORES: ${EXECUTOR_CORES}  EXECUTOR_MEMORY: ${EXECUTOR_MEMORY}"
echo "  DRIVER_CORES: ${DRIVER_CORES}  DRIVER_MEMORY: ${DRIVER_MEMORY}"
echo "  DEFAULT_PARALLELISM: ${DEFAULT_PARALLELISM}"
echo "  SQL_SHUFFLE_PARTITIONS: ${SQL_SHUFFLE_PARTITIONS}"
echo "  WCC_PARALLELISM: ${WCC_PARALLELISM}"
echo "  SPARK_UI_RETAINED_TASKS: ${SPARK_UI_RETAINED_TASKS}"
echo "  SPARK_SQL_UI_RETAINED_EXECUTIONS: ${SPARK_SQL_UI_RETAINED_EXECUTIONS}"
echo "  SPARK_EXECUTOR_HEARTBEAT_INTERVAL: ${SPARK_EXECUTOR_HEARTBEAT_INTERVAL}"
echo "  SPARK_NETWORK_TIMEOUT: ${SPARK_NETWORK_TIMEOUT}"
echo "  SKIP_EXACT_DEDUP: ${SKIP_EXACT_DEDUP}"
echo "  (auto-reuse on _SUCCESS for each of exact/normalized/edges/wcc/keepers/near/isolated/result)"
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
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=512m \
    --conf spark.sql.adaptive.coalescePartitions.parallelismFirst=false \
    --conf spark.sql.adaptive.coalescePartitions.minPartitionSize=128m \
    --conf spark.sql.files.maxRecordsPerFile=2000000 \
    --conf spark.sql.files.ignoreCorruptFiles=false \
    --conf spark.plugins=org.pacman.chukonu.ChukonuPlugin \
    --conf spark.chukonu.enableNativeCodegen=true \
    --conf spark.chukonu.root=/opt/chukonu_install \
    --conf spark.chukonu.cxx=/usr/bin/g++ \
    --conf spark.chukonu.stagingdir=/opt/chukonu_staging \
    --conf spark.chukonu.compileCacheDir=/opt/chukonu_cache \
    --conf spark.chukonu.buildType=Release \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryo.unsafe=true \
    --conf spark.shuffle.service.enabled=false \
    --conf spark.memory.offHeap.enabled=true \
    --conf spark.memory.offHeap.size="${SPARK_OFFHEAP_SIZE:-16g}" \
    --conf spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version=2 \
    --conf spark.scheduler.outputCommitCoordination.enabled=false \
    --conf spark.task.maxFailures=10 \
    --conf spark.stage.maxConsecutiveAttempts=8 \
    --conf spark.executor.extraJavaOptions="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+ParallelRefProcEnabled" \
    --conf spark.driver.extraJavaOptions="-XX:+UnlockExperimentalVMOptions -XX:+UseZGC -XX:+ParallelRefProcEnabled" \
    --conf spark.local.dir="${SPARK_LOCAL_DIR}" \
    tools/minhash_dedup.py \
    --input_path "${INPUT_PATH}" \
    --output_path "${OUTPUT_PATH}" \
    --text_key "${TEXT_KEY}" \
    --score_key "${SCORE_KEY}" \
    --threshold ${THRESHOLD} \
    --ngram_size ${NGRAM_SIZE} \
    --min_length ${MIN_LENGTH} \
    --num_perm ${NUM_PERM} \
    ${LSH_ARGS} \
    ${EXACT_ARGS} \
    --num_parallel ${WCC_PARALLELISM} \
    > >(tee -a "${LOG_FILE}") \
    2> >(tee -a "${ERR_FILE}" >&2)

submit_status=$?

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
