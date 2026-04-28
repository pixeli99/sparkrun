#!/bin/bash

if [ -z "${MASTER_URL}" ]; then
    echo "Error: MASTER_URL is required but not set"
    exit 1
fi

set -x

EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-64G}"
EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-8G}"
DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-256}"
# shuffle 量大（minhash signature + WCC edges），开 4x 默认并行；AQE 会自动 coalesce
SQL_SHUFFLE_PARTITIONS=$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 4)}")

INPUT_PATH=${INPUT_PATH:-"/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012"}
OUTPUT_PATH=${OUTPUT_PATH:-"/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_minhash"}
TEXT_KEY=${TEXT_KEY:-"text"}
SCORE_KEY=${SCORE_KEY:-"stage3_score"}

THRESHOLD=${THRESHOLD:-"0.85"}
NGRAM_SIZE=${NGRAM_SIZE:-"5"}
MIN_LENGTH=${MIN_LENGTH:-"2"}
NUM_PERM=${NUM_PERM:-"128"}
B=${B:-"8"}
R=${R:-"16"}

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

set +x

echo "------------------------------------------------"
echo "MinHash dedup config:"
echo "  MASTER_URL: ${MASTER_URL}"
echo "  INPUT_PATH: ${INPUT_PATH}"
echo "  OUTPUT_PATH: ${OUTPUT_PATH}"
echo "  TEXT_KEY: ${TEXT_KEY}  SCORE_KEY: ${SCORE_KEY}"
echo "  THRESHOLD: ${THRESHOLD}  NUM_PERM: ${NUM_PERM}  B: ${B:-auto}  R: ${R:-auto}"
echo "  EXECUTOR_CORES: ${EXECUTOR_CORES}  EXECUTOR_MEMORY: ${EXECUTOR_MEMORY}"
echo "  DEFAULT_PARALLELISM: ${DEFAULT_PARALLELISM}"
echo "  SQL_SHUFFLE_PARTITIONS: ${SQL_SHUFFLE_PARTITIONS}"
echo "  SPARK_LOCAL_DIR: ${SPARK_LOCAL_DIR}"
echo "  Driver stdout: ${LOG_FILE}"
echo "  Driver stderr: ${ERR_FILE}"
echo "------------------------------------------------"

spark-submit \
    --master ${MASTER_URL} \
    --executor-cores ${EXECUTOR_CORES} \
    --executor-memory ${EXECUTOR_MEMORY} \
    --conf spark.executor.memoryOverhead=${EXECUTOR_MEMORY_OVERHEAD} \
    --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
    --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    --conf spark.sql.adaptive.skewJoin.enabled=true \
    --conf spark.sql.adaptive.advisoryPartitionSizeInBytes=256m \
    --conf spark.sql.files.ignoreCorruptFiles=true \
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
    --conf spark.memory.offHeap.size=4g \
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
    --num_parallel ${DEFAULT_PARALLELISM} \
    > >(tee -a "${LOG_FILE}") \
    2> >(tee -a "${ERR_FILE}" >&2)

submit_status=$?

echo "Stdout: ${LOG_FILE}"
echo "Stderr: ${ERR_FILE}"
echo "spark-submit exit code: ${submit_status}"

exit "${submit_status}"
