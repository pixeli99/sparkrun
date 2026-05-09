#!/bin/bash

# Layer 3: merge several Layer 2 year outputs into one era.
#
# Usage:
#   ERA_NAME=era_2013_2016 YEARS=2013,2014,2015,2016 sbatch --nodes=... submit.sh \
#     examples/minhash_dedup/config_layer3_era.sh \
#     examples/minhash_dedup/run_minhash_dedup.sh

if [ -z "${YEARS}" ]; then
    echo "Error: set YEARS, for example YEARS=2013,2014,2015,2016" >&2
    exit 1
fi

export L2_ROOT="${L2_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_l2_year}"
export L3_ROOT="${L3_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_l3_era}"

era_name="${ERA_NAME:-era_${YEARS//,/_}}"
paths=()
IFS=',' read -r -a years_array <<< "${YEARS}"
for year in "${years_array[@]}"; do
    paths+=("${L2_ROOT}/year_${year}/result")
done
input_path=$(IFS=','; echo "${paths[*]}")

export INPUT_PATH="${input_path}"
export OUTPUT_PATH="${L3_ROOT}/${era_name}"
export TEXT_KEY="${TEXT_KEY:-text}"
export SCORE_KEY="${SCORE_KEY:-stage3_score}"
export WEIGHT_KEY="${WEIGHT_KEY:-duplicate_count}"

export THRESHOLD="${THRESHOLD:-0.85}"
export NGRAM_SIZE="${NGRAM_SIZE:-5}"
export MIN_LENGTH="${MIN_LENGTH:-10}"
export NUM_PERM="${NUM_PERM:-128}"
export B="${B:-8}"
export R="${R:-16}"

export SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-32768}"
export MINHASH_INPUT_PARTITIONS="${MINHASH_INPUT_PARTITIONS:-32768}"
export WCC_PARALLELISM="${WCC_PARALLELISM:-8192}"
export SKIP_EXACT_DEDUP="${SKIP_EXACT_DEDUP:-true}"

export DRIVER_MEMORY="${DRIVER_MEMORY:-128G}"
export DRIVER_CORES="${DRIVER_CORES:-8}"
export EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-300G}"
export EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-256G}"
export SPARK_OFFHEAP_SIZE="${SPARK_OFFHEAP_SIZE:-48g}"

export SPARK_UI_RETAINED_TASKS="${SPARK_UI_RETAINED_TASKS:-1000}"
export SPARK_UI_RETAINED_STAGES="${SPARK_UI_RETAINED_STAGES:-50}"
export SPARK_UI_RETAINED_JOBS="${SPARK_UI_RETAINED_JOBS:-50}"
export SPARK_SQL_UI_RETAINED_EXECUTIONS="${SPARK_SQL_UI_RETAINED_EXECUTIONS:-20}"
export SPARK_UI_TIMELINE_TASKS_MAXIMUM="${SPARK_UI_TIMELINE_TASKS_MAXIMUM:-1000}"
export SPARK_EXECUTOR_HEARTBEAT_INTERVAL="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL:-60s}"
export SPARK_NETWORK_TIMEOUT="${SPARK_NETWORK_TIMEOUT:-600s}"
