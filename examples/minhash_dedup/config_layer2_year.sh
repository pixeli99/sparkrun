#!/bin/bash

# Layer 2: merge already-deduped Layer 1-equivalent outputs within one CC-MAIN year.
#
# By default this reads directories like:
#   ${L1_ROOT}/CC-MAIN-2014-41
#
# If your Layer 1 was produced by this script and lives under per-crawl result/
# directories, set:
#   L1_RESULT_SUFFIX=result
#
# Usage:
#   YEAR=2014 sbatch --nodes=... submit.sh \
#     examples/minhash_dedup/config_layer2_year.sh \
#     examples/minhash_dedup/run_minhash_dedup.sh
#
# Or with a Slurm array whose task id is the year:
#   sbatch --array=2013-2026 --nodes=... submit.sh ...

year="${YEAR:-${SLURM_ARRAY_TASK_ID:-}}"
if [ -z "${year}" ]; then
    echo "Error: set YEAR or use SLURM_ARRAY_TASK_ID as the year" >&2
    exit 1
fi

export L1_ROOT="${L1_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_l1}"
export L2_ROOT="${L2_ROOT:-/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_l2_year}"
export L1_RESULT_SUFFIX="${L1_RESULT_SUFFIX:-}"

suffix_path=""
if [ -n "${L1_RESULT_SUFFIX}" ]; then
    suffix_path="/${L1_RESULT_SUFFIX#/}"
fi

export INPUT_PATH="${L1_ROOT}/CC-MAIN-${year}-*${suffix_path}"
export OUTPUT_PATH="${L2_ROOT}/year_${year}"
export TEXT_KEY="${TEXT_KEY:-text}"
export SCORE_KEY="${SCORE_KEY:-stage3_score}"
export WEIGHT_KEY="${WEIGHT_KEY:-duplicate_count}"

export THRESHOLD="${THRESHOLD:-0.85}"
export NGRAM_SIZE="${NGRAM_SIZE:-5}"
export MIN_LENGTH="${MIN_LENGTH:-10}"
export NUM_PERM="${NUM_PERM:-128}"
export B="${B:-8}"
export R="${R:-16}"

export SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-16384}"
export MINHASH_INPUT_PARTITIONS="${MINHASH_INPUT_PARTITIONS:-16384}"
export WCC_PARALLELISM="${WCC_PARALLELISM:-4096}"
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
