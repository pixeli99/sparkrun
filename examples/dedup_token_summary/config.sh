#!/bin/bash

# Spark export for final minhash_simple dedup outputs.
# It reads only:
#   <DEDUP_BASE>/<dataset>/year_YYYY/*.parquet
# and writes only rows whose STAGE3_SCORE_KEY casts to KEEP_SCORES.

export DEDUP_BASE="${DEDUP_BASE:-/lustre/projects/polyullm/lipengxiang_tmp/minhash_simple/dedup}"
export JOB_MODE="${JOB_MODE:-filter}"
export FILTERED_OUTPUT_PATH="${FILTERED_OUTPUT_PATH:-/work/projects/polyullm/infra/sync_to_b3/0518}"
export FILTERED_OUTPUT_MODE="${FILTERED_OUTPUT_MODE:-overwrite}"
export KEEP_SCORES="${KEEP_SCORES:-1,2}"
export OUTPUT_PATH="${OUTPUT_PATH:-/lustre/projects/polyullm/lipengxiang_tmp/dedup_tokens_by_year_spark.tsv}"
export TOKEN_KEY="${TOKEN_KEY:-token_count}"
export STAGE3_SCORE_KEY="${STAGE3_SCORE_KEY:-stage3_score}"

export EXECUTOR_CORES="${EXECUTOR_CORES:-8}"
export EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-64G}"
export EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-8G}"
export DRIVER_MEMORY="${DRIVER_MEMORY:-32G}"
export DRIVER_CORES="${DRIVER_CORES:-4}"
export DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-512}"
export SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-512}"

export SPARK_NETWORK_TIMEOUT="${SPARK_NETWORK_TIMEOUT:-600s}"
export SPARK_EXECUTOR_HEARTBEAT_INTERVAL="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL:-60s}"
export SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/lustre/projects/polyullm/lipengxiang_tmp/spark_local/${SLURM_JOB_ID:-dedup_token_summary}}"
mkdir -p "${SPARK_LOCAL_DIR}"

# Lustre paths are read through Hadoop file://. Hadoop LocalFileSystem checks
# hidden .*.crc sidecar files; these can become stale after overwrite/copy and
# trigger ChecksumException even when the parquet file is readable. RawLocalFS
# bypasses those local checksum sidecars.
export USE_RAW_LOCAL_FS="${USE_RAW_LOCAL_FS:-true}"

# Last resort: true skips unreadable parquet files, so totals can be lower.
export IGNORE_CORRUPT_FILES="${IGNORE_CORRUPT_FILES:-true}"

export LOG_PATH="${LOG_PATH:-/tmp/spark_logs}"
mkdir -p "${LOG_PATH}"
