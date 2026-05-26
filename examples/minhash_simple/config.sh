#!/bin/bash

# tools/minhash_simple 两个阶段共用的环境变量。
# 1.4TB 量级建议先按 lang/year 切片再跑，单次 input 控制在 200-400GB。

# Stage 1 (save_hash_key.py) 和 Stage 2 (inc_data.py) 都吃这同一个 YAML 路径。
export CONFIG_PATH="${CONFIG_PATH:-examples/minhash_simple/example_zh.yaml}"

# ---------- spark 资源 ----------
# 纯 Python UDF（jieba + numpy minhash）每行都过 Python worker，
# 比 minhash_dedup.py 的 chukonu native 慢一个数量级，所以更多 executor 数能拉一些回来。
export EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
export EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-64G}"
export EXECUTOR_MEMORY_OVERHEAD="${EXECUTOR_MEMORY_OVERHEAD:-16G}"
export DRIVER_MEMORY="${DRIVER_MEMORY:-64G}"
export DRIVER_CORES="${DRIVER_CORES:-4}"
export DEFAULT_PARALLELISM="${DEFAULT_PARALLELISM:-2048}"

# minhash 签名扁平展开后行数 = 输入 doc 数 * num_buckets (默认 32)，shuffle 量大
export SQL_SHUFFLE_PARTITIONS="${SQL_SHUFFLE_PARTITIONS:-$(awk "BEGIN {print int(${DEFAULT_PARALLELISM} * 8)}")}"

export SPARK_NETWORK_TIMEOUT="${SPARK_NETWORK_TIMEOUT:-600s}"
export SPARK_EXECUTOR_HEARTBEAT_INTERVAL="${SPARK_EXECUTOR_HEARTBEAT_INTERVAL:-60s}"

# 容器内 /tmp 由 submit_local.sh 把宿主机 /raid/enroot/runtime/user-100736 mount 进来，
# 是每节点本地 NVMe。SPARK_LOCAL_DIR 必须落到 /tmp 下面，shuffle 才会走本地盘。
# 写 lustre 路径会绕开 mount，直接打爆 lustre（已踩坑，参考 81527）。
export SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/tmp/spark_local/${SLURM_JOB_ID:-default}}"
mkdir -p "${SPARK_LOCAL_DIR}"

# Lustre paths are read via Hadoop file://. RawLocalFS bypasses hidden .*.crc
# sidecar checks, which can be stale after overwrite/copy.
export USE_RAW_LOCAL_FS="${USE_RAW_LOCAL_FS:-true}"

# Last resort: true skips unreadable/corrupt parquet files. This can make
# outputs incomplete, so keep false unless you have confirmed bad inputs.
export IGNORE_CORRUPT_FILES="${IGNORE_CORRUPT_FILES:-true}"

export LOG_PATH="${LOG_PATH:-/tmp/spark_logs}"
mkdir -p "${LOG_PATH}"
