#!/bin/bash

# 数据集
export INPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012"
export OUTPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_v3"
export TEXT_KEY="text"
export SCORE_KEY="stage3_score"

# MinHash 参数（threshold=0.85 时 b=8, r=16 是 datasketch 经验最优组合）
export THRESHOLD="0.85"
export NGRAM_SIZE="5"
export MIN_LENGTH="10"
export NUM_PERM="128"
export B="8"
export R="16"

# 17TB 全量生产参数
# 你的 32 节点作业日志里 DEFAULT_PARALLELISM=1984，7936 个 shuffle partition 太少。
# 先跳过全局 exact dedup，避免 Stage 2 在 17TB 全量上产生超大 driver/task metrics 压力。
export SQL_SHUFFLE_PARTITIONS="65536"
export WCC_PARALLELISM="8192"
export SKIP_EXACT_DEDUP="true"

# Executor 内存：SBATCH --mem=512GB 是节点 hard limit，给 JVM heap 全部 512G 会被 cgroup kill。
# chukonu native + Python UDF workers + JVM overhead 大概要留 96GB 给 native 侧。
export EXECUTOR_MEMORY="416G"
export EXECUTOR_MEMORY_OVERHEAD="64G"

# driver/AppStatusListener 防 OOM：全量输入约 30 万 input tasks，默认 UI/status
# 会在 driver 侧保留太多 task metrics。
export DRIVER_MEMORY="128G"
export DRIVER_CORES="8"
export SPARK_UI_RETAINED_TASKS="1000"
export SPARK_UI_RETAINED_STAGES="50"
export SPARK_UI_RETAINED_JOBS="50"
export SPARK_SQL_UI_RETAINED_EXECUTIONS="20"
export SPARK_UI_TIMELINE_TASKS_MAXIMUM="1000"
export SPARK_EXECUTOR_HEARTBEAT_INTERVAL="60s"
export SPARK_NETWORK_TIMEOUT="600s"

# 断点续跑：每个落盘点（exact/normalized/edges/wcc/keepers/near/isolated/result）
# 看到 _SUCCESS 就自动 reuse；想从某一步重跑就 rm 掉那一步及后续目录。
