#!/bin/bash

# 数据集
export INPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012"
export OUTPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_dedup_v2"
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
# Stage 2 现在是窄 shuffle exact dedup，正式跑保留 exact dedup，不开启 skip。
export SQL_SHUFFLE_PARTITIONS="65536"
export WCC_PARALLELISM="8192"
export SKIP_EXACT_DEDUP="false"

# 断点续跑参数：只有确认对应目录存在 _SUCCESS 时再改成 true。
export REUSE_EXACT="false"
export REUSE_WCC="false"
