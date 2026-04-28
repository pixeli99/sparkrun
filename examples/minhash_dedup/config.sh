#!/bin/bash

# 数据集
export INPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012"
export OUTPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012_minhash"
export TEXT_KEY="text"
export SCORE_KEY="stage3_score"

# MinHash 参数（threshold=0.85 时 b=8, r=16 是 datasketch 经验最优组合）
export THRESHOLD="0.85"
export NGRAM_SIZE="5"
export MIN_LENGTH="2"
export NUM_PERM="128"
export B="8"
export R="16"
