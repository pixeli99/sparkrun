#!/bin/bash

# 输入输出路径按实际环境修改
export INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/dclm-baseline-1.0/global-shard_03_of_10/*/*.jsonl.gz"
export OUTPUT_PATH="/work/projects/polyullm/reallm.xyz/spark-runner/demo/dclm-baseline-1.0/shard03"
export FILE_TYPE="json"
export NUM_PARTITIONS="500"
export ADAPTER="dclm"
