#!/bin/bash

# 输入输出路径按实际环境修改
export INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/fineweb-edu/100BT/*.parquet"
export OUTPUT_PATH="/work/projects/polyullm/reallm.xyz/spark-runner/demo/fineweb_standardize/100BT"
export FILE_TYPE="parquet"
export NUM_PARTITIONS="100"
export ADAPTER="fineweb_edu"
