#!/bin/bash

# 输入输出路径按实际环境修改
export INPUT_PATH="/work/projects/polyullm/pretrain_data/fineweb/CC-MAIN-2013-20/data/CC-MAIN-2013-20/*.parquet"
export OUTPUT_PATH="/work/projects/polyullm/pretrain_data/fineweb_standardize/CC-MAIN-2013-20"
export FILE_TYPE="parquet"
export ADAPTER="fineweb"
export NUM_PARTITIONS="0"
export PARTITION_SIZE_MB="512"