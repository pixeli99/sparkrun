#!/bin/bash

# dataset parameters
export INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/Nemotron-CC-Math-v1/*/*.parquet"
export OUTPUT_PATH="/work/projects/polyullm/wtf/data/Nemotraon-CC-Math-v1-dedup"
export FILE_TYPE="parquet"
export TEXT_KEY="text"

# dedup parameters
export THRESHOLD="0.85"
export NGRAM_SIZE="5"
export MIN_LENGTH="2"
export NUM_PERM="128"
export B="8"
export R="16"
export WITH_SPLIT="False"
export RUN_CHUKONU="True"