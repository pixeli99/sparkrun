#!/bin/bash

# dataset parameters
INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/Nemotron-CC-Math-v1/*/*.parquet"
OUTPUT_PATH="/work/projects/polyullm/wtf/data/Nemotraon-CC-Math-v1-dedup"
FILE_TYPE="parquet"
TEXT_KEY="text"

# dedup parameters
THRESHOLD="0.85"
NGRAM_SIZE="5"
MIN_LENGTH="2"
NUM_PERM="128"
B="8"
R="16"
WITH_SPLIT="False"
RUN_CHUKONU="True"