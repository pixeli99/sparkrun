#!/bin/bash


# export INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/dolma3_mix-6T-1025/data/stack_edu-C"
# export OUTPUT_PATH="/work/projects/polyullm/reallm.xyz/spark-runner/demo/dolma3_mix-6T-1025/stack-edu-C"
# export FILE_TYPE="json"
# export NUM_PARTITIONS="100"
# export ADAPTER="dolma3_mix_code"


# you can use this example to inject environment variables into the slurm jobs
# INPUT_PATH="/work/projects/polyullm/congkai/pretrain_data/dolma3_mix-6T-1025/data/stack_edu-C"
# OUTPUT_PATH="/work/projects/polyullm/reallm.xyz/spark-runner/demo/dolma3_mix-6T-1025/stack-edu-C"
# FILE_TYPE="json"
# NUM_PARTITIONS="100"
# ADAPTER="dolma3_mix_code"

# sbatch --reservation=megatron \
#     --export=ALL,INPUT_PATH="$INPUT_PATH",OUTPUT_PATH="$OUTPUT_PATH",FILE_TYPE="$FILE_TYPE",NUM_PARTITIONS="$NUM_PARTITIONS",ADAPTER="$ADAPTER" \
#     submit.sh examples/fineweb_standardize/empty.sh examples/fineweb_standardize/run_fineweb_standardize.sh