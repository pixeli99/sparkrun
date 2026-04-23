#!/bin/bash

# ====================================================================
# 配置脚本 - 支持通过 --export=ALL 传递环境变量
# ====================================================================
# 优先级说明：
#   1. 环境变量（通过 --export=ALL 传递）
#   2. 位置参数（$1, $2 等，由 submit.sh 传递）
#   3. 默认值（脚本内定义）
# ====================================================================

# 默认值定义
DEFAULT_INPUT="/work/projects/polyullm/congkai/pretrain_data/Nemotron-CC-Math-v1/*/*.parquet"
DEFAULT_OUTPUT="/work/projects/polyullm/wtf/data/Nemotron-CC-Math-v1-analysis/report.json"
DEFAULT_LOG_PATH="/work/projects/polyullm/lpx_log/spark/logs/slurm"
DEFAULT_MODEL_PATH="/work/projects/polyullm/models/Qwen/Qwen2.5-0.5B"
DEFAULT_TOOLS="sample count_tokens"
DEFAULT_SAMPLE_N="5"
DEFAULT_TEXT_KEY="text"
DEFAULT_LINE_ID_KEY=""  # 若统计 stage2 (noborder) 输出的正文行,设为 "line_id"
DEFAULT_PROGRESS_INTERVAL_SEC="30"
DEFAULT_CACHE_INPUT="false"
DEFAULT_COUNT_TOTAL_DOCS="false"

# 优先级：环境变量 > 位置参数 > 默认值
export INPUT_PATH="${INPUT_PATH:-${1:-$DEFAULT_INPUT}}"
export OUTPUT_PATH="${OUTPUT_PATH:-${2:-$DEFAULT_OUTPUT}}"
export LOG_PATH="${LOG_PATH:-$DEFAULT_LOG_PATH}"
export MODEL_PATH="${MODEL_PATH:-$DEFAULT_MODEL_PATH}"
export TOOLS="${TOOLS:-$DEFAULT_TOOLS}"
export SAMPLE_N="${SAMPLE_N:-$DEFAULT_SAMPLE_N}"
export TEXT_KEY="${TEXT_KEY:-$DEFAULT_TEXT_KEY}"
export LINE_ID_KEY="${LINE_ID_KEY:-$DEFAULT_LINE_ID_KEY}"
export PROGRESS_INTERVAL_SEC="${PROGRESS_INTERVAL_SEC:-$DEFAULT_PROGRESS_INTERVAL_SEC}"
export CACHE_INPUT="${CACHE_INPUT:-$DEFAULT_CACHE_INPUT}"
export COUNT_TOTAL_DOCS="${COUNT_TOTAL_DOCS:-$DEFAULT_COUNT_TOTAL_DOCS}"

# ====================================================================
# 使用示例：
#   sbatch --nodes=4 submit.sh examples/token_count/config.sh examples/token_count/run_token_count.sh
#   
#   或者通过环境变量传递参数：
#   sbatch --export=ALL,INPUT_PATH="/path/to/input",OUTPUT_PATH="/path/to/output" \
#          submit.sh examples/token_count/config.sh examples/token_count/run_token_count.sh
# ====================================================================
