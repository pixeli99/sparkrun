#!/bin/bash
#SBATCH --job-name=fw_stage1
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=64
#SBATCH --gpus-per-node=8
#SBATCH --mem=512G
#SBATCH --output=logs/%x-%A_%a.out
#SBATCH --error=logs/%x-%A_%a.err

set -euo pipefail
mkdir -p logs

IMAGE="/lustre/projects/polyullm/container/lmsysorg-sglang+v0.5.6.sqsh"
MOUNTS="/work/projects/polyullm:/work/projects/polyullm,/lustre/projects/polyullm:/lustre/projects/polyullm"
HOME_DIR="/work/projects/polyullm/lipengxiang"

INPUT_DIR="/work/projects/polyullm/pretrain_data/fineweb_standardize/CC-REPLACE-ME"
OUTPUT_DIR="/work/projects/polyullm/lipengxiang_tmp/fineweb_scored/CC-REPLACE-ME"
PY_SCRIPT="/lustre/projects/polyullm/lipengxiang_tmp/sglang_stage1.py"
MODEL_PATH="/work/projects/polyullm/lipengxiang/public_share/stage1_filter_v7_0312"

if [[ ! -d "${INPUT_DIR}" ]]; then
  echo "输入目录不存在: ${INPUT_DIR}"
  exit 1
fi

CC_NAME="$(basename "${INPUT_DIR}")"
mkdir -p "${OUTPUT_DIR}"

echo "[TASK] cc=${CC_NAME}"
echo "[TASK] input=${INPUT_DIR}"
echo "[TASK] output=${OUTPUT_DIR}"

srun --nodes=1 --ntasks=1 \
  --container-name=llamafactory_${SLURM_JOB_ID}_single \
  --container-image="${IMAGE}" \
  --container-mounts="${MOUNTS}" \
  --container-writable \
  bash -lc "
    set -euo pipefail
    export HOME='${HOME_DIR}'
    cd /lustre/projects/polyullm/lipengxiang_tmp
    python ${PY_SCRIPT} \
      --input-dir '${INPUT_DIR}' \
      --output-dir '${OUTPUT_DIR}' \
      --model-path '${MODEL_PATH}' \
      --num-gpus 8
  "
