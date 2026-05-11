#!/bin/bash

# Same defaults as config.sh, but put Spark local shuffle/spill data on node-local RAID.
export CONFIG_PATH="${CONFIG_PATH:-examples/minhash_simple/fineweb_012_year_2013.yaml}"
export SPARK_LOCAL_DIR="${SPARK_LOCAL_DIR:-/raid/${USER:-unknown}/spark_local/${SLURM_JOB_ID:-default}}"

source "$(dirname "${BASH_SOURCE[0]}")/config.sh"

mkdir -p "${SPARK_LOCAL_DIR}"
