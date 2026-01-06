#!/bin/bash
set -x

# Read from environment variables, use defaults if not set
container_mounts="/lustre/projects/polyullm:/lustre/projects/polyullm,/work/projects/polyullm:/work/projects/polyullm"
container_image="/lustre/projects/polyullm/container/chukonu+3.4.1-jdk11-2026010601.sqsh"

# Run the command in the container
srun --nodes=1 --ntasks=1 -w "$head_node" \
    --container-mounts=$container_mounts \
    --container-image=$container_image \
    --container-writable \
    --container-remap-root \
    --pty bash

# than you can run pyspark --master $MASTER_URL to launch a client

