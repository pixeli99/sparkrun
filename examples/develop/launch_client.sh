#!/bin/bash
set -x

# Read from environment variables, use defaults if not set
container_mounts="/lustre/projects/polyullm:/lustre/projects/polyullm,/work/projects/polyullm:/work/projects/polyullm,/opt/chukonu_cache:/opt/chukonu_cache"
container_image="/lustre/projects/polyullm/container/chukonu+3.4.1-20250818.sqsh"

# Run the command in the container
srun --nodes=1 --ntasks=1 -w "$head_node" \
    --container-mounts=$container_mounts \
    --container-image=$container_image \
    --container-writable \
    --pty bash

# than you can run pyspark --master $MASTER_URL to launch a client

