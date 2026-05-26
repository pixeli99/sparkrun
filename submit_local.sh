#!/bin/bash
#SBATCH --job-name=spark
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=500GB
#SBATCH --cpus-per-task=64
#SBATCH --output=logs/slurm/%j-spark.out
#SBATCH --error=logs/slurm/%j-spark.err
#SBATCH --exclude=kb3-a1-nv-dgx01,kb3-a1-nv-dgx04
# set -x

# ========================================================
# replace these information with your own
# ========================================================
workdir=$(pwd)
tmp_dir=${SPARK_TMP_ROOT:-/raid/enroot/runtime/user-100736}

# cache_dir=$tmp_dir/chukonu_cache
container_image=/lustre/projects/polyullm/pretrain/container/spark_simhash.sqsh
container_name=chukonu-spark-${SLURM_JOB_ID:-manual}
container_mounts=/lustre/projects/polyullm:/lustre/projects/polyullm,/work/projects/polyullm:/work/projects/polyullm
# ========================================================

# Getting the node names
nodes=$(scontrol show hostnames "$SLURM_JOB_NODELIST")
nodes_array=($nodes)

# Get the IP address of the head node
head_node=${nodes_array[0]}
head_node_ip=$(srun --nodes=1 --ntasks=1 -w "$head_node" hostname --ip-address)

# Start Spark Master
port=$((17077 + (${SLURM_JOB_ID:-0} % 20000)))
master_webui_port=$((28031 + (${SLURM_JOB_ID:-0} % 20000)))
ip_head=$head_node_ip:$port
export ip_head
echo "IP Head: $ip_head"
echo "Master web UI port: $master_webui_port"

# create tmp folder
# mkdir -p ${tmp_dir}
# chmod -R 777 ${tmp_dir}
# mkdir -p ${cache_dir}

printenv

echo "Starting Spark Master at $head_node"
# mkdir -p ${tmp_dir}/${head_node}/{tmp,logs,work}
# chmod -R 777 ${tmp_dir}/${head_node}
head_mounts=${container_mounts},${tmp_dir}:/tmp
srun --nodes=1 --ntasks=1 -w "$head_node" \
    --container-name=$container_name \
    --container-mounts=$head_mounts \
    --container-image=$container_image \
    --container-writable \
    --container-remap-root \
    bash -c "bash /opt/spark/sbin/start-master.sh -h $head_node_ip --port $port --webui-port $master_webui_port && tail -f /dev/null" &

sleep 5

# number of nodes other than the head node
worker_num=$((SLURM_JOB_NUM_NODES - 1))
if (( worker_num < 1 )); then
    echo "Error: Spark standalone mode needs at least 2 SLURM nodes: 1 master + >=1 worker."
    echo "Submit with: sbatch --nodes=<N>=2 submit.sh <config.sh> <run_task.sh>"
    exit 1
fi

for ((i = 1; i <= worker_num; i++)); do
    node_i=${nodes_array[$i]}
    # echo "create tmp folder"
    # mkdir -p ${tmp_dir}/${node_i}/{tmp,logs,work}
    # chmod -R 777 ${tmp_dir}/${node_i}
    worker_mounts=${container_mounts},${tmp_dir}:/tmp
    echo "Starting Spark Worker $i at $node_i"
    srun --nodes=1 --ntasks=1 -w "$node_i" \
        --container-name=$container_name \
        --container-mounts=$worker_mounts \
        --container-image=$container_image \
        --container-writable \
        --container-remap-root \
        bash -c "bash /opt/spark/sbin/start-worker.sh -d /opt/spark/work spark://$head_node_ip:$port && tail -f /dev/null" &
        sleep 5
done

config_script=$1
task_script=$2

master_url=spark://$head_node_ip:$port
executor_cores=$(( $SLURM_CPUS_PER_TASK ))
executor_memory=$(( $SLURM_MEM_PER_NODE / 1024 ))G
default_parallelism=$(( $SLURM_CPUS_PER_TASK * $worker_num ))
# spark_local_dir=${SPARK_LOCAL_DIR:-/tmp/spark_local}

echo "================ run task ========================"
echo "config_script: $config_script"
echo "task_script: $task_script"
echo "master_url: $master_url"
echo "executor_cores: $executor_cores"
echo "executor_memory: $executor_memory"
echo "default_parallelism: $default_parallelism"
echo "slurm_job_id: $SLURM_JOB_ID"
echo "log_dir: $tmp_dir"
# echo "spark_local_dir: $spark_local_dir"
echo "=================================================="

echo "sleep 60 seconds"
sleep 60


SCRIPTS="
export MASTER_URL='$master_url'
export EXECUTOR_CORES='$executor_cores'
export EXECUTOR_MEMORY='$executor_memory'
export DEFAULT_PARALLELISM='$default_parallelism'
export SLURM_JOB_ID='$SLURM_JOB_ID'
export LOG_PATH='$tmp_dir'

cd '$workdir' &&
source '$config_script' &&
bash '$task_script'"

PYTHONUNBUFFERED=1 srun --overlap --nodes=1 --ntasks=1 -w "$head_node" \
    --container-name=$container_name \
    bash -c "$SCRIPTS"

# Clean up Spark processes
cleanup() {
    echo "Shutting down Spark cluster..."
    srun --overlap --nodes=1 --ntasks=1 -w "$head_node" \
        --container-name=$container_name \
        --container-image=$container_image \
        --container-writable \
        --container-remap-root \
        bash -c "bash /opt/spark/sbin/stop-master.sh"

    for ((i = 1; i <= worker_num; i++)); do
        node_i=${nodes_array[$i]}
        srun --overlap --nodes=1 --ntasks=1 -w "$node_i" \
            --container-name=$container_name \
            --container-image=$container_image \
            --container-writable \
            --container-remap-root \
            bash -c "bash /opt/spark/sbin/stop-worker.sh"
    done
}

# Set up trap to call cleanup function on script exit
trap cleanup EXIT
