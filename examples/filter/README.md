# FastText Filter Demo

This example demonstrates how to run a data filtering job using FastText language detection model with Spark.

## Prerequisites

1. Clone this repository

2. Verify that the paths in `submit.sh` have proper write permissions. Pay special attention to:
   - **sbatch logs**: The `--output` and `--error` paths specified in the script
   - **Spark temporary files and logs**: The `tmp_dir` and `cache_dir` configuration. Temporary files should preferably be stored on Lustre filesystem for better performance

3. Ensure the FastText model file exists at the path specified in `run_fasttext_filter.sh`:
   - Default model path: `/lustre/projects/polyullm/reallm.xyz/spark_resource/lid.176.bin`
   - The model file will be distributed to compute nodes via Spark's `--files` option

4. Ensure the Python environment is available at the path specified in `run_fasttext_filter.sh`:
   - Default environment path: `/lustre/projects/polyullm/reallm.xyz/spark_resource/spark_env_20251201_c`
   - The environment should have `fasttext` package installed

## How to Run

Submit the job using the following command:

```bash
sbatch --nodes=<number-of-nodes> submit.sh <config-file> <task-script>
```

**Parameters:**
- `<number-of-nodes>`: The number of compute nodes you want to allocate for the job
- `<config-file>`: Custom configuration file for additional parameters (optional)
- `<task-script>`: The script that defines the Spark job to execute

**Advanced Options:**

You can also customize CPU and memory allocation through sbatch parameters. However, it is recommended to consult with the repository owner or system administrator to evaluate the appropriate resource requirements for your specific workload.

**Example:**

```bash
cp examples/filter/config.sh examples/filter/my_config.sh
# Modify my_config.sh to meet your requirements, e.g., update INPUT_PATH and OUTPUT_PATH
sbatch --nodes=4 submit.sh examples/filter/my_config.sh examples/filter/run_fasttext_filter.sh
```

This example demonstrates the recommended workflow:
1. Copy the default configuration file to create your own custom configuration
2. Modify `my_config.sh` to customize parameters such as input/output paths, file type, and text key
3. Submit the job using 4 nodes with your custom configuration file and the task script