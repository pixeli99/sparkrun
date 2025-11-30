# Dedup Demo

This example demonstrates how to run a deduplication and weakly connected components (WCC) computation job using Spark.

## Prerequisites

1. Clone this repository

2. Verify that the paths in `submit.sh` have proper write permissions. Pay special attention to:
   - **sbatch logs**: The `--output` and `--error` paths specified in the script
   - **Spark temporary files and logs**: The `tmp_dir` configuration. Temporary files should preferably be stored on Lustre filesystem for better performance

## How to Run

Submit the job using the following command:

```bash
sbatch --nodes=<number-of-nodes> submit.sh <config-file> <task-script>
```

**Parameters:**
- `<number-of-nodes>`: The number of compute nodes you want to allocate for the job
- `<config-file>`: Custom configuration file for additional parameters (optional)
- `<task-script>`: The script that defines the Spark job to execute

## Advanced Options

You can also customize CPU and memory allocation through sbatch parameters. However, it is recommended to consult with the repository owner or system administrator to evaluate the appropriate resource requirements for your specific workload.

**Example:**

```bash
sbatch --nodes=16 submit.sh examples/dedup_wcc/config.sh examples/dedup_wcc/run_dedup_wcc.sh
```

This example submits a job using 16 nodes with the configuration file `examples/dedup_wcc/config.sh` and the task script `examples/dedup_wcc/run_dedup_wcc.sh`.