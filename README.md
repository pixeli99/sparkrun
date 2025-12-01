# Spark Runner

A framework for running distributed Spark jobs on SLURM-managed clusters using containerized environments. This repository provides tools and examples for submitting and managing large-scale data processing tasks with Apache Spark.

## Overview

Spark Runner simplifies the process of running Spark applications on HPC clusters by:

- **Automated Cluster Setup**: Automatically configures Spark master and worker nodes across allocated compute nodes
- **Container Integration**: Uses containerized environments (chukonu) for consistent execution environments
- **Resource Management**: Integrates with SLURM for job scheduling and resource allocation
- **Example Workflows**: Provides ready-to-use examples for common data processing tasks

## Project Structure

```
spark-runner/
├── submit.sh              # Main SLURM submission script for Spark jobs
├── tools/                 # Python scripts for Spark tasks
│   ├── dedup_wcc.py      # Deduplication and weakly connected components
│   └── fasttext_filter.py # FastText-based language filtering
└── examples/             # Example configurations and run scripts
    ├── dedup_wcc/        # Deduplication example
    └── filter/           # FastText filtering example
```

## Development Guidelines

### Creating a New Example

When adding a new Spark task to this repository, follow this structure:

#### 1. Directory Structure

Create a new directory under `examples/` with the following files:

```
examples/your_task/
├── config.sh          # Configuration file with environment variables
├── run_<task_name>.sh # Task execution script
└── README.md          # Documentation for your example
```

#### 2. Configuration File (`config.sh`)

The configuration file should export environment variables that will be used by your task script. Common variables include:

```bash
#!/bin/bash

# Dataset parameters
export INPUT_PATH="/path/to/input/data"
export OUTPUT_PATH="/path/to/output/data"
export FILE_TYPE="parquet"  # or "json"
export TEXT_KEY="text"      # key name for text field

# Task-specific parameters
export PARAM1="value1"
export PARAM2="value2"
```

**Best Practices:**
- Use absolute paths for input/output directories
- Prefer Lustre filesystem paths for better performance
- Document all parameters in your README.md

#### 3. Task Script (`run_<task_name>.sh`)

The task script should:

1. **Validate required environment variables**:
   ```bash
   if [ -z "${MASTER_URL}" ]; then
       echo "Error: MASTER_URL is required but not set"
       exit 1
   fi
   ```

2. **Set default values** for optional parameters:
   ```bash
   EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
   EXECUTOR_MEMORY="${EXECUTOR_MEMORY:-32G}"
   ```

3. **Build the spark-submit command** with proper configuration:
   ```bash
   spark-submit \
       --master ${MASTER_URL} \
       --executor-cores ${EXECUTOR_CORES} \
       --executor-memory ${EXECUTOR_MEMORY} \
       --conf spark.default.parallelism=${DEFAULT_PARALLELISM} \
       --conf spark.sql.shuffle.partitions=${SQL_SHUFFLE_PARTITIONS} \
       tools/your_script.py \
       --input_path ${INPUT_PATH} \
       --output_path ${OUTPUT_PATH}
   ```

4. **Handle logging**:
   ```bash
   LOG_PATH=${LOG_PATH:-/path/to/logs}
   TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
   LOG_FILE="${LOG_PATH}/spark-runtime-${TIMESTAMP}.log"
   ```

#### 4. Python Task Script (`tools/your_script.py`)

Your Python script should:

1. **Use argparse** for command-line arguments:
   ```python
   parser = argparse.ArgumentParser()
   parser.add_argument("--input_path", type=str, required=True)
   parser.add_argument("--output_path", type=str, required=True)
   ```

2. **Configure SparkSession** with appropriate settings:
   ```python
   conf = SparkConf()
   conf.set("spark.app.name", "YourTaskName")
   spark = SparkSession.builder.config(conf=conf).getOrCreate()
   ```

3. **Handle errors gracefully** and provide informative output:
   ```python
   count = df.count()
   print(f"Total number of documents: {count}")
   ```

4. **Clean up resources**:
   ```python
   spark.stop()
   ```

#### 5. README.md Template

Your example README should include:

```markdown
# Task Name Demo

Brief description of what this example demonstrates.

## Prerequisites

1. List any required dependencies
2. File paths and permissions
3. External resources (models, data, etc.)

## How to Run

Submit the job using:
```bash
sbatch --nodes=<number-of-nodes> submit.sh <config-file> <task-script>
```

**Parameters:**
- `<number-of-nodes>`: Number of compute nodes
- `<config-file>`: Your custom configuration file
- `<task-script>`: The task execution script

**Example:**
```bash
cp examples/your_task/config.sh examples/your_task/my_config.sh
# Modify my_config.sh to meet your requirements
sbatch --nodes=4 submit.sh examples/your_task/my_config.sh examples/your_task/run_task.sh
```
```

### Prerequisites Checklist

Before submitting a job, ensure:

- [ ] **Write Permissions**: Verify paths in `submit.sh` have proper write permissions:
  - SLURM log paths (`--output` and `--error`)
  - Spark temporary directories (`tmp_dir` and `cache_dir`)
  - Output paths specified in your config

- [ ] **Filesystem**: Prefer Lustre filesystem for temporary files and logs for better performance

- [ ] **Dependencies**: Ensure all required files are accessible:
  - Python environments
  - Model files (if using `--files` option)
  - Input data files

- [ ] **Resource Requirements**: Consult with repository owner or system administrator for appropriate resource allocation

### Submitting Jobs

The standard workflow is:

```bash
# 1. Copy and customize configuration
cp examples/your_task/config.sh examples/your_task/my_config.sh
# Edit my_config.sh with your paths and parameters

# 2. Submit the job
sbatch --nodes=<N> submit.sh examples/your_task/my_config.sh examples/your_task/run_task.sh
```

**Resource Customization:**

You can customize CPU and memory allocation through sbatch parameters, but it's recommended to consult with the repository owner or system administrator to evaluate appropriate resource requirements for your specific workload.

### Code Style Guidelines

- **Bash Scripts**: Use `set -x` for debugging, but disable it before spark-submit to avoid verbose output
- **Python Scripts**: Follow PEP 8 style guidelines, use type hints where appropriate
- **Error Handling**: Always validate required environment variables and provide clear error messages
- **Logging**: Use descriptive log messages and include timestamps
- **Documentation**: Document all parameters and their default values

## Advanced Features

### Accessing Spark Web UI from VDI

To access the Spark Web UI while a job is running:

```bash
# 1. Start local proxy through jump server
ssh -D 8888 -N -J reallm.xyz@10.112.5.23 reallm.xyz@10.127.128.11

# 2. Open a new terminal and launch Edge with SOCKS5 proxy
& "C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe" --proxy-server="socks5://127.0.0.1:8888" --user-data-dir="C:\temp\spark-edge"
```

**Note**: The Spark Web UI is only accessible while the Spark application is running. If the application has finished, you can only check the corresponding logs.

## Examples

- **[Deduplication and WCC](examples/dedup_wcc/)**: Demonstrates document deduplication using MinHash LSH and weakly connected components computation
- **[FastText Filter](examples/filter/)**: Shows how to filter data using FastText language detection model

