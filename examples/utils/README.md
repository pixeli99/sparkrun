# utils Jobs README

This doc explains how to submit the utility Spark job via Slurm, using only `config.sh` and `run_util.sh`.

## Prerequisites
- Verify paths in `submit.sh` have write permission (sbatch `--output/--error`, Spark temp dirs).
- Ensure the Python environment in `run_util.sh` (`ENV_ROOT`/`ENV_PYTHON`) exists on all nodes.
- Set dataset/model paths in `config.sh` (`INPUT_PATH`, `OUTPUT_PATH`, `MODEL_PATH`, etc.).

## How to Run

Submit the job with sbatch:
```bash
sbatch --nodes=<number-of-nodes> submit.sh examples/utils/config.sh examples/utils/run_util.sh
```

**Parameters:**
- `<number-of-nodes>`: nodes to allocate.
- `config.sh`: provides defaults and env overrides (key ones below).
- `run_util.sh`: Spark job launcher; reads values from env/config (e.g., `INPUT_PATH`, `OUTPUT_PATH`, `MODEL_PATH`, `TOOLS`, `SAMPLE_N`).

Key `config.sh` variables:
- `TOOLS`: which utilities to run, e.g. `sample count_tokens` (space-separated).
- `SAMPLE_N`: sample size when `sample` tool is enabled.

**Example:**
```bash
cp examples/utils/config.sh examples/utils/my_config.sh
# Edit my_config.sh to set INPUT_PATH/OUTPUT_PATH/MODEL_PATH/TOOLS/SAMPLE_N
sbatch --nodes=4 submit.sh examples/utils/my_config.sh examples/utils/run_util.sh
```

## Notes
- Customize defaults in `config.sh` (or a copy) to match your data and model paths.
- Logs follow `LOG_PATH` defined in the config/env; check `.log`/`.err` for issues.
- Output file (JSON) structure from `util.py`:
  - `meta`: the parsed CLI args/environment (input, output, model, tools, sample size, etc.).
  - `total_docs`: count of documents loaded.
  - `details`:
    - `samples` (if `sample` tool enabled): list of objects with `text`.
    - `total_tokens` / `total_tokens_human` (if `count_tokens` enabled): raw count and human-readable string.