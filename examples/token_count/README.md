# token_count Jobs README

This doc explains how to submit the token_count Spark job via Slurm, using only `config.sh` and `run_token_count.sh`.

## Prerequisites
- Verify paths in `submit.sh` have write permission (sbatch `--output/--error`, Spark temp dirs).
- Ensure the Python environment in `run_token_count.sh` (`ENV_ROOT`/`ENV_PYTHON`) exists on all nodes.
- Set dataset/model paths in `config.sh` (`INPUT_PATH`, `OUTPUT_PATH`, `MODEL_PATH`, etc.).

## How to Run

Submit the job with sbatch:
```bash
sbatch --nodes=<number-of-nodes> submit.sh examples/token_count/config.sh examples/token_count/run_token_count.sh
```

**Parameters:**
- `<number-of-nodes>`: nodes to allocate.
- `config.sh`: provides defaults and env overrides (key ones below).
- `run_token_count.sh`: Spark job launcher; reads values from env/config (e.g., `INPUT_PATH`, `OUTPUT_PATH`, `MODEL_PATH`, `TOOLS`, `SAMPLE_N`).

Key `config.sh` variables:
- `TOOLS`: which token_countities to run, e.g. `sample count_tokens` or just use 'count_tokens'(space-separated).
- `SAMPLE_N`: sample size when `sample` tool is enabled.

**Example:**
```bash
cp examples/token_count/config.sh examples/token_count/my_config.sh
# Edit my_config.sh to set INPUT_PATH/OUTPUT_PATH/MODEL_PATH/TOOLS/SAMPLE_N
sbatch --nodes=4 submit.sh examples/token_count/my_config.sh examples/token_count/run_token_count.sh
```

## Notes
- Customize defaults in `config.sh` (or a copy) to match your data and model paths.
- Logs follow `LOG_PATH` defined in the config/env; check `.log`/`.err` for issues.
- Output file (JSON) structure from `token_count.py`:
  - `meta`: the parsed CLI args/environment (input, output, model, tools, sample size, etc.).
  - `total_docs`: count of documents loaded.
  - `details`:
    - `total_tokens` / `total_tokens_human` (if `count_tokens` enabled): raw count and human-readable string.
    - `samples` (if `sample` tool enabled): list of objects with `text`.