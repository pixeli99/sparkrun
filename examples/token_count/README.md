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
- `TOOLS`: which tools to run, e.g. `sample count_tokens` or just `count_tokens` (space-separated).
- `SAMPLE_N`: sample size when `sample` tool is enabled.
- `TEXT_KEY`: text column name (default `text`).
- `LINE_ID_KEY`: 设为 `line_id` 时,脚本会解析该列的 JSON `{"line_id": [...]}`,只对被标记的正文行统计 token。用于 stage2 (noborder) 输出的 parquet。留空则统计整列 text。

**Example:**
```bash
cp examples/token_count/config.sh examples/token_count/my_config.sh
# Edit my_config.sh to set INPUT_PATH/OUTPUT_PATH/MODEL_PATH/TOOLS/SAMPLE_N
sbatch --nodes=4 submit.sh examples/token_count/my_config.sh examples/token_count/run_token_count.sh
```

**Example (对 stage2 noborder parquet 只统计正文 token):**
```bash
export INPUT_PATH="/work/projects/polyullm/lipengxiang_tmp/fineweb_noborder/CC-MAIN-*/*.parquet"
export OUTPUT_PATH="/path/to/fineweb_noborder_tokens.json"
export LINE_ID_KEY="line_id"
sbatch --nodes=4 --export=ALL submit.sh examples/token_count/config.sh examples/token_count/run_token_count.sh
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