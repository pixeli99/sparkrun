# Dedup Score Filter

Export final `minhash_simple` dedup outputs after filtering out score-0 cases.

The job reads only final result directories:

```text
<DEDUP_BASE>/<dataset>/year_YYYY/*.parquet
```

It ignores `year_YYYY_remove_ids` directories and writes rows where
`stage3_score` casts to `1` or `2` into:

```text
/work/projects/polyullm/infra/sync_to_b3/0518/<dataset>/year_YYYY/*.parquet
```

The default mode is `filter`, so it writes filtered parquet and does not run the
token TSV summary. Set `JOB_MODE=summary` or `JOB_MODE=both` if the summary is
needed.

Run with the repo-level Spark submit wrapper:

```bash
sbatch --nodes=2 \
  --export=ALL,EXTRA_CONTAINER_MOUNTS=/work/projects:/work/projects \
  submit.sh \
  examples/dedup_token_summary/config.sh \
  examples/dedup_token_summary/run_dedup_token_summary.sh
```

If the target host path is already visible inside the container, the
`EXTRA_CONTAINER_MOUNTS` override is not needed:

```bash
sbatch --nodes=2 submit.sh \
  examples/dedup_token_summary/config.sh \
  examples/dedup_token_summary/run_dedup_token_summary.sh
```

Useful overrides:

```bash
sbatch --nodes=4 \
  --export=ALL,DEDUP_BASE=/lustre/projects/polyullm/lipengxiang_tmp/minhash_simple/dedup,FILTERED_OUTPUT_PATH=/work/projects/polyullm/infra/sync_to_b3/0518,KEEP_SCORES=1,2,EXTRA_CONTAINER_MOUNTS=/work/projects:/work/projects \
  submit.sh examples/dedup_token_summary/config.sh examples/dedup_token_summary/run_dedup_token_summary.sh
```
