export INPUT_PATH="/work/projects/polyullm/lipengxiang_tmp/fineweb_noborder/CC-MAIN-*/*.parquet"
export OUTPUT_PATH="/lustre/projects/polyullm/lipengxiang_tmp/sparkrun/fineweb_scored_tokens.json"
export LINE_ID_KEY=""
sbatch --reservation=pretrain --nodes=38 --export=ALL submit.sh examples/token_count/config.sh examples/token_count/run_token_count.sh