用途：调用标准化脚本（工具内置 adapter 选择）。

输出目录建议：
`{base}/{dataset}/{stage}/{version}`
例子：`/work/projects/polyullm/data/fineweb/standard/v20251217`
下游只读目录，不依赖 part 文件名。

运行示例（FineWeb）：
```bash
MASTER_URL=spark://... \
INPUT_PATH=/path/to/fineweb/input \
OUTPUT_PATH=/path/to/fineweb/standard \
ADAPTER=fineweb \
NUM_PARTITIONS=0 \
bash examples/fineweb_standardize/run_fineweb_standardize.sh
```
