# MinHash Dedup

全局 MinHash 近似去重，priority-aware 选 keeper。读 FineWeb-style scored
parquet，按 jaccard 相似度（默认 threshold=0.85）找出 near-duplicate cluster，
每个 cluster 内保留 `stage3_score` 最高的那条，输出加一列 `duplicate_count`。
如果输入已经带有 `duplicate_count`，脚本会把它当作每行代表的原始 doc 数，
方便先局部 dedup、再全局 dedup。

跟 [`dedup_wcc`](../dedup_wcc) 的差别：

| | dedup_wcc | minhash_dedup（这个） |
|---|---|---|
| 选 keeper 方式 | `monotonically_increasing_id` 最小（≈随机） | `stage3_score` priority 最高 |
| 输出 schema | 只有 `text + uid` | 保留所有原列 + `duplicate_count` |
| 算法 | MinHash + WCC | 完全一样 |
| chukonu | ✅ | ✅ |

priority 公式与 datatrove `QualityPriority` 等价：

```
score_rank = {"2": 3, "1": 2, "0": 1, others: 0}
priority   = clamp(score_rank * 20000 + min(5000, len(text) // 20), 1, 65535)
```

cluster 内 priority tie-break 时按文本长度降序，再按 uid 升序。

## Pipeline

每个有意义的中间产物都独立落盘到 lustre。下一次 `sbatch` 同样命令再跑：看到对应
目录下有 `_SUCCESS` 就自动跳过该阶段。挂哪续哪，没有 reuse flag。
新写 checkpoint 时会先写到 `output/_staging/<stage>-<app>-<ts>/`；只有 staging
目录出现 `_SUCCESS` 后才会切换到正式目录。失败留下的同 stage staging 目录会在下次
重跑该 stage 前自动清理，正式目录没有 `_SUCCESS` 会被视为半成品并重写。
如果本轮运行重建了某个上游 checkpoint，下游即使已有 `_SUCCESS` 也会被视为 stale：
脚本会先在 staging 写出新下游，成功后再替换旧目录，避免复用不匹配的旧结果。
替换一个已有完整目录时，旧目录会先临时移动到 `output/_backup/`；新目录确认有
`_SUCCESS` 后才删除 backup。

```
input parquet (递归扫嵌套子目录)
        │
        ▼
  ┌────────────────────────────────────────┐
  │ 1+2. skip 模式：lazy uid，必要时落盘     │  output/exact/  ← 非 skip exact dedup
  │      非 skip：全局 exact dedup           │                 ← skip 时 uid checkpoint
  ├────────────────────────────────────────┤
  │ 3a. normalize text (Python UDF 固化)    │  output/normalized/
  │                                         │  (uid, normalized_text)
  ├────────────────────────────────────────┤
  │ 3b. chukonu minhash LSH                 │  output/edges/
  │     候选相似边                           │  (src, dst)
  ├────────────────────────────────────────┤
  │ 4.  WCC 聚 cluster                       │  output/wcc/
  │                                         │  (vid, component)
  ├────────────────────────────────────────┤
  │ 5a. 窄列 max_by(uid) 选 keeper          │  output/keepers/
  │     避免 wide-row partial agg OOM        │  (component, keeper_uid, dup_cnt)
  ├────────────────────────────────────────┤
  │ 5b. keeper_uid join 回 wide row         │  output/near/
  │                                         │  原列 + duplicate_count
  ├────────────────────────────────────────┤
  │ 5c. left_anti 选孤立 doc                 │  output/isolated/
  │                                         │  原列 + duplicate_count
  ├────────────────────────────────────────┤
  │ 5d. union near + isolated                │  output/result/   ← 最终输出
  └────────────────────────────────────────┘
```

**`SKIP_EXACT_DEDUP=true` 时会尽量复用已有 `normalized/`**：如果 `normalized/_SUCCESS`
已经存在，脚本不会重写它，Stage 3b 会直接读该目录。后续需要 join 回原始宽表时，
如果 `exact/_SUCCESS` 不存在，脚本会把 `exact/` 作为 uid checkpoint 落盘，避免
`near/isolated` 再反复从 raw 输入生成 uid。这个 legacy uid 仍依赖 Spark file listing
顺序和 FilePartition 划分稳定；代码强制 `spark.sql.files.ignoreCorruptFiles=false`，
不允许偶发跳文件。
如果 `exact/` 和 `normalized/` 都存在，脚本会检查两者行数一致；不一致时直接失败，
防止继续写出 uid 错位的后续结果。

非 skip 模式必须写 `exact/`：行数变了，结果跟 lazy raw 不再等价。skip 模式下的
`exact/` 不是再做一遍 exact dedup，只是 `raw + uid + __exact_cnt` 的 checkpoint。

## How to Run

```bash
sbatch --nodes=<N≥2> submit.sh examples/minhash_dedup/config.sh examples/minhash_dedup/run_minhash_dedup.sh
```

`submit.sh` 用 head node 跑 Spark master、其余 N-1 个节点跑 worker。
**`--nodes=1` 不可用**：worker_num 会变成 0 → `DEFAULT_PARALLELISM=0` →
后续 spark-submit 直接炸。Smoke test 至少 `--nodes=2`，全量推荐 32+ 节点。

`MASTER_URL` / `EXECUTOR_CORES` / `EXECUTOR_MEMORY` / `DEFAULT_PARALLELISM`
都由 `submit.sh` 根据 SLURM 资源自动算出。

## Resource Sizing for 17TB Tokens

按 17TB 原始文本 / ~17B docs 估算（粗略）：

| `--nodes=` | total cores | shuffle 估时 | 备注 |
|---|---|---|---|
| 16 | 1024 | 6 – 12 h | 偏紧 |
| 32 | 2048 | 3 – 6 h | 推荐 |
| 64 | 4096 | 2 – 4 h | 阶段 3-4 网络可能成瓶颈 |

阶段 3 (minhash) 是 CPU bound，chukonu 加速 ~10x；阶段 4 (WCC) 是 graph algo，
edge 数 ~ near-dup pair 数。如果 `dedup_ratio` 高（30%+），cluster 多、edge 多，
WCC 内存可能吃紧 —— 那时把 `--num_parallel` 调大或加节点。

`spark.local.dir` 指向 per-job 子目录：默认
`/lustre/projects/polyullm/lipengxiang_tmp/spark_local/${SLURM_JOB_ID}`，
shuffle spill 写到这里，每个 job 互不污染，跑完可以整个 rm 掉。
提交前确保父目录所在 lustre pool 有数 TB 空闲。可通过 `SPARK_LOCAL_DIR`
环境变量整体覆盖。

## Custom Config

```bash
cp examples/minhash_dedup/config.sh examples/minhash_dedup/my_config.sh
# 改 INPUT_PATH / OUTPUT_PATH / threshold 等
sbatch --nodes=32 submit.sh examples/minhash_dedup/my_config.sh examples/minhash_dedup/run_minhash_dedup.sh
```

| 变量 | 默认 | 含义 |
|------|------|------|
| `INPUT_PATH` | fineweb_012 lustre 路径 | 输入根目录，递归扫所有 parquet；也支持逗号分隔多个路径/glob |
| `OUTPUT_PATH` | fineweb_012_minhash | 输出根目录；下面建 exact/ normalized/ edges/ wcc/ keepers/ near/ isolated/ result/ |
| `TEXT_KEY` | `text` | 文本列名 |
| `SCORE_KEY` | `stage3_score` | 质量分列名 |
| `WEIGHT_KEY` | `duplicate_count` | 输入已 dedup 时每行代表的原始 doc 数；列不存在则按 1 |
| `THRESHOLD` | `0.85` | jaccard 相似度阈值 |
| `NUM_PERM` | `128` | MinHash 排列数 |
| `B` | `8` | LSH bands；`B=""` 时按 `optimal_param` 自动求解 |
| `R` | `16` | LSH rows per band；`R=""` 时同上 |
| `NGRAM_SIZE` | `5` | n-gram 长度（chukonu 内部用） |
| `MIN_LENGTH` | `2` | 最少 n-gram 数；过短文档被跳过 |
| `SQL_SHUFFLE_PARTITIONS` | `DEFAULT_PARALLELISM * 8` | Spark SQL shuffle 分区数；全量 17TB 建议显式调到数万级 |
| `WCC_PARALLELISM` | `DEFAULT_PARALLELISM` | 传给 chukonu WCC 的并行度 |
| `MINHASH_INPUT_PARTITIONS` | `SQL_SHUFFLE_PARTITIONS` | 读 `normalized/` 后进入 chukonu 前最多 coalesce 到该分区数 |
| `DRIVER_MEMORY` | `64G` | driver JVM heap；全量几十万 input tasks 时建议 `128G` |
| `DRIVER_CORES` | `4` | driver cores；全量任务建议 `8` |
| `SPARK_UI_RETAINED_TASKS` | `1000` | driver AppStatusListener/Spark UI 保留 task 数，防止 task metrics 打爆 driver |
| `SPARK_SQL_UI_RETAINED_EXECUTIONS` | `20` | Spark SQL UI 保留执行数 |
| `SPARK_EXECUTOR_HEARTBEAT_INTERVAL` | `60s` | executor heartbeat 间隔，降低 driver metrics 事件压力 |
| `SPARK_NETWORK_TIMEOUT` | `600s` | 网络超时，避免 listener 短时阻塞时误判 executor 丢失 |
| `SKIP_EXACT_DEDUP` | `false` | `true` 时跳过 stage 2 全局 exact dedup；若后续宽表阶段需要，会把 `exact/` 作为 uid checkpoint 落盘 |
| `SPARK_LOCAL_DIR` | `<lustre>/spark_local/${SLURM_JOB_ID}` | shuffle spill 目录，per-job 隔离 |

`B * R = NUM_PERM` 是约束。如果不知怎么调 b/r，把 **两个都设成空串**
（`export B=""; export R=""`），脚本不会把 `--b/--r` 传给 python，
`optimal_param` 会按 (threshold, num_perm) 自动求最优组合。

作业挂了直接 `sbatch` 同样命令再跑一次。每个落盘点（normalized / edges / wcc /
keepers / near / isolated / result，以及非 skip 模式下的 exact）开头会 check
`_SUCCESS`，存在就跳过该阶段。

想从某一步重新算就 `rm -r` 掉那一步**及后续**所有目录：

```bash
# 例：keepers 跑出来不对，想重算 keepers 及之后
rm -rf $OUTPUT_PATH/{keepers,near,isolated,result}
sbatch --nodes=32 submit.sh examples/minhash_dedup/my_config.sh examples/minhash_dedup/run_minhash_dedup.sh
```

新版本会在本轮运行内自动识别“上游刚刚重建导致下游 stale”的情况，并在 staging
成功后替换下游旧目录。不过跨多次手工改配置时，仍建议按上面的方式连后续目录一起删，
这样日志和磁盘状态最清楚。

skip 模式下 `exact/` 是 uid checkpoint；如果只想复用已有 `normalized/` 重跑 MinHash，
不要删 `normalized/`，只删 `edges/` 及后续目录即可。

`_staging/` 只保存失败或正在写的临时 checkpoint，`_backup/` 只保存替换 checkpoint
过程中的临时备份。同一个 `OUTPUT_PATH` 不要并发跑两份任务；确认没有任务在写时，
可以手工清理旧 staging/backup：

```bash
rm -rf $OUTPUT_PATH/{_staging,_backup}
```

## Output Schema

`output_path/result/` 下的 parquet：所有原列 + `duplicate_count`。

| 列名 | 类型 | 含义 |
|------|------|------|
| `duplicate_count` | bigint | 该 cluster 原始 doc 数（exact + minhash near-dup 之和） |

## Hierarchical Dedup

如果一次全局跑不动，可以先按 shard 局部跑，再把各 shard 的
`result/` 合起来做第二层全局 dedup：

```bash
# 第一层：每个 shard 单独跑，输出 /dedup_local/shard_x/result/
export INPUT_PATH="/lustre/.../fineweb_012/shard_x"
export OUTPUT_PATH="/lustre/.../fineweb_012_dedup_local/shard_x"

# 第二层：读所有局部 result，再全局跑一次
export INPUT_PATH="/lustre/.../fineweb_012_dedup_local/*/result"
export OUTPUT_PATH="/lustre/.../fineweb_012_dedup_global"
export WEIGHT_KEY="duplicate_count"
```

第二层会继承第一层的 `duplicate_count`，所以最终
`sum(duplicate_count)` 仍然表示原始文档数。注意这对 MinHash near-dup
不是严格等价于一次性全局去重；局部阶段只保留一个 representative 时，
跨 shard 的近重复可能会有少量漏召回。

如果 Layer 1 已经按 `CC-MAIN-*` 做完，推荐继续用小 fan-in merge tree：

```text
L1: CC-MAIN-YYYY-WW               (已完成；如果是脚本产物也可以是 CC-MAIN-YYYY-WW/result)
        │
        ▼
L2: year_YYYY/result              每年 merge 多个 CC-MAIN
        │
        ▼
L3: era_YYYY_YYYY/result          每 3-4 年 merge 一组
        │
        ▼
L4: global/result                 merge 所有 era
```

Layer 2 按年跑：

```bash
YEAR=2014 sbatch --nodes=16 submit.sh \
  examples/minhash_dedup/config_layer2_year.sh \
  examples/minhash_dedup/run_minhash_dedup.sh
```

如果你的 L1 根目录下每个 crawl 本身就是 parquet 数据目录，就保持默认：

```bash
L1_ROOT=/lustre/projects/polyullm/lipengxiang_tmp/fineweb_012
L1_RESULT_SUFFIX=
```

如果 L1 是本脚本跑出的 `CC-MAIN-*/result`，再设置：

```bash
L1_RESULT_SUFFIX=result
```

Layer 3 按 era 跑：

```bash
ERA_NAME=era_2013_2016 YEARS=2013,2014,2015,2016 sbatch --nodes=24 submit.sh \
  examples/minhash_dedup/config_layer3_era.sh \
  examples/minhash_dedup/run_minhash_dedup.sh
```

Layer 4 最终 global：

```bash
sbatch --nodes=32 submit.sh \
  examples/minhash_dedup/config_layer4_global.sh \
  examples/minhash_dedup/run_minhash_dedup.sh
```

## Verify

跑完后做 sanity 检查：

```python
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

OUT = "/lustre/.../fineweb_012_minhash"
inp = spark.read.option("recursiveFileLookup","true").parquet("/lustre/.../fineweb_012")
res = spark.read.parquet(f"{OUT}/result")

n_in   = inp.count()
n_kept = res.count()
n_sum  = res.agg(F.sum("duplicate_count")).collect()[0][0]
print(f"input={n_in:,}  kept={n_kept:,}  sum(dup_count)={n_sum:,}")
print(f"sum(dup_count) == input ?  {n_sum == n_in}")
print(f"dedup ratio: {1 - n_kept/n_in:.2%}")
```

`sum(duplicate_count) == input rows` 必须成立 —— 每个 cluster 加起来就是输入。
脚本最后也会做同样检查；不相等会直接退出失败，不会打印 `ALL DONE`。
