# MinHash Dedup

全局 MinHash 近似去重，priority-aware 选 keeper。读 FineWeb-style scored
parquet，按 jaccard 相似度（默认 threshold=0.85）找出 near-duplicate cluster，
每个 cluster 内保留 `stage3_score` 最高的那条，输出加一列 `duplicate_count`。

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

5 个阶段，每阶段中间结果都写到 lustre，方便 debug 和重跑：

```
input parquet (递归扫嵌套子目录)
        │
        ▼
  ┌────────────────────────────────┐
  │ 1. read + 加 priority、uid     │
  ├────────────────────────────────┤
  │ 2. exact dedup (priority-aware)│  output_path/exact/
  │    相同 text 压成 1 条，记原数  │
  ├────────────────────────────────┤
  │ 3. chukonu minhash             │
  │    LSH 找候选相似边 (src,dst)   │
  ├────────────────────────────────┤
  │ 4. WCC 聚 cluster              │  output_path/wcc/
  │    (vid, component) 表          │
  ├────────────────────────────────┤
  │ 5. cluster 内选 keeper         │  output_path/result/   ← 最终输出
  │    + duplicate_count            │
  └────────────────────────────────┘
```

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
| `INPUT_PATH` | fineweb_012 lustre 路径 | 输入根目录，递归扫所有 parquet |
| `OUTPUT_PATH` | fineweb_012_minhash | 输出根目录（下面建 exact/, wcc/, result/） |
| `TEXT_KEY` | `text` | 文本列名 |
| `SCORE_KEY` | `stage3_score` | 质量分列名 |
| `THRESHOLD` | `0.85` | jaccard 相似度阈值 |
| `NUM_PERM` | `128` | MinHash 排列数 |
| `B` | `8` | LSH bands；`B=""` 时按 `optimal_param` 自动求解 |
| `R` | `16` | LSH rows per band；`R=""` 时同上 |
| `NGRAM_SIZE` | `5` | n-gram 长度（chukonu 内部用） |
| `MIN_LENGTH` | `2` | 最少 n-gram 数；过短文档被跳过 |
| `SPARK_LOCAL_DIR` | `<lustre>/spark_local/${SLURM_JOB_ID}` | shuffle spill 目录，per-job 隔离 |

`B * R = NUM_PERM` 是约束。如果不知怎么调 b/r，把 **两个都设成空串**
（`export B=""; export R=""`），脚本不会把 `--b/--r` 传给 python，
`optimal_param` 会按 (threshold, num_perm) 自动求最优组合。

## Output Schema

`output_path/result/` 下的 parquet：所有原列 + `duplicate_count`。

| 列名 | 类型 | 含义 |
|------|------|------|
| `duplicate_count` | bigint | 该 cluster 原始 doc 数（exact + minhash near-dup 之和） |

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
