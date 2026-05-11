# minhash_simple

`tools/minhash_dedup.py` 的纯 PySpark + numpy 替代版本，**不依赖 chukonu**。
两阶段拆分：先用 `save_hash_key.py` 把 MinHash 签名落盘，再用 `inc_data.py` 拿
新签名跟历史签名（可选）+ 自身做去重。

## 何时用这个 vs `minhash_dedup`

| | minhash_simple | minhash_dedup |
|---|---|---|
| 依赖 | pyspark + numpy + jieba + simhash | + chukonu C++ extension |
| 速度 | 全程 Python UDF，每行过 Python worker | C++ native，10-50x 快 |
| Keeper | min(id)，无质量感知 | priority (score, length, uid) |
| 聚类 | 逐 band 局部最小，不是 WCC | WCC 连通分量 |
| duplicate_count | 不输出 | 每条 kept row 都带 |
| 断点续跑 | 没有（两阶段就是粗粒度 checkpoint） | 每个 stage 都 `_SUCCESS` |
| 增量场景 | 原生支持（`source_hash_paths` + `inc_paths`） | 没直接对应 |

**选 simple 的场景**：单轮去重，输入已按 lang/year 切到 200-400GB 单片，没有质量分，要走「历史 + 增量」流水线，能接受激进去重（删多）。

**选 dedup 的场景**：1TB+ 单 job，要质量优先，要 duplicate_count 给下游做加权采样，job 半路可能挂。

## 用法

```bash
# 1. 编辑 examples/minhash_simple/example_zh.yaml（或拷一份改）
# 2. 启动 spark 集群拿到 MASTER_URL（参考 submit.sh）
export MASTER_URL=spark://...
source examples/minhash_simple/config.sh

# Stage 1: 算签名落盘（每个 source 写一份）
bash examples/minhash_simple/run_save_hash_key.sh

# Stage 2: 拿签名做去重，写 output_path
bash examples/minhash_simple/run_inc_data.sh
```

`CONFIG_PATH` 默认 `examples/minhash_simple/example_zh.yaml`，可被环境变量覆盖。

## YAML 字段

**Stage 1 (`save_hash_key.py`) 读 `hash_source`**：

- `key`: md5 加盐，区分不同 source（防 id 跨 source 撞车）
- `path`: 输入 parquet 路径。支持 str / list / `{path, start_date, end_date, exclude}` 日期模板
- `hash_oss_path`: 签名输出目录

**Stage 2 (`inc_data.py`) 读**：

- `source_hash_paths` (可选): 历史已保留集合的签名目录列表
- `inc_paths`: 本次新增。每项的 `hash_oss_path` 必须是 Stage 1 同一 source 写出的位置，`key` + `path` 必须跟 Stage 1 一致（否则 id 对不上）
- `distinct_id`: 同一 text 出现在多个 part 里时，先合并 `(band_idx, band_hashes, id)` 三元组
- `output_path`: 去重后 text 输出目录
- `multi_inc` (可选): 把上面整套包成列表，一个 YAML 跑多组

## 1.4TB 实操提醒

1. **必须切片**。`inc_data.py` 那个 `row_number().partitionBy([band_idx, band_hashes])` 在大 boilerplate 桶上是单 reducer 死等，1.4TB 一锅炖会卡在 last 0.1% 几小时不动。先按 lang / year / source 切到 200-400GB 单片再跑。

2. **`num_buckets=32, num_hashes_per_bucket=10`** 是 320 个 perm，比 `minhash_dedup.py` 默认 128 perm 重一倍多，召回更激进、shuffle 量更大。觉得删太多就在 `save_hash_key.py` 里调小 `num_buckets`。

3. **纯 Python UDF 瓶颈**：`n_grams` 走 jieba、`process_md5_udf` 是 Python md5。1.4TB 这量级，单 stage 1 写完估计 4-10 小时（视集群），主要时间花在 JVM↔Python 序列化和 jieba 分词。这是用 `minhash_simple` 而不是 chukonu 版的固定成本。

4. **依赖**：executor 上要装 `jieba`、`simhash`、`numpy`、`pyyaml`。container image 没带的话先 `pip install`。
