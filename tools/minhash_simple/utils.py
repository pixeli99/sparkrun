"""Utilities for the two-stage simple MinHash dedup pipeline.

简化版（不依赖 chukonu）：normalize_text + jieba n-gram + numpy MinHash 签名，
和 tools/minhash_dedup.py 是平级替代关系，参数语义、阶段切分都不同。
"""

import hashlib
import json
import re
import unicodedata
import zlib
from datetime import datetime, timedelta
from typing import List, Tuple

import numpy as np
import yaml
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from simhash import Simhash


# ---------------------------------------------------------------------------
# id / hash helpers
# ---------------------------------------------------------------------------
def get_md5(data_dict, key=None):
    """text 列加 salt 算 md5 当 id。data_dict 通常就是 text 字符串。"""
    data_md5 = hashlib.md5(json.dumps(data_dict, sort_keys=True).encode("utf-8")).hexdigest()
    if key:
        data_md5 = key + data_md5
    return data_md5


process_md5_udf = udf(get_md5, StringType())


def get_hash_bucket(url, bucket_num=100000):
    url = " ".join(url)
    text_hash = Simhash(url, f=64).value
    return str(text_hash % bucket_num)


process_hash_bucket_udf = udf(get_hash_bucket, StringType())


def sha1_hash32(text):
    return zlib.crc32(text.encode("utf-8"))


# ---------------------------------------------------------------------------
# text normalization
# ---------------------------------------------------------------------------
def remove_punctuation(text):
    if text is None:
        return ""
    out_chars = []
    for char in text:
        if not unicodedata.category(char).startswith("P"):
            out_chars.append(char)
        else:
            out_chars.append(" ")
    return "".join(out_chars)


@udf(returnType=StringType())
def normalize_text(text):
    text = remove_punctuation(text)
    text = text.lower()
    text = unicodedata.normalize("NFD", text)
    text = re.sub(r"\s+", " ", text)
    return text


# ---------------------------------------------------------------------------
# n-grams + minhash
# ---------------------------------------------------------------------------
def n_grams(text, n, max_tokens: int = 10000):
    import jieba

    n = int(n)
    words = [word for word in jieba.cut(text) if len(word.strip()) != 0]
    length = len(words)
    if length < n:
        return [" ".join(words)]

    tokens = set()
    for i in range(length - n + 1):
        tokens.add(" ".join(words[i : i + n]))
        if len(tokens) > max_tokens:
            break
    return tokens


def build_generate_hash_values_function(
    num_buckets,
    num_hashes_per_bucket,
    hash_dtype=np.uint32,
    max_hash_value=4_294_967_295,
    hash_mod_prime=4_294_967_291,
    seed=9899,
):
    """Returns a closure that maps (content, idx) -> [(band_idx, band_hashes, idx)].

    permutations 在 driver 端按固定 seed inline 生成，pickle 进闭包随 task 一起下发；
    所有 executor 拿到完全一样的矩阵。比预生成 .npy 然后 --files 分发省一步部署。
    """
    random_state = np.random.RandomState(seed)
    num_perm = num_buckets * num_hashes_per_bucket
    hash_ranges = [
        (i * num_hashes_per_bucket, (i + 1) * num_hashes_per_bucket)
        for i in range(num_buckets)
    ]
    permutations = np.array(
        [
            (
                random_state.randint(1, hash_mod_prime, dtype=hash_dtype),
                random_state.randint(0, hash_mod_prime, dtype=hash_dtype),
            )
            for _ in range(num_perm)
        ],
        dtype=hash_dtype,
    ).T

    def _generate_hash_values(
        content: str,
        idx: int,
        ngram_size=5,
    ) -> List[Tuple[int, bytes, int]]:
        tokens = n_grams(content, ngram_size, max_tokens=10000)
        a, b = permutations
        hv = np.array([sha1_hash32(token) for token in tokens], dtype=hash_dtype)
        phv = (np.einsum("b,t->bt", hv, a) + b) % hash_mod_prime
        phv = np.bitwise_and(phv % hash_mod_prime, max_hash_value)
        hash_values = phv.min(axis=0)
        Hs = [
            bytes(hash_values[start:end].byteswap().data) for start, end in hash_ranges
        ]
        return [(band_idx, band_hashes, idx) for band_idx, band_hashes in enumerate(Hs)]

    return _generate_hash_values


# ---------------------------------------------------------------------------
# misc
# ---------------------------------------------------------------------------
def generate_edges(nodes: List[int]) -> List[Tuple[int]]:
    if len(nodes) <= 1:
        return []
    min_node = min(nodes)
    return [(n,) for n in nodes if n != min_node]


def load_yaml_file(fn):
    if isinstance(fn, str):
        with open(fn, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)
    return fn


def get_input_path(patten):
    """递归展开 input path 配置。支持：

    - str: 原样返回
    - list: 逐项展开拼接
    - dict: 必须含 'path'；若同时有 start_date/end_date，则按日期 format 展开
    """
    if isinstance(patten, str):
        return [patten]
    if isinstance(patten, list):
        res = []
        for v in patten:
            res.extend(get_input_path(v))
        return res
    if isinstance(patten, dict):
        assert "path" in patten
        if "start_date" not in patten and "end_date" not in patten:
            return patten["path"] if isinstance(patten["path"], list) else [patten["path"]]
        start_date = datetime.strptime(patten["start_date"], "%Y%m%d")
        end_date = datetime.strptime(patten["end_date"], "%Y%m%d")
        exclude = patten.get("exclude", [])
        res = []
        while start_date <= end_date:
            date_str = start_date.strftime("%Y%m%d")
            if date_str not in exclude:
                res.append(patten["path"].format(date=date_str))
            start_date += timedelta(days=1)
        return res
