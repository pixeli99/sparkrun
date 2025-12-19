### 数据标准化入口

已提供：
- FineWebAdapter

怎么用（FineWeb）：
```python
from tools.data_schema.adapters import FineWebAdapter

adapter = FineWebAdapter()
doc = adapter.adapt_and_validate(raw_record)
# doc.text, doc.meta
```

怎么扩展新数据集：
1. 在 `tools/data_schema/adapters/` 新建文件，继承 `BaseAdapter`
2. `adapt` 里直接取字段构造 `Meta` 和 `StandardDocument`
3. 在 `adapters/__init__.py` 里导出新适配器
