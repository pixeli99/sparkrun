from .. import BaseAdapter, Meta, StandardDocument


class Dolma3MixCCAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里common crawl。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-cc"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixCodeAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里code。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-stack-edu"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixCodeAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里code。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-stack-edu"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixOCRAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里olmocr。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-olmocr"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixOCRAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里olmocr。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-olmocr"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixMathAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里fine math。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-math"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)


class Dolma3MixWikiAdapter(BaseAdapter):
    """
    适配 dolma3_mix-6T-1025 数据集里wiki。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dolma3_mix-6T-1025-wiki"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "metadata": str(raw_record["metadata"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
