from .. import BaseAdapter, Meta, StandardDocument


class DCLMAdapter(BaseAdapter):
    """
    适配 DCLM 数据集。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "dclm-baseline-1.0"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "previous_word_count": str(raw_record["previous_word_count"]),
                "url": str(raw_record["url"]),
                "warcinfo": str(raw_record["warcinfo"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
