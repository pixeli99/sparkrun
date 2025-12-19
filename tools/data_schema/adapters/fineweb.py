from .. import BaseAdapter, Meta, StandardDocument


class FineWebAdapter(BaseAdapter):
    """
    适配 FineWeb 数据集。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        doc_id = self.build_doc_id("fineweb", text)
        meta = Meta(
            source="fineweb",
            doc_id=doc_id,
            chunk_id="0",
            others={
                "dump": str(raw_record["dump"]),
                "url": str(raw_record["url"]),
                "date": str(raw_record["date"]),
                "file_path": str(raw_record["file_path"]),
                "language": str(raw_record["language"]),
                "language_score": str(raw_record["language_score"]),
                "token_count": str(raw_record["token_count"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
