from .. import BaseAdapter, Meta, StandardDocument


class FineWebEduAdapter(BaseAdapter):
    """
    适配 FineWeb 数据集。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        source = "fineweb_edu_sample_100bt"
        doc_id = self.build_doc_id(source, text)
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "id": str(raw_record["id"]),
                "file_path": str(raw_record["file_path"]),
                "language": str(raw_record["language"]),
                "language_score": str(raw_record["language_score"]),
                "token_count": str(raw_record["token_count"]),
                "score": str(raw_record["score"]),
                "int_score": str(raw_record["int_score"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
