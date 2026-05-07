from .. import BaseAdapter, Meta, StandardDocument


class FineWeb2Adapter(BaseAdapter):
    """
    适配 FineWeb2 数据集。
    """

    def adapt(self, raw_record) -> StandardDocument:
        text = raw_record["text"]
        doc_id = raw_record["id"]
        source = "fineweb2"
        meta = Meta(
            source=source,
            doc_id=doc_id,
            chunk_id="0",
            others={
                "id": str(doc_id),
                "dump": str(raw_record["dump"]),
                "url": str(raw_record["url"]),
                "date": str(raw_record["date"]),
                "file_path": str(raw_record["file_path"]),
                "language": str(raw_record["language"]),
                "language_score": str(raw_record["language_score"]),
                "language_script": str(raw_record["language_script"]),
                "minhash_cluster_size": str(raw_record["minhash_cluster_size"]),
                "top_langs": str(raw_record["top_langs"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
