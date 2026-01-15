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
                "metadata": str(json.dumps(raw_record["metadata"])),
                "previous_word_count": int(raw_record["previous_word_count"]),
                "url": str(raw_record["url"]),
                "warcinfo": str(raw_record["warcinfo"]),
                "language_id_whole_page_fasttext": str(json.dumps(raw_record["language_id_whole_page_fasttext"])),
                "fasttext_openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train_prob": str(raw_record["fasttext_openhermes_reddit_eli5_vs_rw_v2_bigram_200k_train_prob"]),
                "bff_contained_ngram_count_before_dedupe": str(raw_record["bff_contained_ngram_count_before_dedupe"]),
            },
        )
        return StandardDocument(text=text, meta=meta)
