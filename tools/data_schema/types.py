from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class Meta:
    """
    Metadata for a standardized document.
    """

    source: str
    doc_id: str
    chunk_id: str = "0"
    others: Dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class StandardDocument:
    """
    Standard representation: text plus metadata.
    """

    text: str
    meta: Meta


def validate_standard_document(doc: StandardDocument) -> None:
    """
    Fail fast when required fields are missing or empty.
    """
    if doc.text is None or doc.text == "":
        raise ValueError("text must be non-empty")
    if doc.meta is None:
        raise ValueError("meta must be provided")
    if doc.meta.source is None or doc.meta.source == "":
        raise ValueError("meta.source must be non-empty")
    if doc.meta.doc_id is None or doc.meta.doc_id == "":
        raise ValueError("meta.doc_id must be non-empty")
    if doc.meta.chunk_id is None or doc.meta.chunk_id == "":
        raise ValueError("meta.chunk_id must be a non-empty string")
