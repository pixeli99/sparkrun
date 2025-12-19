from abc import ABC, abstractmethod
import hashlib
import random
from typing import Optional

from .types import StandardDocument, validate_standard_document


class BaseAdapter(ABC):
    """
    Base class for dataset-specific adapters.

    Each adapter converts a raw record into the standard structure:
    StandardDocument(text=str, meta=Meta(...)).
    """

    @abstractmethod
    def adapt(self, raw_record) -> StandardDocument:
        """
        Convert a raw record to StandardDocument.
        Implementations must fail fast on malformed input.
        """

    def build_doc_id(self, source: str, text: str, order_num: Optional[int] = None) -> str:
        if order_num is None:
            order_num = random.getrandbits(63)
        text = text or ""
        head = text[:10]
        tail = text[-10:] if len(text) > 10 else text
        payload = f"{source}{order_num}{head}{tail}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def adapt_and_validate(self, raw_record) -> StandardDocument:
        """
        Convenience wrapper to enforce validation.
        """
        doc = self.adapt(raw_record)
        validate_standard_document(doc)
        return doc
