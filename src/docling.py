"""Docling wrapper used for document conversions."""
import threading
from docling.document_converter import DocumentConverter

class DoclingInterface:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            with cls._lock:
                if not cls._instance:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialize(*args, **kwargs)
        return cls._instance

    def _initialize(self, *args, **kwargs):
        self.converter = DocumentConverter()

    def convert_document(self, filename: str):
        return self.converter.convert(filename)
