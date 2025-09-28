"""Veeva Vault API client.

Simplified, resilient Veeva client used by the retrieval and download pipelines.
"""
from __future__ import annotations
import os
import time
import urllib.parse
from typing import List, TypeVar, Literal, Any
import requests

from src.decorators import retry
from src.exceptions.exceptions import ExpiredTokenException, NotReadyException
from src.logging import SingletonLogger
from src.models.document import Document
from src.models.document_metadata import DocumentMetadata

T = TypeVar("T")
logger = SingletonLogger().get_logger()


class Veeva:
    TEMP_FOLDER = "tmp"

    def __init__(self, url: str, username: str, password: str, session_id: str | None = None):
        self.url = url.rstrip("/") + "/"
        self.username = username
        self.password = password
        self.session_id = session_id or ""
        self.user_id: str | None = None
        logger.info("Veeva client initialized")

    @staticmethod
    def _create_payload(payload: dict) -> str:
        return "&".join([f"{k}={v}" for k, v in payload.items()])

    @retry((requests.exceptions.Timeout,), delay=10, times=2)
    def _authentication(self) -> tuple[str, str]:
        url = urllib.parse.urljoin(self.url, "auth")
        payload = {"username": self.username, "password": self.password}
        headers = {"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"}
        resp = requests.post(url, headers=headers, data=urllib.parse.urlencode(payload), timeout=60)
        resp.raise_for_status()
        data = resp.json()
        return data["sessionId"], data["userId"]

    @retry((requests.exceptions.Timeout,), delay=10, times=2)
    def _session_keep_alive(self) -> None:
        url = urllib.parse.urljoin(self.url, "keep-alive")
        headers = {"Authorization": self.session_id, "Accept": "application/json"}
        resp = requests.post(url, headers=headers, timeout=60)
        resp.raise_for_status()

    @retry((ExpiredTokenException,), delay=1, times=2)
    def submit_vql_query(self, model: T, execution_type: Literal["Incremental", "Load"] = "Incremental", id: str | None = None) -> List[T]:
        url = urllib.parse.urljoin(self.url, "query")
        if id:
            url = urllib.parse.urljoin(url + "/", id)
        query = model.get_query(execution_type)
        encoded_query = urllib.parse.quote(query.encode("ascii"))
        payload = self._create_payload({"q": encoded_query})
        headers = {"Authorization": self.session_id, "Accept": "application/json", "X-VaultAPI-DescribeQuery": "true", "Content-Type": "application/x-www-form-urlencoded"}
        resp = requests.post(url, headers=headers, data=payload, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("responseStatus") == "FAILURE":
            errors = result.get("errors", [])
            if errors and errors[0].get("type") == "INVALID_SESSION_ID":
                raise ExpiredTokenException()
        validated = [model.model_validate(x) for x in result.get("data", [])]
        # handle pagination
        rd = result.get("responseDetails", {})
        nxt = rd.get("next_page")
        if nxt:
            time.sleep(0.5)
            nid = nxt.split("/")[-1]
            validated.extend(self.submit_vql_query(model, execution_type, nid))
        return validated

    @retry((ExpiredTokenException,), delay=1, times=2)
    def submit_export_documents(self, documents: List[DocumentMetadata]) -> str:
        url = urllib.parse.urljoin(self.url, "objects/documents/batch/actions/fileextract?source=false&renditions=true")
        payload = "[" + ",".join([d.get_document_id() for d in documents]) + "]"
        headers = {"Authorization": self.session_id, "Content-Type": "application/json", "Accept": "application/json"}
        resp = requests.post(url, headers=headers, data=payload.encode("ascii"), timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("responseStatus") == "FAILURE":
            errors = result.get("errors", [])
            if errors and errors[0].get("type") == "INVALID_SESSION_ID":
                raise ExpiredTokenException()
        job_id = str(result["job_id"])
        return job_id

    @retry((NotReadyException,), delay=60, times=5)
    @retry((ExpiredTokenException,), delay=1, times=2)
    def retrieve_export_documents_results(self, job_id: str) -> List[Document]:
        url = urllib.parse.urljoin(self.url, f"objects/documents/batch/actions/fileextract/{job_id}/results")
        headers = {"Authorization": self.session_id, "Accept": "application/json"}
        resp = requests.get(url, headers=headers, timeout=60)
        resp.raise_for_status()
        result = resp.json()
        if result.get("responseStatus") == "FAILURE":
            errors = result.get("errors", [])
            if errors and errors[0].get("type") == "INVALID_SESSION_ID":
                raise ExpiredTokenException()
            raise NotReadyException()
        documents = [Document.model_validate(x) for x in result.get("data", []) if x.get("responseStatus") == "SUCCESS"]
        return documents

    @retry((ExpiredTokenException,), delay=1, times=2)
    def download_item_content(self, document: Document) -> Document:
        item = f"u{document.user_id}/{document.file}"
        url = urllib.parse.urljoin(self.url, f"services/file_staging/items/content/{item}")
        os.makedirs("tmp", exist_ok=True)
        file_path = os.path.join("tmp", f"{document.id}.pdf")
        headers = {"Authorization": self.session_id, "Accept": "application/json"}
        with requests.get(url, headers=headers, stream=True, timeout=60) as resp:
            resp.raise_for_status()
            with open(file_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
        document.system_path = file_path
        document.file = f"{document.id}.pdf"
        return document
