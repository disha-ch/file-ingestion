from __future__ import annotations
from typing import Dict, Optional
from src.logging import SingletonLogger

logger = SingletonLogger().get_logger()

class Document:
    def __init__(self, id: int, document_status: str, major_version_number: int, minor_version_number: int, file: str, user_id: Optional[int] = None, system_path: Optional[str] = None, s3_path: Optional[str] = None):
        self.id = id
        self.major_version_number = major_version_number
        self.minor_version_number = minor_version_number
        self.document_status = document_status
        self.file = file
        self.user_id = user_id
        self.system_path = system_path
        self.s3_path = s3_path

    @classmethod
    def model_validate(cls, api_response: Dict[str, str], **kwargs) -> "Document":
        status = api_response.get("status__v")
        if status is None:
            logger.warning("[MISSING FIELD] 'status__v' not found in API response: %s", api_response)
            status = "None"
        else:
            logger.debug("[FOUND FIELD] 'status__v' found in API response.")
        return cls(
            id=int(api_response["id"]),
            major_version_number=int(api_response.get("major_version_number__v", 0)),
            minor_version_number=int(api_response.get("minor_version_number__v", 0)),
            document_status=str(status),
            file=api_response.get("file", ""),
            user_id=int(api_response["user_id__v"]) if api_response.get("user_id__v") else None,
        )
