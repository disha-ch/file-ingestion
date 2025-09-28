"""S3 connector: simple helpers for get/put/delete and JSON helpers."""
from __future__ import annotations
import json
import io
import boto3
from typing import Dict, Any


class S3:
    def __init__(self, bucket: str, region_name: str | None = None):
        self.bucket = bucket
        self.client = boto3.client("s3", region_name=region_name)

    def put_object(self, content: str, folder: str, file_name: str) -> None:
        key = f"{folder.rstrip('/')}/{file_name}"
        self.client.put_object(Bucket=self.bucket, Key=key, Body=content.encode("utf-8"))

    def upload_document(self, prefix: str, document) -> str:
        # document expected to have system_path and file attributes
        key = f"{prefix.rstrip('/')}/{document.file}"
        with open(document.system_path, "rb") as f:
            self.client.put_object(Bucket=self.bucket, Key=key, Body=f.read())
        return f"s3://{self.bucket}/{key}"

    def get_json(self, folder: str, file_name: str) -> Dict[str, str]:
        key = f"{folder.rstrip('/')}/{file_name}"
        try:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            body = resp["Body"].read().decode("utf-8")
            return json.loads(body)
        except self.client.exceptions.NoSuchKey:
            return {}
        except Exception:
            return {}

    def delete_object(self, folder: str, file_name: str) -> None:
        key = f"{folder.rstrip('/')}/{file_name}"
        self.client.delete_object(Bucket=self.bucket, Key=key)

    def download_document(self, folder: str, file_name: str, local_path: str) -> None:
        key = f"{folder.rstrip('/')}/{file_name}"
        with open(local_path, "wb") as f:
            resp = self.client.get_object(Bucket=self.bucket, Key=key)
            f.write(resp["Body"].read())
