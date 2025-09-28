"""
File ingestion DynamoDB helper: thin wrapper for file ingestion table operations.
This module intentionally keeps operations simple and testable.
"""
from __future__ import annotations

from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError


class FileIngestionTable:
    table_name: str

    def __init__(self, table_name: str, region_name: str | None = None):
        self.table_name = table_name
        self.client = boto3.client("dynamodb", region_name=region_name)

    def get_document(self, file_id: str) -> Optional[Dict]:
        try:
            resp = self.client.get_item(TableName=self.table_name, Key={"file_id": {"S": file_id}})
            return resp.get("Item")
        except ClientError:
            raise

    def put_document(self, item: Dict) -> None:
        # item expected to be a plain dict with string values
        ddb_item = {k: {"S": str(v)} for k, v in item.items() if v is not None}
        self.client.put_item(TableName=self.table_name, Item=ddb_item)

    def delete_document(self, item: Dict) -> None:
        # item expected to contain 'file_id'
        self.client.delete_item(TableName=self.table_name, Key={"file_id": {"S": str(item["file_id"])}})
