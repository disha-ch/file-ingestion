"""Generic simple DynamoDB wrapper used by the pipeline."""
from __future__ import annotations
from typing import Dict, Optional
import boto3
from botocore.exceptions import ClientError


class DynamoDB:
    def __init__(self, table_name: str, region_name: str | None = None):
        self.table_name = table_name
        self.client = boto3.resource("dynamodb", region_name=region_name)
        self.table = self.client.Table(self.table_name)

    def get_document(self, file_id: str) -> Optional[Dict]:
        try:
            resp = self.table.get_item(Key={"file_id": str(file_id)})
            return resp.get("Item")
        except ClientError:
            raise

    def put_item(self, item: Dict) -> None:
        self.table.put_item(Item=item)

    def update_document(self, item: Dict) -> None:
        # Full overwrite semantics for simplicity in this scaffold
        self.put_item(item)

    def delete_document(self, item: Dict) -> None:
        if "file_id" not in item:
            return
        self.table.delete_item(Key={"file_id": str(item["file_id"])})
