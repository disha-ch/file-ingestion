"""Secrets Manager lightweight wrapper."""
from __future__ import annotations
import json
import boto3
from typing import Any, Dict


class SecretManager:
    def __init__(self, secret_name: str, region_name: str | None = None):
        self.secret_name = secret_name
        self.client = boto3.client("secretsmanager", region_name=region_name)
        self._cache: Dict[str, Any] = {}

    def get(self, key: str) -> Any:
        # Lazy load full secret and cache
        if not self._cache:
            resp = self.client.get_secret_value(SecretId=self.secret_name)
            secret_string = resp.get("SecretString", "{}")
            try:
                self._cache = json.loads(secret_string)
            except Exception:
                self._cache = {}
        return self._cache.get(key)
