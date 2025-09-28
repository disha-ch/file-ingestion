"""Email helper: simple placeholder that logs formatted emails.

In production this maps to an SES or Lambda-based email sender.
"""
from __future__ import annotations
from typing import Any, Dict, List
import json
import logging

logger = logging.getLogger("email")


class Email:
    def __init__(self, sender_lambda: str | None, recipients: List[str] | None = None):
        self.sender_lambda = sender_lambda
        self.recipients = recipients or []

    def format_email(self, subject: str, payload: Dict[str, Any]) -> None:
        # In prod: call Lambda or SES. Here we log structured payload for tests.
        logger.info("EMAIL [%s] -> %s\n%s", subject, ", ".join(self.recipients), json.dumps(payload, indent=2))

    def format_kb_ds_sync_summary(self, subject: str, kb_data: Dict[str, Any]) -> None:
        logger.info("KB SYNC EMAIL [%s]\n%s", subject, json.dumps(kb_data, indent=2))
