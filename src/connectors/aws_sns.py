"""SNS wrapper for publish operations."""
from __future__ import annotations
import boto3


class SNS:
    def __init__(self, topic_arn: str, region_name: str | None = None):
        self.topic_arn = topic_arn
        self.client = boto3.client("sns", region_name=region_name)

    def publish(self, message: str) -> None:
        self.client.publish(TopicArn=self.topic_arn, Message=message)
