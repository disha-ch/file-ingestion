"""Service initialization and helpers."""
from __future__ import annotations
import json
import os
from functools import lru_cache
from typing import Any, Dict, List, Tuple

from src.connectors import Veeva, FileIngestion, SNS, S3, Email, BedrockAgent, DynamoDB, LLM, SecretManager

PIPELINE_CONFIG_PATH = os.environ.get("PIPELINE_CONFIG_PATH", "pipeline_config.dev.json")

def normalize_impacted_business_areas(sites_dict: Dict[str, Any]) -> Dict[str, List[List[str]]]:
    out = {}
    for site, site_info in sites_dict.items():
        filter_val = site_info.get("load_filter_value", {})
        areas = [filter_val.get(f"impacted_business_area_{i}", []) for i in range(1,7)]
        out[site] = areas
    return out

@lru_cache()
def load_pipeline_config(config_path: str | None = None) -> Dict[str, Any]:
    config_path = config_path or PIPELINE_CONFIG_PATH
    with open(config_path, "r", encoding="utf-8") as f:
        config = json.load(f)
    return config

@lru_cache()
def initialize_services() -> Tuple:
    config = load_pipeline_config()
    secret_name = os.environ.get("secret_name")
    if not secret_name:
        raise KeyError("secret_name environment variable is required")
    sm = SecretManager(secret_name)
    veeva = Veeva(
        f"{sm.get('veeva_url')}/api/v24.3/",
        sm.get("veeva_username"),
        sm.get("veeva_password"),
        sm.get("veeva_session_id")
    )
    file_ingestion_table = FileIngestion(sm.get("dynamodb_file_ingest_table"))
    kbr_questions_table = DynamoDB(config.get("dynamodb_table"))
    sns = SNS("arn:aws:sns:placeholder")  # placeholder
    s3 = S3(config.get("s3_bucket"))
    email = Email("placeholder", [
        "ops@example.com"
    ])
    bedrock = BedrockAgent(sm.get("knowledge_id"), sm.get("datasource_id"))
    llm = LLM()
    return (veeva, file_ingestion_table, sns, s3, email, bedrock, kbr_questions_table, llm, sm, config)

@lru_cache()
def get_impacted_business_areas_incremental():
    config = load_pipeline_config()
    sites = config.get("incremental_load_sites", {})
    return normalize_impacted_business_areas(sites)

@lru_cache()
def get_impacted_business_areas_load():
    config = load_pipeline_config()
    sites = config.get("initial_load_sites", {})
    return normalize_impacted_business_areas(sites)

@lru_cache()
def get_countries():
    _, _, _, _, _, _, _, _, sm, _ = initialize_services()
    filters = json.loads(sm.get("veeva_filters") or "{}")
    return filters.get("countries", [])
