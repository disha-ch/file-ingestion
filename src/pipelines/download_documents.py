"""Document download & process pipeline"""
from __future__ import annotations
import json
import os
from typing import Any, Dict, List, Tuple
from datetime import datetime
from src.logging import SingletonLogger
from src.utils import initialize_services

logger = SingletonLogger().get_logger()

def filter_metadata(d: Dict[str, Any]) -> Dict[str, str]:
    keys_to_keep = [
        "impacted_business_area_1", "impacted_business_area_2", "impacted_business_area_3",
        "impacted_business_area_4", "impacted_business_area_5", "impacted_business_area_6",
        "owning_business_area_1", "owning_business_area_2", "owning_business_area_3", "owning_business_area_4",
        "name", "document_number", "language", "country", "file_id", "major_version", "minor_version",
    ]
    filtered = {}
    for key in keys_to_keep:
        val = d.get(key)
        if not val or (isinstance(val, list) and len(val) == 0):
            filtered[key] = "UNKNOWN"
        else:
            filtered[key] = ", ".join(val) if isinstance(val, list) else str(val)
    logger.info("[DEBUG] Filtered metadata for S3 upload: %s", filtered)
    return filtered

def process_document(doc: Any, veeva, dynamodb, s3, bedrock) -> None:
    metadata = dynamodb.get_document(str(doc.id))
    if not metadata:
        logger.warning("No DynamoDB entry for doc %s. Skipping.", doc.id)
        return
    current_status = metadata.get("status", "UNKNOWN")
    if current_status != "DOWNLOADING":
        logger.info("Skipping doc %s: Status is %s", doc.id, current_status)
        return

    # version check
    veeva_major_version = getattr(doc, "major_version_number", None) or (doc.__dict__.get("data", {}) or {}).get("major_version_number__v")
    db_major_version = metadata.get("major_version", "0")
    if veeva_major_version is not None and str(veeva_major_version) != str(db_major_version):
        logger.warning("Version mismatch for doc %s: veeva=%s db=%s", doc.id, veeva_major_version, db_major_version)

    logger.info("Downloading content for doc %s", doc.id)
    doc = veeva.download_item_content(doc)

    site = metadata["site"]
    document_type = metadata.get("document_type", "").lower()
    site = site.replace(" ", "_").replace("?", "").replace("&", "").replace("/", "_")
    document_type = document_type.replace(" ", "_").replace("?", "").replace("&", "").replace("/", "_")

    s3_path = f"kb_documents/{site}/{document_type}"
    logger.info("Uploading doc %s to S3 at %s", doc.id, s3_path)
    doc.s3_path = s3.upload_document(s3_path, doc)

    metadata_s3 = {"metadataAttributes": filter_metadata(metadata)}
    s3.put_object(json.dumps(metadata_s3), s3_path, f"{doc.file}.metadata.json")

    metadata["status"] = "OK"
    dynamodb.update_document(metadata)
    logger.info("Updated status to OK for doc %s in DynamoDB.", doc.id)


def download_documents(job_id: str, experiment_id: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    veeva, dynamodb, _, s3, email, bedrock, _, _, _, _ = initialize_services()
    results = []
    errors = []
    try:
        os.environ["experiment_id"] = f"{experiment_id} - {str(job_id)}"
        export_documents = veeva.retrieve_export_documents_results(job_id)
        for doc in export_documents:
            try:
                process_document(doc, veeva, dynamodb, s3, bedrock)
                results.append({"step": "Downloaded document", "description": f"Document ID: {doc.id}", "status": "OK"})
            except Exception as doc_err:
                errors.append({"step": "Download document failed", "description": f"Document ID: {getattr(doc,'id','unknown')}", "status": "FAILED", "details": str(doc_err)})
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        errors.append({"step": "Download job failed", "description": f"Job ID: {job_id}", "status": "FAILED", "details": str(e)})
    return results, errors
