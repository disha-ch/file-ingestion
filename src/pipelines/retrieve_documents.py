"""Full retrieval pipeline orchestration (planning)."""
from __future__ import annotations
import datetime
import json
import os
import re
from functools import lru_cache
from typing import Dict, List, Literal
from collections import Counter

from src.logging import SingletonLogger
from src.connectors import S3, SNS, DynamoDB, Email, Veeva, FileIngestion
from src.models import (Country, DocumentMetadata, WithdrawnDocument, BusinessArea1, BusinessArea2, BusinessArea3, BusinessArea4, BusinessArea5, BusinessArea6, BusinessProcessL1, BusinessProcessL2, BusinessProcessL3, BusinessProcessL4, BusinessProcessL5, Equipment, EquipmentType, MaterialGroup, ObjectReference, ProductFamily, ProductVariant, SubstanceMaterialEquipment)
from src.utils import initialize_services, get_impacted_business_areas_incremental, get_impacted_business_areas_load

logger = SingletonLogger().get_logger()

@lru_cache(maxsize=None)
def get_veeva_data(veeva: Veeva, s3: S3):
    return {
        "countries": s3.get_json("constants", "countries.json") | {c.id: c.name for c in veeva.submit_vql_query(Country)},
        # ... same pattern for other lookup types
    }

def update_s3_json_files(s3: S3, veeva_data: Dict[str, Dict[str, str]]):
    for category, data in veeva_data.items():
        s3.put_object(json.dumps(data, indent=2), "constants", f"{category}.json")

def compute_document_type(document_number: str) -> str:
    full_pattern = r"\b(?:SOP|BDR|WI|SPEC|REP|GUID|FORM|STND|TMP)\b"
    results = re.findall(full_pattern, document_number or "")
    return results[0] if len(results) else ""

def process_documents(veeva: Veeva, dynamodb: DynamoDB, veeva_data: Dict[str, Dict[str, str]], execution_type: Literal["Incremental", "Load"]):
    logger.info("Fetching business documents from Veeva...")
    docs = veeva.submit_vql_query(DocumentMetadata, execution_type)
    for doc in docs:
        doc.rename_relations(**veeva_data)
    if execution_type == "Incremental":
        impacted_business_areas = get_impacted_business_areas_incremental()
    else:
        impacted_business_areas = get_impacted_business_areas_load()

    download_files_list = []
    list_of_documents = {}
    for doc in docs:
        # site detection
        def find_matching_key(doc, impacted_business_areas):
            for key, value in impacted_business_areas.items():
                if DocumentMetadata.filter_by_impacted_business_area(doc, value):
                    logger.info("Matched doc %s to site %s", doc.file_id, key)
                    return key
            return None

        matching_key = find_matching_key(doc, impacted_business_areas)
        if matching_key is None:
            logger.info("Skipping doc %s: No site match found.", doc.file_id)
            continue

        metadata = dynamodb.get_document(str(doc.file_id))
        action = "CREATE" if metadata is None else "UPDATE"
        list_of_documents[doc.file_id] = action

        if execution_type == "Incremental" and action == "UPDATE":
            try:
                dynamodb.delete_document(metadata)
            except Exception as e:
                logger.error("Error preparing update for doc %s: %s", doc.file_id, str(e))
            metadata = doc.model_dump()
        else:
            metadata = metadata or doc.model_dump()

        metadata["site"] = matching_key
        metadata["document_type"] = compute_document_type(metadata.get("document_number", ""))
        metadata["status"] = "DOWNLOADING"
        dynamodb.update_document(metadata)
        download_files_list.append(doc)

    return download_files_list, list_of_documents

def submit_export_jobs(veeva: Veeva, sns: SNS, download_files_list: List[DocumentMetadata]) -> List[str]:
    chunked_list = [download_files_list[i:i+100] for i in range(0, len(download_files_list), 100)]
    job_ids = []
    for chunk in chunked_list:
        job_id = veeva.submit_export_documents(chunk)
        job_ids.append(str(job_id))
    return job_ids

def delete_withdrawn_documents(veeva: Veeva, dynamodb: DynamoDB, s3: S3):
    delete_documents = veeva.submit_vql_query(WithdrawnDocument)
    deleted_docs = {}
    for doc in delete_documents:
        metadata = dynamodb.get_document(str(doc.file_id))
        if metadata is not None:
            site = metadata["site"]
            document_type = metadata["document_type"].lower()
            s3.delete_object(f"kb_documents/{site}/{document_type}", f"{doc.file_id}.pdf")
            s3.delete_object(f"kb_documents/{site}/{document_type}", f"{doc.file_id}.pdf.metadata.json")
            dynamodb.delete_document(metadata)
            deleted_docs[doc.file_id] = "DELETE"
    return deleted_docs

def retrieve_documents(experiment_id: str, execution_type: Literal["Incremental", "Load"]) -> List[str]:
    list_of_documents = {"Experiment ID": experiment_id}
    veeva, dynamodb, sns, s3, email, _, _, _, _ = initialize_services()
    try:
        veeva_data = get_veeva_data(veeva, s3)
        update_s3_json_files(s3, veeva_data)
        download_files_list, doc_status = process_documents(veeva, dynamodb, veeva_data, execution_type)
        list_of_documents.update(doc_status)
        list_of_documents["nº Checked Documents"] = len(download_files_list)
        job_ids = submit_export_jobs(veeva, sns, download_files_list)
        list_of_documents["job_ids"] = "-".join(job_ids) if job_ids else ""
        deleted_docs = delete_withdrawn_documents(veeva, dynamodb, s3)
        list_of_documents.update(deleted_docs)
        email.format_email("[SUCCESS]. Synchronization planned.", list_of_documents)
        return job_ids
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        list_of_documents["error"] = str(e)
        email.format_email("[FAILURE]. An error was found.", list_of_documents)
        return []

def pipeline_retrieve_documents(experiment_id: str, execution_type: str, email: Email):
    start_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    process_steps = []
    summary = {}
    errors = []
    raw_data = {}
    try:
        process_steps.append({"step": "Initialize services", "description": "Connecting to services", "status": "OK"})
        veeva, file_ingestion, sns, s3, _, bedrock, kbr_questions_table, llm, sm, config = initialize_services()
        veeva_data = get_veeva_data(veeva, s3)
        update_s3_json_files(s3, veeva_data)
        download_files_list, doc_status = process_documents(veeva, file_ingestion, veeva_data, execution_type)
        process_steps.append({"step": "Process documents", "description": f"{len(download_files_list)} documents processed.", "status": "OK", "details": "<br>".join([f"{doc}: {status}" for doc, status in doc_status.items()])})
        job_ids = submit_export_jobs(veeva, sns, download_files_list)
        process_steps.append({"step": "Submit export jobs", "description": f"{len(job_ids)} export jobs submitted.", "status": "OK", "details": ", ".join(job_ids)})
        deleted_docs = delete_withdrawn_documents(veeva, file_ingestion, s3)
        if deleted_docs:
            process_steps.append({"step": "Delete withdrawn documents", "description": f"{len(deleted_docs)} withdrawn documents deleted.", "status": "OK", "details": ", ".join([str(doc_id) for doc_id in deleted_docs])})
        else:
            process_steps.append({"step": "Delete withdrawn documents", "description": "No withdrawn documents found.", "status": "OK"})
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        email_data = {"start_time": start_time, "end_time": end_time, "process_steps": process_steps, "summary": summary, "errors": errors, "raw_data": raw_data}
        email.format_email(f"Document Retrieval Pipeline [{experiment_id}] - SUCCESS", email_data)
        return job_ids
    except Exception as e:
        errors.append(str(e))
        end_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        email_data = {"start_time": start_time, "end_time": end_time, "process_steps": process_steps, "summary": summary, "errors": errors, "raw_data": raw_data}
        email.format_email(f"Document Retrieval Pipeline [{experiment_id}] - FAILURE", email_data)
        raise

def dict_count(d):
    return dict(Counter(d.values()))
