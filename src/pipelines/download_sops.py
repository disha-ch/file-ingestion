"""High-level retrieval & export orchestration (planning)"""
from __future__ import annotations
import json
import os
import re
from functools import lru_cache
from typing import Any, Dict, List
from tqdm import tqdm
from src.logging import SingletonLogger
from src.models import (
    BusinessArea1, BusinessArea2, BusinessArea3, BusinessArea4, BusinessArea5, BusinessArea6,
    BusinessProcessL1, BusinessProcessL2, BusinessProcessL3, BusinessProcessL4, BusinessProcessL5,
    Country, DocumentMetadata, Equipment, EquipmentType, MaterialGroup, ObjectReference,
    ProductFamily, ProductVariant, SubstanceMaterialEquipment,
)
from src.utils import initialize_services

logger = SingletonLogger().get_logger()

@lru_cache(maxsize=None)
def get_veeva_data(veeva, s3) -> Dict[str, Dict[str, str]]:
    return {
        "countries": s3.get_json("constants", "countries.json") | {c.id: c.name for c in veeva.submit_vql_query(Country)},
        "object_reference": s3.get_json("constants", "object_reference.json") | {c.id: c.name for c in veeva.submit_vql_query(ObjectReference)},
        "business_area_1": s3.get_json("constants", "business_area_1.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea1)},
        "business_area_2": s3.get_json("constants", "business_area_2.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea2)},
        "business_area_3": s3.get_json("constants", "business_area_3.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea3)},
        "business_area_4": s3.get_json("constants", "business_area_4.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea4)},
        "business_area_5": s3.get_json("constants", "business_area_5.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea5)},
        "business_area_6": s3.get_json("constants", "business_area_6.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessArea6)},
        "product_family": s3.get_json("constants", "product_family.json") | {c.id: c.name for c in veeva.submit_vql_query(ProductFamily)},
        "product_variant": s3.get_json("constants", "product_variant.json") | {c.id: c.name for c in veeva.submit_vql_query(ProductVariant)},
        "material_group": s3.get_json("constants", "material_group.json") | {c.id: c.name for c in veeva.submit_vql_query(MaterialGroup)},
        "substance_material": s3.get_json("constants", "substance_material.json") | {c.id: c.name for c in veeva.submit_vql_query(SubstanceMaterialEquipment)},
        "equipment": s3.get_json("constants", "equipment.json") | {c.id: c.name for c in veeva.submit_vql_query(Equipment)},
        "equipment_type": s3.get_json("constants", "equipment_type.json") | {c.id: c.name for c in veeva.submit_vql_query(EquipmentType)},
        "business_process_l1": s3.get_json("constants", "business_process_l1.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessProcessL1)},
        "business_process_l2": s3.get_json("constants", "business_process_l2.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessProcessL2)},
        "business_process_l3": s3.get_json("constants", "business_process_l3.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessProcessL3)},
        "business_process_l4": s3.get_json("constants", "business_process_l4.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessProcessL4)},
        "business_process_l5": s3.get_json("constants", "business_process_l5.json") | {c.id: c.name for c in veeva.submit_vql_query(BusinessProcessL5)},
    }

def update_s3_json_files(s3, veeva_data: Dict[str, Dict[str, str]]) -> None:
    logger.info("Starting update of S3 JSON files with latest Veeva data")
    for category, data in veeva_data.items():
        file_name = f"{category}.json"
        folder_name = "constants"
        try:
            json_content = json.dumps(data, indent=2)
            s3.put_object(json_content, folder_name, file_name)
            logger.info("Successfully updated S3 file: %s/%s", folder_name, file_name)
        except Exception as e:
            logger.error("Error updating S3 file %s/%s: %s", folder_name, file_name, str(e))
    logger.info("Completed update of S3 JSON files")

def compute_document_type(document_number: str) -> str:
    full_pattern = r"\b(?:SOP|BDR|WI|SPEC|REP|GUID|FORM|STND|TMP)\b"
    matches = re.findall(full_pattern, document_number or "")
    return matches[0] if matches else ""

def process_documents(veeva, s3, veeva_data, documents: List[str]) -> List[DocumentMetadata]:
    docs = veeva.submit_vql_query(DocumentMetadata)
    for doc in docs:
        doc.rename_relations(**veeva_data)
    filtered_docs = [doc for doc in docs if doc.document_number in documents]
    logger.info("A total of %d have been detected.", len(filtered_docs))
    return filtered_docs

def submit_export_jobs(veeva, download_files_list: List[DocumentMetadata]) -> List[str]:
    chunked_list = [download_files_list[i:i+100] for i in range(0, len(download_files_list), 100)]
    job_ids = []
    for chunk in chunked_list:
        job_id = veeva.submit_export_documents(chunk)
        job_ids.append(str(job_id))
    return job_ids

def download_document(doc, veeva, s3) -> None:
    doc = veeva.download_item_content(doc)
    doc.s3_path = s3.upload_document("ingestion", doc)

def retrieve_documents(documents: List[str]) -> List[str]:
    list_of_documents = {"Experiment ID": os.getenv("experiment_id", "env")}
    veeva, _, sns, s3, email, _, _, _, _ = initialize_services()
    try:
        veeva_data = get_veeva_data(veeva, s3)
        update_s3_json_files(s3, veeva_data)
        download_files_list = process_documents(veeva, s3, veeva_data, documents)
        list_of_documents["nº Checked Documents"] = len(download_files_list)
        job_ids = submit_export_jobs(veeva, download_files_list)
        list_of_documents["job_ids"] = "-".join(job_ids) if job_ids else ""
        for job_id in job_ids:
            export_documents = veeva.retrieve_export_documents_results(job_id)
            for doc in export_documents:
                download_document(doc, veeva, s3)
        return job_ids
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        list_of_documents["error"] = str(e)
        email.format_email("[FAILURE]. An error was found.", list_of_documents)
        return []
