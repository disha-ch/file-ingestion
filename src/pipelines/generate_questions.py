"""Generate LLM-based questions for a document and persist them."""
from __future__ import annotations
import uuid
import os
from src.docling import DoclingInterface
from src.logging import SingletonLogger
from src.utils import initialize_services

logger = SingletonLogger().get_logger()

def generate_questions(folder_name: str, file_name: str) -> None:
    list_of_documents = {"Experiment ID": os.getenv("experiment_id", "test")}
    _, file_ingestion, _, s3, email, _, dynamodb, llm, _ = initialize_services()
    local_file_path = f"tmp/{file_name}"
    try:
        s3.download_document(folder_name, file_name, local_file_path)
        file_content = DoclingInterface().convert_document(local_file_path)
        md_text = file_content.document.export_to_markdown()
        md_file_path = f"{os.path.splitext(local_file_path)[0]}.md"
        with open(md_file_path, "w", encoding="utf-8") as f:
            f.write(md_text)
        prompt = f"Generate a set of questions that are very related with the following document: {md_text}. I want at least 5 related questions."
        questions = llm.get_with_structured_output(prompt, list)["questions"] if hasattr(llm, "get_with_structured_output") else []
        for q in questions:
            file = file_ingestion.get_document(file_name.split(".")[0])
            q["Expected"] = file.get("document_number") if file else ""
            q["Generator"] = "AI"
            q["question_id"] = str(uuid.uuid4())
            dynamodb.put_item(q)
    except Exception as e:
        logger.error("An error occurred: %s", str(e), exc_info=True)
        list_of_documents["error"] = str(e)
        email.format_email("[FAILURE]. An error was found.", list_of_documents)
