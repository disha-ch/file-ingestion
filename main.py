# main.py
"""
Main entrypoint for the SOP document ingestion and question-generation pipeline.

This script orchestrates different phases of the pipeline depending on the day
of the week or an explicit override via environment variables.

Phases:
    - Document retrieval from Veeva Vault
    - Download of associated SOP files from S3
    - Conversion, metadata extraction, and ingestion into DynamoDB
    - LLM-powered question generation for knowledge base enrichment

Environment Variables:
    PIPELINE_PHASE: Optional. Override phase selection (values: "retrieve", "download", "generate").
    ENV: Deployment environment (dev/test/prod).
"""

import os
import sys
import logging
from datetime import datetime

from src.pipelines import retrieve_documents, download_documents, generate_questions
from src.logging import SingletonLogger
from src.utils import initialize_services


logger = SingletonLogger().get_logger()


def run_pipeline(phase: str) -> None:
    """
    Dispatch the pipeline phase to the appropriate handler.

    Args:
        phase (str): The pipeline phase to run. Valid values:
            - "retrieve"
            - "download"
            - "generate"
    """
    logger.info("Starting pipeline phase: %s", phase)

    if phase == "retrieve":
        retrieve_documents.retrieve_documents()
    elif phase == "download":
        download_documents.download_documents()
    elif phase == "generate":
        # In production this might iterate over new documents and call generate_questions
        # For simplicity, we just log the trigger.
        logger.info("Triggering question generation phase...")
        # Example call:
        # generate_questions.generate_questions(folder_name, file_name)
    else:
        logger.error("Invalid pipeline phase: %s", phase)
        raise ValueError(f"Invalid pipeline phase: {phase}")

    logger.info("Completed pipeline phase: %s", phase)


def select_phase() -> str:
    """
    Select which pipeline phase to execute based on weekday or env override.

    Returns:
        str: The selected phase.
    """
    override_phase = os.getenv("PIPELINE_PHASE")
    if override_phase:
        return override_phase.lower()

    weekday = datetime.utcnow().weekday()
    # Example logic:
    if weekday in (0, 3):  # Monday, Thursday
        return "retrieve"
    elif weekday in (1, 4):  # Tuesday, Friday
        return "download"
    elif weekday in (2, 5):  # Wednesday, Saturday
        return "generate"
    else:
        # Sunday - maintenance or no run
        logger.info("No pipeline scheduled on Sunday.")
        sys.exit(0)


if __name__ == "__main__":
    try:
        logger.info("Initializing services...")
        initialize_services()

        phase = select_phase()
        run_pipeline(phase)

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error("Pipeline execution failed: %s", str(e), exc_info=True)
        sys.exit(1)
