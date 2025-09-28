# main.py
"""
Main entrypoint for the SOP document ingestion and question-generation pipeline.

This script orchestrates pipeline execution in two modes:
    - Incremental (Mon–Fri): Fetch and process only recent document updates.
    - Initial Load (Sat–Sun): Perform a full load of all eligible documents.

Environment Variables:
    PIPELINE_PHASE: Optional. Override phase selection ("retrieve", "download", "generate").
    LOAD_TYPE: Optional. Force load type ("Incremental", "Load").
    ENV: Deployment environment (dev/test/prod).
"""

import os
import sys
from datetime import datetime

from src.pipelines import retrieve_documents, download_documents, generate_questions
from src.logging import SingletonLogger
from src.utils import initialize_services


logger = SingletonLogger().get_logger()


def run_pipeline(phase: str, load_type: str) -> None:
    """
    Dispatch the pipeline phase to the appropriate handler.

    Args:
        phase (str): Pipeline phase. One of ["retrieve", "download", "generate"].
        load_type (str): Load type. One of ["Incremental", "Load"].
    """
    logger.info("Starting pipeline phase: %s (Load Type: %s)", phase, load_type)

    if phase == "retrieve":
        retrieve_documents.retrieve_documents(load_type)
    elif phase == "download":
        download_documents.download_documents(load_type)
    elif phase == "generate":
        logger.info("Triggering question generation (Load Type: %s)...", load_type)
        # Example call (iterating over docs would be real logic):
        # generate_questions.generate_questions(folder_name, file_name)
    else:
        logger.error("Invalid pipeline phase: %s", phase)
        raise ValueError(f"Invalid pipeline phase: {phase}")

    logger.info("Completed pipeline phase: %s (Load Type: %s)", phase, load_type)


def select_load_type() -> str:
    """
    Determine whether to run Incremental or Initial Load based on the day of week.

    Returns:
        str: "Incremental" (Mon–Fri) or "Load" (Sat–Sun).
    """
    override = os.getenv("LOAD_TYPE")
    if override:
        return override.capitalize()

    weekday = datetime.utcnow().weekday()
    if weekday in range(0, 5):  # Monday–Friday
        return "Incremental"
    elif weekday in (5, 6):  # Saturday–Sunday
        return "Load"
    else:
        raise ValueError("Unexpected weekday calculation")


def select_phase() -> str:
    """
    Determine which pipeline phase to run based on environment variable.

    Returns:
        str: "retrieve", "download", or "generate".
    """
    override_phase = os.getenv("PIPELINE_PHASE")
    if override_phase:
        return override_phase.lower()

    # Default phase when not overridden
    return "retrieve"


if __name__ == "__main__":
    try:
        logger.info("Initializing services...")
        initialize_services()

        load_type = select_load_type()
        phase = select_phase()

        logger.info("Selected Load Type: %s | Phase: %s", load_type, phase)
        run_pipeline(phase, load_type)

        logger.info("Pipeline execution completed successfully.")

    except Exception as e:
        logger.error("Pipeline execution failed: %s", str(e), exc_info=True)
        sys.exit(1)
