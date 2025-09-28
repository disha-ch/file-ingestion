SOP Metadata Ingestion & QnA Assistant

An **end-to-end metadata ingestion and knowledge retrieval pipeline** built on **AWS** and enhanced with **Generative AI**.
This system ingests SOP (Standard Operating Procedure) documents, enriches them with metadata, and powers a **chatbot assistant** that answers site-specific questions in natural language.

The solution demonstrates how **cloud-native infrastructure** + **LLM-powered enrichment** can transform static compliance documents into **living knowledge bases**.

---

## Features

* **Automated Document Ingestion** – fetches files from enterprise repositories and cloud sources.
* **Metadata Enrichment** – detects language, classifies documents, extracts sites, and tracks versions.
* **Generative AI Q&A** – uses AWS Bedrock-integrated LLMs to generate contextual Q&A pairs from documents.
* **Knowledge Base Assistant** – “SOP Assistant” chatbot consumes enriched files, enabling **site-specific Q&A**.
* **AWS-Native Infrastructure** – S3, DynamoDB, Secrets Manager, and SNS for storage, metadata, config, and notifications.
* **Observability & Reliability** – structured logging, retry decorators, exception handling, and automated email reports.
* **Configurable Pipelines** – JSON-based configuration for dev/test/prod environments.

---

## Architecture

```text
azcdi-us-ops-sop-fileingest-datapipeline/
├─ src/
│  ├─ connectors/        # AWS + external system connectors
│  ├─ exceptions/        # Custom exception classes
│  ├─ models/            # Document, metadata, site, language models
│  ├─ pipelines/         # Ingestion, retrieval, Q&A generation workflows
│  ├─ decorators.py      # Retry, logging, timing
│  ├─ docling.py         # Document parsing utilities
│  ├─ experiment.py      # Utility functions for experiments
│  ├─ logging.py         # Centralized logging setup
│  ├─ utils.py           # Service initialization & helper functions
│  └─ __init__.py
├─ tests/                # Unit tests for connectors, pipelines, models
├─ tmp/                  # Temp runtime artifacts (gitignored)
├─ main.py               # Entry point: orchestrates pipelines per weekday
├─ pipeline_config.*.json # Configs for dev/test/prod
├─ Dockerfile            # Containerized execution
├─ requirements.txt      # Python dependencies
├─ playbook.md           # Ops playbook
└─ README.md             # This file
```

---

## ⚙️ How It Works

1. **Document Download**

   * Pipelines (`download_documents.py`, `download_sops.py`) fetch raw files from external repositories.
   * Files are stored in S3 and tracked via metadata in DynamoDB.

2. **Metadata Enrichment**

   * `document_metadata.py` + `language.py` classify documents by site, language, and version.
   * Withdrawn/active states are reconciled (`withdrawn_documents.py`).

3. **Generative AI Layer**

   * `generate_questions.py` invokes AWS Bedrock LLMs.
   * Extracted Q&A pairs enrich the knowledge base.

4. **Knowledge Assistant**

   * “SOP Assistant” chatbot consumes enriched files.
   * Supports **site-specific natural language queries**.

5. **Ops & Notifications**

   * `logging.py` provides structured observability.
   * `decorators.py` ensures retries for transient failures.
   * `email.py` + `aws_sns.py` send alerts/reports to operators.

---

## Setup & Installation

### Prerequisites

* Python 3.10+
* AWS account with IAM permissions for S3, DynamoDB, Secrets Manager, SNS
* Docker (optional, for containerized runs)

### Installation

```bash
git clone https://github.com/<your-username>/file-ingestion.git
cd file-ingestion
pip install -r requirements.txt
```

### Configuration

Set environment variables in `.env`:

```bash
AWS_REGION=<region>
AWS_PROFILE=<profile>
STAGE=dev|test|prod
```

Pipeline configs live in:

* `pipeline_config.dev.json`
* `pipeline_config.test.json`
* `pipeline_config.prod.json`

### Run Locally

```bash
python main.py 
```

* Executes specific pipelines depending on weekday phase (e.g., ingestion Mon-Wed, QA Thu-Fri).

## Testing

Unit tests are included for connectors, models, and pipelines. Run with:

```bash
pytest tests/
```
