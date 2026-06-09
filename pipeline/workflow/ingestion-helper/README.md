# Data Commons Ingestion Helper

Provides orchestration, database synchronization, lock management, and metadata updates for the Data Commons Spanner ingestion workflow.

The service is implemented as a FastAPI microservice designed to run on Google Cloud Run.

---

## Architecture & Code Layout

The codebase is organized into a layered architecture:

```
ingestion-helper/
├── app.py                     # Main application entry point; registers routers
├── config.py                  # Centralized configuration (env vars)
├── models.py                  # Shared base Pydantic models & Enums
├── dependencies.py            # FastAPI dependency injections (Spanner & GCS lifecycles)
├── __init__.py                # Top-level package init; defines __version__
│
├── clients/                   # Layer 1: External GCP Resource Interfaces
│   ├── __init__.py            # Exposes SpannerClient & StorageClient
│   ├── spanner.py             # Cloud Spanner driver & mutations
│   ├── storage.py             # Google Cloud Storage driver
│   ├── schema.sql             # Spanner DDL Schema
│   └── spanner_test.py        # Client-level unit tests
│
├── utils/                     # Layer 2: Core Processing & Calculations
│   ├── __init__.py
│   ├── aggregation.py         # BigQuery async aggregation polling logic
│   ├── embeddings.py          # Vertex AI text embedding generation
│   ├── imports.py             # General metadata metrics extractor
│   └── embeddings_test.py     # Embedding calculation unit tests
│
└── routes/                    # Layer 3: REST API Handlers
    ├── models.py              # Route-shared base models (BaseResponse)
    ├── imports.py             # /imports/*
    ├── database.py            # /database/*
    ├── embeddings.py          # /embeddings/*
    ├── aggregation.py         # /aggregation/*
    └── cache.py               # /cache/*
```

---

## API Documentation

The microservice exposes RESTful endpoints. All responses return a structured JSON payload.

Interactive OpenAPI documentation is automatically served at `/docs` (Swagger UI) and `/redoc` when running the service.

### Summary of Routes

| Endpoint | Method | Request Body | Response Model | Description |
| :--- | :--- | :--- | :--- | :--- |
| `/database/initialize` | `POST` | *None* | `BaseResponse` | Boots database DDL schemas & proto descriptors. |
| `/database/seed` | `POST` | *None* | `BaseResponse` | Seeds baseline empty nodes required by schema. |
| `/database/lock/acquire` | `POST` | `LockAcquireRequest` | `BaseResponse` | Attempts to acquire the global Spanner ingestion lock. |
| `/database/lock/release` | `POST` | `LockReleaseRequest` | `BaseResponse` | Releases the global ingestion lock. |
| `/embeddings/ingest` | `POST` | `EmbeddingIngestionRequest` | `EmbeddingIngestionResponse` | Generates text embeddings for updated Spanner nodes. |
| `/imports/info` | `POST` | `ImportInfoRequest` | `List[ImportInfoItem]` | Returns all imports in `STAGING` state ready to ingest. |
| `/imports/status` | `POST` | `UpdateImportStatusRequest` | `BaseResponse` | Updates import-specific status and refresh windows. |
| `/imports/version` | `POST` | `UpdateImportVersionRequest` | `BaseResponse` | Updates import version and records audit logs. |
| `/imports/ingestion-status` | `POST` | `UpdateIngestionStatusRequest` | `BaseResponse` | Records final pipeline statuses and Dataflow metrics. |
| `/aggregation/run` | `POST` | `AggregationRequest` | `AggregationResponse` | Submits BigQuery aggregation queries asynchronously. |
| `/aggregation/status` | `POST` | `AggregationStatusRequest` | `AggregationStatusResponse` | Polls active BigQuery aggregation job statuses. |
| `/cache/clear` | `POST` | *None* | `BaseResponse` | Flushes the Redis cache (if configured). |

---

## Environment Configuration

All application configurations are centralized inside **[config.py](config.py)** and loaded from system environment variables. 

### Configuration Options

| Variable | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `PROJECT_ID` | `str` | *None* | GCP Project ID. |
| `SPANNER_PROJECT_ID` | `str` | *None* | GCP Project ID hosting the target Spanner instance. |
| `SPANNER_INSTANCE_ID` | `str` | *None* | Cloud Spanner Instance ID. |
| `SPANNER_DATABASE_ID` | `str` | *None* | Cloud Spanner Metadata Database ID (tracks jobs). |
| `SPANNER_GRAPH_DATABASE_ID` | `str` | *None* | Cloud Spanner Graph Database ID (stores triples). |
| `BQ_SPANNER_CONN_ID` | `str` | *None* | BigQuery connection ID used to query Spanner in aggregations. |
| `GCS_BUCKET_ID` | `str` | *None* | GCS Bucket ID used for staging and versioning imports. |
| `LOCATION` / `REGION` | `str` | `us-central1` | GCP region where resources are deployed. |
| `ENABLE_EMBEDDINGS` | `bool` | `false` | Enables/disables Vertex AI embedding generation. |
| `IS_BASE_DC` | `bool` | `true` | Identifies if this is a Base Data Commons instance. |
| `TIMEOUT` | `int` | `1700` | Query execution timeout in seconds for Spanner transactions. |
| `EMBEDDING_MODEL_ID` | `str` | `text-embedding-005` | Vertex AI Text Embedding model version. |
| `REDIS_HOST` | `str` | *None* | Redis host address (triggers cache clears). |
| `REDIS_PORT` | `str` | `6379` | Redis port. |
| `GCS_OUTPUT_PREFIX` | `str` | `""` | Optional folder prefix for GCS writes. |

---

## Local Development

This project uses **`uv`** for dependency locking, virtual environment management, and packaging.

### Prerequisites
Make sure you have `uv` installed:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### 1. Run the Server Locally
To start the FastAPI development server with hot-reloading:
```bash
uv run uvicorn app:app --host 127.0.0.1 --port 8080 --reload
```
This will start the service locally on `http://127.0.0.1:8080`.

### 2. Run the Unit Test Suite
Unit tests are located next to the source files they verify. Run them using:
```bash
uv run pytest
```
*Note: Pytest is configured in `pyproject.toml` to map the root project directory into the Python path.*

### 3. Build & Package Natively
The package utilizes the **Hatchling** build backend. The version is resolved dynamically from `__init__.py`. 

To build source distributions and wheels:
```bash
uv build
```
The build system is configured to include the Spanner DDL schema (`clients/schema.sql`) inside the built wheel.

---

## Containerization & Deployment

The service is packaged as a Docker container. 

The `Dockerfile` utilizes multi-stage building and layer caching:
1. It copies `pyproject.toml` and performs `uv sync --no-dev --no-install-project` to cache dependencies before copying the application code.
2. It compiles the required Google Cloud Spanner protobuf descriptor sets during build time:
   ```dockerfile
   RUN protoc --include_imports --descriptor_set_out=clients/storage.pb storage.proto
   ```
3. It starts the web server inside the container using Uvicorn:
   ```dockerfile
   CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]
   ```
