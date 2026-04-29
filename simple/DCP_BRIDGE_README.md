# Data Commons Platform (DCP) Bridge for Custom Data

This document describes the "DCP Bridge" mode added to the `simple` ingestion tool. This mode bridges the gap between the easy-to-use Custom Data Commons tools (which handle CSV resolution and melting) and the high-scale Production Platform (which loads data into Spanner via Dataflow).

## Overview

The standard Custom DC workflow processes CSV and MCF files and loads them into a local SQLite or Cloud SQL database. The Production workflow, however, expects graph files (MCF or JSON-LD) to load into Spanner using Dataflow.

The **DCP Bridge** mode (`dcpbridge`) connects these two workflows:
1.  It uses the `simple` tool's `ObservationsImporter` to load wide CSVs and MCF files into a temporary SQLite database, handling entity resolution and data sanitization automatically based on your `config.json`.
2.  It extracts the resolved data from SQLite and exports it as **sharded JSON-LD files**.
3.  If the output directory is on Google Cloud Storage (`gs://`), it can **automatically trigger** the Dataflow ingestion workflow to load the data into Spanner.

## Usage

To run the bridge, you should execute the `simple` tool's `main.py` as a module from the `simple` directory to avoid package shadowing issues.

```bash
cd simple
PYTHONPATH=. python3 -m stats.main \
  --input_dir=/path/to/input_dir/ \
  --output_dir=/path/to/output_dir/ \
  --mode=dcpbridge
```

*   **`--input_dir`**: Directory containing your `config.json`, CSV data files, and MCF schema files.
*   **`--output_dir`**: Directory where the JSON-LD shards will be written. Can be a local path or a GCS path (e.g., `gs://my-bucket/path/`).
*   **`--mode=dcpbridge`**: Activates the bridge mode.

## Sharding

To handle massive datasets (gigabytes of data) without crashing workers with Out-of-Memory errors, the exporter automatically splits the data into shards of **10,000 rows** each:
*   **Schema Shards**: Schema triples from your MCF files are written first (e.g., `output-00000.jsonld`).
*   **Observation Shards**: Observations from your CSV files are written in subsequent shards (e.g., `output-00001.jsonld`, `output-00002.jsonld`).

## Automation & Environment Variables

If you specify a GCS path for `--output_dir`, the script will attempt to automatically trigger the Dataflow ingestion workflow using the `gcloud` CLI.

To support deployment via infrastructure tools like **Terraform**, the trigger parameters can be injected via environment variables. If not provided, they fall back to default test values:

| Environment Variable | Description | Default Fallback |
| :--- | :--- | :--- |
| `SPANNER_INSTANCE_ID` | Target Spanner Instance | `gabe-test-dcp-instance` |
| `SPANNER_DATABASE_ID` | Target Spanner Database | `gabe-test-dcp-db-v2` |
| `WORKFLOW_NAME` | Cloud Workflow name | `gabe-test-ingestion-orchestrator` |
| `PROJECT_ID` | GCP Project ID | `datcom-website-dev` |
| `WORKFLOW_LOCATION` | Workflow Location | `us-central1` |
| `TEMP_LOCATION` | GCS Temp Location | `gs://.../temp` |
| `REGION` | Dataflow Region | `us-central1` |

## Dependencies

This mode introduces dependencies on `pyld` and `rdflib`. Ensure they are installed in your environment:

```bash
pip install pyld rdflib
```
