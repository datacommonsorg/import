# Pipeline Utility Scripts

This directory contains utility and operations scripts for the Data Commons import and ingestion pipeline.

---

## Available Scripts

| Script | Description |
| :--- | :--- |
| **[`run_ingestion.sh`](run_ingestion.sh)** | Manually triggers the Cloud Spanner ingestion workflow for a given import. |

---

## `run_ingestion.sh`

Manually triggers the `spanner-ingestion-workflow` in Google Cloud Workflows to ingest an import's data into Cloud Spanner.

### Syntax

```bash
./pipeline/scripts/run_ingestion.sh <importName> <env> [latestVersion]
```

### Arguments

- **`importName`** *(required)*: The full import name including script path prefix (e.g. `scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test`).
- **`env`** *(required)*: Target environment (`staging` or `prod`).
- **`latestVersion`** *(optional)*: Full GCS path to the graph data / version directory (e.g. `gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/2026_08_07_17_19_57`). If omitted, the workflow queries Spanner for the latest staging version.

### Example

```bash
./pipeline/scripts/run_ingestion.sh \
  scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test \
  staging
```
