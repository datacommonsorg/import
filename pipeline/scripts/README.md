# Pipeline Utility Scripts

This directory contains utility and operations scripts for the Data Commons import and ingestion pipeline.

---

## Available Scripts

| Script | Description |
| :--- | :--- |
| **[`run_ingestion.sh`](run_ingestion.sh)** | Updates the import version via `import-helper` and triggers the Cloud Spanner ingestion workflow (`spanner-ingestion-workflow`) for a given import. |
| **[`release_lock.sh`](release_lock.sh)** | Releases the global Spanner ingestion lock (`global_ingestion_lock`) held by a specific workflow execution via `ingestion-helper`. |
| **[`freeze_db.sh`](freeze_db.sh)** | Freezes or unfreezes Mixer Spanner reads at a given timestamp by acquiring/releasing `IngestionLock` and inserting/removing a dummy row in `IngestionHistory`. |

---

## `run_ingestion.sh`

Updates the import version and version history via `import-helper` (`POST /imports/version`) and triggers `spanner-ingestion-workflow` (or `spanner-ingestion-workflow-staging`) in Google Cloud Workflows to ingest an import's data into Cloud Spanner.

### Syntax

```bash
./pipeline/scripts/run_ingestion.sh <importName> <env: staging|prod> <latestVersion: full GCS path with wildcard>
```

### Arguments

- **`importName`** *(required)*: The full import name including script path prefix (e.g. `scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test`).
- **`env`** *(required)*: Target environment (`staging` or `prod`).
- **`latestVersion`** *(required)*: Full GCS glob path to the `.mcf` graph files (or `STAGING` to use the latest staging version in Spanner).

### Example

```bash
./pipeline/scripts/run_ingestion.sh \
  scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test \
  staging \
  'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*'
```

---

## `release_lock.sh`

Releases the global Spanner ingestion lock (`global_ingestion_lock`) held by a specific workflow execution ID via `ingestion-helper` (`POST /database/lock/release`).

### Syntax

```bash
./pipeline/scripts/release_lock.sh <workflowId> <env: staging|prod>
```

### Arguments

- **`workflowId`** *(required)*: The Cloud Workflow execution ID currently holding the lock.
- **`env`** *(required)*: Target environment (`staging` or `prod`).

### Example

```bash
./pipeline/scripts/release_lock.sh 12345678-1234-1234-1234-123456789abc staging
```

---

## `freeze_db.sh`

Freezes or unfreezes Mixer Spanner reads at a point-in-time snapshot by:
1. Acquiring or releasing `global_ingestion_lock` in `IngestionLock`.
2. Inserting or removing a dummy `RUNNING` row (`manual-read-freeze`) in `IngestionHistory`.

### Syntax

```bash
./pipeline/scripts/freeze_db.sh freeze <env: staging|prod> <timestamp>
./pipeline/scripts/freeze_db.sh unfreeze <env: staging|prod>
./pipeline/scripts/freeze_db.sh get <env: staging|prod>
```

### Arguments

- **`action`** *(required)*: `freeze`, `unfreeze`, or `get`.
- **`env`** *(required)*: Target environment (`staging` or `prod`).
- **`timestamp`** *(required for `freeze`)*: Point-in-time snapshot timestamp (accepts ISO-8601 UTC such as `"2026-09-23T08:30:00Z"` or timezone strings such as `"2026-09-23 14:00:00 IST"`).

### Examples

```bash
# Get current stale read timestamp in prod
./pipeline/scripts/freeze_db.sh get prod

# Freeze prod reads at 2:00 PM IST on Sept 23, 2026
./pipeline/scripts/freeze_db.sh freeze prod "2026-09-23 14:00:00 IST"

# Unfreeze prod reads and release the lock
./pipeline/scripts/freeze_db.sh unfreeze prod
```
