# Pipeline Utility Scripts

This directory contains utility and operations scripts for the Data Commons import and ingestion pipeline.

---

## Available Scripts

| Script | Description |
| :--- | :--- |
| **[`run_ingestion.sh`](run_ingestion.sh)** | Triggers the Cloud Spanner ingestion workflow for an import via `ingestion-helper` (supports `--dry-run`). |
| **[`update_lock.sh`](update_lock.sh)** | Releases or force-acquires the global Spanner ingestion lock for a workflow execution. |
| **[`get_version.sh`](get_version.sh)** | Fetches the last `SUCCESS` version for a given import via `ingestion-helper`. |

---

## `run_ingestion.sh`

Triggers the `spanner-ingestion-workflow` for an import through the `ingestion-helper` service (`POST /imports/ingest`). The import is sent with `forceIngestion: true`, so it is ingested even if that version already succeeded. The script does not update the import's version in Spanner.

### Syntax

```bash
./pipeline/scripts/run_ingestion.sh <importName> <env> <latestVersion> [--dry-run]
```

### Arguments

- **`importName`** *(required)*: The import name (e.g. `USFed_ConstantMaturityRates_Test` or `scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test`). If a `:` prefix is present, only the part after `:` is sent.
- **`env`** *(required)*: Target environment (`staging` or `prod`).
- **`latestVersion`** *(required)*: Full GCS path (with wildcard) to the graph files to ingest (e.g. `'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*'`).
- **`--dry-run`** *(optional)*: The helper resolves the import list and returns it without starting the workflow (`status: SKIPPED`).

### Example

```bash
./pipeline/scripts/run_ingestion.sh \
  USFed_ConstantMaturityRates_Test \
  staging \
  'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*' \
  --dry-run
```

On success without `--dry-run`, the response has `status: SUBMITTED` and the workflow `executionName`.

---

## `update_lock.sh`

Updates the global Spanner ingestion lock (`IngestionLock`) through the `ingestion-helper` service.

### Syntax

```bash
./pipeline/scripts/update_lock.sh <release|acquire> <workflowId> <env>
```

### Arguments

- **`mode`** *(required)*:
  - `release`: Releases the lock held by `workflowId` (`POST /database/lock/release`). Fails if the lock is not held by that workflow.
  - `acquire`: Force-assigns the lock to `workflowId`, even if another workflow holds it (`POST /database/lock/acquire` with `force: true`).
- **`workflowId`** *(required)*: The Cloud Workflows execution ID.
- **`env`** *(required)*: Target environment (`staging` or `prod`).

### When to use

- A cancelled or crashed workflow never reaches its release step, so it keeps the lock. Use `release` to free it.
- Waiting workflows poll for the lock in no particular order, so after a `release` any of them may grab it. To hand the lock to a specific execution (e.g. a rerun after a failure), use `acquire` with that execution's ID instead; it picks up the lock on its next poll.

> [!CAUTION]
> `acquire` takes the lock away from whichever workflow holds it, including a running one. Make sure the current owner is no longer running first.

### Examples

```bash
# Free the lock held by a cancelled execution.
./pipeline/scripts/update_lock.sh release 12345678-1234-1234-1234-123456789abc staging

# Hand the lock to a waiting rerun execution.
./pipeline/scripts/update_lock.sh acquire 12345678-1234-1234-1234-123456789abc prod
```

---

## `get_version.sh`

Fetches the last `SUCCESS` version for an import from Spanner via the `ingestion-helper` service (`GET /imports/version`).

### Syntax

```bash
./pipeline/scripts/get_version.sh <importName> <env>
```

### Arguments

- **`importName`** *(required)*: The import name (e.g. `USFed_ConstantMaturityRates_Test` or `scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test`).
- **`env`** *(required)*: Target environment (`staging` or `prod`).

### Example

```bash
./pipeline/scripts/get_version.sh \
  USFed_ConstantMaturityRates_Test \
  staging
```
