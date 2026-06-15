# Data Commons Import Workflow

The `datacommons-import-workflow` package contains orchestration configurations, Google Cloud Workflows definitions, and integration tests for the Cloud Spanner data ingestion pipeline. It handles the deployment of the pipeline services and executes end-to-end integration tests to verify data ingestion from Cloud Storage to Cloud Spanner.

---

## Continuous Integration & Deployment (CI/CD)

Pushes to the `master` branch automatically trigger the main parent orchestrator build defined in **[cloudbuild.yaml](cloudbuild.yaml)**.

The automated pipeline executes the following steps:
1.  **Build:** Builds three container images in parallel:
    - The `ingestion-helper` service.
    - The `import-helper` service.
    - The Dataflow Flex Template used for Spanner data ingestion.
2.  **Staging Deployment:** Deploys the services and workflows to the Staging environment (`datcom-ci` project) using staging configurations.
3.  **Integration Testing:** Runs the end-to-end integration test suite (`spanner_ingestion_test.py`) against the Staging environment inside a `uv`-enabled container.
4.  **Production Deployment:** Upon successful completion of all integration tests, promotes the container images and deploys the updated services and workflows to the Production environment (`datcom-import-automation-prod` project).

---

## Manual Testing in Staging (Sandbox)

To test changes end-to-end without overwriting the shared `:latest` image tag in the Staging registry, developers should build a custom-tagged image and deploy it to the Staging environment (`datcom-ci` project).

### Step 1: Build and Push a Custom-Tagged Image
Run this command from the `pipeline/workflow/ingestion-helper/` directory to build your local code changes and push them to the Staging registry with a unique developer-specific tag (e.g., `dev-test-1`):

```bash
# Inside pipeline/workflow/ingestion-helper/
gcloud builds submit . \
  --config=cloudbuild.yaml \
  --project=datcom-ci \
  --substitutions=_VERSION=dev-<your-name>
```

### Step 2: Deploy the Custom-Tagged Image to Staging
Run this command from the parent `pipeline/workflow/` directory. It uses **[deploy-services.yaml](deploy-services.yaml)** (the child deployment configuration) to deploy your custom-tagged image to the Staging Cloud Run service and update the workflows, targeting the staging project (`datcom-ci`):

```bash
# Inside pipeline/workflow/
gcloud builds submit . \
  --config=deploy-services.yaml \
  --project=datcom-ci \
  --substitutions=\
_PROJECT_ID=datcom-ci,\
_SPANNER_PROJECT_ID=datcom-ci,\
_SPANNER_INSTANCE_ID=datcom-spanner-test,\
_SPANNER_DATABASE_ID=dc-test-db,\
_SPANNER_GRAPH_DATABASE_ID=dc-test-db,\
_GCS_BUCKET_ID=datcom-ci-test,\
_LOCATION=us-central1,\
_GCS_MOUNT_BUCKET=datcom-ci-test,\
_BQ_DATASET_ID=datacommons,\
_PROJECT_NUMBER=879489846695,\
_BQ_SPANNER_CONN_ID=projects/datcom-ci/locations/us-central1/connections/bq_spanner_conn_test,\
_VERSION=dev-<your-name>
```

### Step 3: Run the Integration Tests
Once the staging deployment completes, execute the integration test runner from your local terminal. The runner uses **`uv`** to manage its execution environment:

```bash
# Inside pipeline/workflow/
uv run python spanner_ingestion_test.py
```

#### What the test runner does:
1.  **Environment Setup:** `uv` automatically creates a virtual environment and installs the required dependencies (`google-cloud-spanner`, `google-cloud-workflows`, and `absl-py`) defined in `pyproject.toml`.
2.  **Database Cleanup:** Deletes existing test import records in the staging Spanner database.
3.  **Workflow Triggering:** Connects to GCP and triggers the staging Cloud Workflows (`import-automation-workflow` and `spanner-ingestion-workflow`).
4.  **Verification:** Polls the workflows for completion and queries the staging Spanner database to verify the ingested data is marked as `SUCCESS`.
