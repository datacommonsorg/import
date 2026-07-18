# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime
import os

from fastapi.testclient import TestClient
from app import app
from dependencies import get_spanner_client, get_storage_client
import config

client = TestClient(app)


class TestMain(unittest.TestCase):

    def setUp(self):
        # Clear overrides before each test
        app.dependency_overrides.clear()

    def tearDown(self):
        # Clear overrides after each test
        app.dependency_overrides.clear()

    def test_embedding_ingestion_success(self):
        # Mock SpannerClient instance and its database
        mock_spanner_client = MagicMock()
        mock_database = MagicMock()
        mock_spanner_client.database = mock_database
        mock_spanner_client.embedding_table = "NodeEmbedding"
        mock_spanner_client.embedding_index = "NodeEmbeddingIndex"
        mock_spanner_client.embedding_label_index = "NodeEmbeddingLabelIndex"

        # Mock snapshot and execute_sql
        mock_snapshot = MagicMock()
        mock_database.snapshot.return_value.__enter__.return_value = mock_snapshot

        class MockField:
            def __init__(self, name):
                self.name = name

        class MockResults:
            def __init__(self, rows, field_names):
                self.rows = rows
                self.fields = [MockField(name) for name in field_names]

            def __iter__(self):
                return iter(self.rows)

        expected_timestamp = datetime(2026, 4, 20, 12, 0, 0)

        # Mock side effect for execute_sql
        # First call: get_latest_lock_timestamp
        # Second call: get_updated_nodes
        mock_snapshot.execute_sql.side_effect = [
            [(expected_timestamp,)],
            MockResults(
                rows=[
                    ("dc/1", "Node 1", ["Topic"]),
                    ("dc/2", None, ["Topic"]),
                    ("dc/3", "SV 1", ["StatisticalVariable"])
                ],
                field_names=["subject_id", "name", "types"]
            )
        ]

        # Mock transaction for generate_embeddings_partitioned
        transactions = []
        def run_in_transaction_side_effect(func):
            mock_transaction = MagicMock()
            mock_transaction.execute_update.return_value = 2
            transactions.append(mock_transaction)
            return func(mock_transaction)

        mock_database.run_in_transaction.side_effect = run_in_transaction_side_effect

        # Register dependency override
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        # Call the FastAPI endpoint
        response = client.post("/embeddings/ingest", json={"enableEmbeddings": True})

        # Assertions
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        self.assertEqual(response.json()["affected_rows"], 2)

        # Assertions for get_latest_lock_timestamp and get_updated_nodes
        self.assertEqual(mock_database.snapshot.call_count, 2)
        call_args_list = mock_snapshot.execute_sql.call_args_list
        self.assertEqual(len(call_args_list), 2)

        # Check first call (lock timestamp)
        self.assertIn("IngestionLock", call_args_list[0].args[0])

        # Check second call (updated nodes)
        args2, kwargs2 = call_args_list[1]
        self.assertIn("Node", args2[0])
        self.assertEqual(kwargs2["params"]["timestamp"], expected_timestamp)
        self.assertEqual(kwargs2["params"]["node_types"], ["StatisticalVariable", "Topic"])

        # Assertions for generate_embeddings_partitioned
        mock_database.run_in_transaction.assert_called_once()
        self.assertEqual(len(transactions), 1)
        transactions[0].execute_update.assert_called_once()
        args_tx, kwargs_tx = transactions[0].execute_update.call_args
        self.assertIn("INSERT OR UPDATE INTO NodeEmbedding", args_tx[0])

        # Verify data passed to generate_embeddings_partitioned reached execute_update
        batch = kwargs_tx["params"]["nodes"]
        self.assertEqual(len(batch), 2)
        self.assertEqual(batch[0][0], "dc/1")
        self.assertEqual(batch[1][0], "dc/3")
        self.assertEqual(batch[0][1], '{"title": "dc/1", "name": "Node 1"}')

    def test_seed_database_success(self):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        # Call the FastAPI endpoint
        response = client.post("/database/seed")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_spanner_client.seed_database.assert_called_once()

    def test_update_import_status_success(self):
        mock_spanner_client = MagicMock()
        mock_storage_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client
        app.dependency_overrides[get_storage_client] = lambda: mock_storage_client

        payload = {
            "imports": [
                {
                    "importName": "import1",
                    "status": "STAGING",
                    "latestVersion": "gs://bucket/import1/version1.csv",
                    "graphPath": "gs://bucket/import1/graph/"
                },
                {
                    "importName": "import2",
                    "status": "STAGING",
                    "latestVersion": "gs://bucket/import2/version2.csv",
                    "graphPath": "gs://bucket/import2/graph/"
                }
            ],
            "jobId": "job123",
            "executionTime": 100,
            "dataVolume": 200,
            "nextRefresh": "2026-07-01T00:00:00Z"
        }

        response = client.post("/imports/status", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        
        # Verify storage was called for both imports
        self.assertEqual(mock_storage_client.update_version_file.call_count, 4) # 2 per import
        self.assertEqual(mock_storage_client.update_provenance_file.call_count, 2)
        self.assertEqual(mock_storage_client.update_import_summary.call_count, 2)
        
        # Verify spanner was called for both imports
        self.assertEqual(mock_spanner_client.update_version_history.call_count, 2)
        self.assertEqual(mock_spanner_client.update_import_status.call_count, 2)

    def test_update_import_version_success(self):
        mock_spanner_client = MagicMock()
        mock_storage_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client
        app.dependency_overrides[get_storage_client] = lambda: mock_storage_client

        # Mock storage calls
        mock_storage_client.get_staging_version.side_effect = lambda name: f"ver_{name}"
        mock_storage_client.get_import_summary.side_effect = lambda name, version: {
            "importName": name,
            "status": "STAGING",
            "latestVersion": f"gs://bucket/{name}/{version}.csv"
        }

        payload = {
            "imports": ["import1", "import2"],
            "version": "STAGING",
            "comment": "release-comment",
            "override": False
        }

        response = client.post("/imports/version", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        self.assertIn("Import: import1 Version: ver_import1 Status: STAGING", response.json()["message"])
        self.assertIn("Import: import2 Version: ver_import2 Status: STAGING", response.json()["message"])

        # Verify storage was called
        mock_storage_client.get_staging_version.assert_any_call("import1")
        mock_storage_client.get_staging_version.assert_any_call("import2")
        self.assertEqual(mock_storage_client.update_provenance_file.call_count, 2)
        self.assertEqual(mock_storage_client.update_version_file.call_count, 2)

        # Verify spanner was called
        self.assertEqual(mock_spanner_client.update_version_history.call_count, 2)
        self.assertEqual(mock_spanner_client.update_import_status.call_count, 2)

    @patch('routes.imports.import_utils.get_caller_identity')
    def test_update_import_version_override_success(self, mock_get_caller_identity):
        mock_get_caller_identity.return_value = "test-caller"
        mock_spanner_client = MagicMock()
        mock_storage_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client
        app.dependency_overrides[get_storage_client] = lambda: mock_storage_client

        # Mock storage calls
        mock_storage_client.get_staging_version.side_effect = lambda name: f"ver_{name}"
        mock_storage_client.get_import_summary.side_effect = lambda name, version: {
            "importName": name,
            "status": "NOT_STAGING",  # Starts as NOT_STAGING, will be overridden to STAGING
            "latestVersion": f"gs://bucket/{name}/{version}.csv"
        }

        payload = {
            "imports": ["import1", "import2"],
            "version": "STAGING",
            "comment": "release-comment",
            "override": True
        }

        response = client.post("/imports/version", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        self.assertIn("Import: import1 Version: ver_import1 Status: STAGING", response.json()["message"])
        self.assertIn("Import: import2 Version: ver_import2 Status: STAGING", response.json()["message"])

        # Verify storage was called
        mock_storage_client.get_staging_version.assert_any_call("import1")
        mock_storage_client.get_staging_version.assert_any_call("import2")
        self.assertEqual(mock_storage_client.update_provenance_file.call_count, 2)
        self.assertEqual(mock_storage_client.update_version_file.call_count, 2)

        # Verify spanner was called with the overridden comment containing caller
        mock_spanner_client.update_version_history.assert_any_call(
            "import1", "ver_import1", "version-override:test-caller release-comment", workflow_id=None, status="STAGING"
        )
        mock_spanner_client.update_version_history.assert_any_call(
            "import2", "ver_import2", "version-override:test-caller release-comment", workflow_id=None, status="STAGING"
        )
        self.assertEqual(mock_spanner_client.update_version_history.call_count, 2)
        self.assertEqual(mock_spanner_client.update_import_status.call_count, 2)
        
        # Verify get_caller_identity was called exactly once outside of the loop
        mock_get_caller_identity.assert_called_once()

    @patch("routes.ingestion.requests.post")
    def test_start_ingestion_success(self, mock_requests_post):
        mock_storage_client = MagicMock()
        mock_storage_client.check_bucket_exists.return_value = True

        app.dependency_overrides[get_storage_client] = lambda: mock_storage_client

        mock_val_resp = MagicMock()
        mock_val_resp.status_code = 200
        mock_requests_post.return_value = mock_val_resp

        os.environ["DC_API_KEY"] = "mock-valid-key"
        os.environ["STORAGE_EMULATOR_HOST"] = "localhost:9099"

        try:
            payload = {
                "jobName": "projects/test-project/locations/us-central1/jobs/test-job",
                "gcsBucket": "test-bucket",
                "inputDirectory": "ingestion/input"
            }
            response = client.post("/ingestion/start", json=payload)

            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "SUBMITTED")
            self.assertEqual(response.json()["operationName"], "projects/test-project/locations/us-central1/operations/mock-operation")

            mock_storage_client.check_bucket_exists.assert_called_once_with("test-bucket")

            mock_requests_post.assert_called_once_with(
                "https://api.datacommons.org/v2/node",
                json={"nodes": ["country/USA"], "property": "->name"},
                headers={"x-api-key": "mock-valid-key"},
                timeout=5
            )
        finally:
            os.environ.pop("DC_API_KEY", None)
            os.environ.pop("STORAGE_EMULATOR_HOST", None)

    @patch("routes.ingestion.requests.post")
    def test_start_ingestion_api_key_403_failure(self, mock_requests_post):
        mock_storage_client = MagicMock()
        mock_storage_client.check_bucket_exists.return_value = True

        app.dependency_overrides[get_storage_client] = lambda: mock_storage_client

        mock_val_resp = MagicMock()
        mock_val_resp.status_code = 403
        mock_requests_post.return_value = mock_val_resp

        os.environ["DC_API_KEY"] = "mock-invalid-key"
        os.environ["STORAGE_EMULATOR_HOST"] = "localhost:9099"

        try:
            payload = {
                "jobName": "projects/test-project/locations/us-central1/jobs/test-job",
                "gcsBucket": "test-bucket",
                "inputDirectory": "ingestion/input"
            }
            response = client.post("/ingestion/start", json=payload)
            self.assertEqual(response.status_code, 400)
            self.assertIn("rejected by api.datacommons.org (HTTP 403)", response.json()["detail"])
        finally:
            os.environ.pop("DC_API_KEY", None)
            os.environ.pop("STORAGE_EMULATOR_HOST", None)

    def test_get_job_config_success(self):
        # Set environment variables for the test
        os.environ["STORAGE_EMULATOR_HOST"] = "localhost:9099" # trigger emulator stubbed code path
        
        try:
            # Call endpoint with explicit jobName
            payload = {
                "jobName": "projects/test-project/locations/us-central1/jobs/test-job"
            }
            response = client.post("/ingestion/config", json=payload)
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "SUCCESS")
            self.assertEqual(response.json()["config"][0]["name"], "DATA_RUN_MODE")

            # Call endpoint without jobName, using env var fallback
            os.environ["INGESTION_JOB_NAME"] = "projects/test-project/locations/us-central1/jobs/test-job-env"
            response = client.post("/ingestion/config", json={})
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "SUCCESS")
            self.assertEqual(response.json()["config"][0]["name"], "DATA_RUN_MODE")
        finally:
            os.environ.pop("STORAGE_EMULATOR_HOST", None)
            os.environ.pop("INGESTION_JOB_NAME", None)


    @patch('routes.imports.import_utils.get_ingestion_metrics')
    def test_update_ingestion_status(self, mock_get_ingestion_metrics):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        mock_metrics = {
            'execution_time': 120,
            'node_count': 1000,
            'edge_count': 2000,
            'obs_count': 500,
            'ts_count': 300,
        }
        mock_get_ingestion_metrics.return_value = mock_metrics

        payload = {
            "importList": [{"importName": "import1"}],
            "workflowId": "wf-123",
            "status": "SUCCESS",
            "jobId": "job-456"
        }

        response = client.post("/imports/ingestion-status", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_spanner_client.update_ingestion_status.assert_called_once_with(
            ["import1"], "wf-123", "SUCCESS"
        )
        mock_get_ingestion_metrics.assert_called_once_with(
            config.PROJECT_ID, config.LOCATION, "job-456"
        )
        mock_spanner_client.update_import_version_history.assert_called_once_with(
            [{"importName": "import1", "latestVersion": None}], "wf-123", status="SUCCESS", metrics=mock_metrics
        )

    @patch('routes.imports.import_utils.get_ingestion_metrics')
    def test_update_ingestion_history_pending(self, mock_get_ingestion_metrics):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        payload = {
            "workflowId": "wf-123",
            "status": "PENDING",
            "stage": "dataflow",
            "importList": [{"importName": "import1"}]
        }

        response = client.post("/imports/ingestion-history", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_spanner_client.update_ingestion_history.assert_called_once_with(
            workflow_id="wf-123",
            status="PENDING",
            stage="dataflow",
            job_id=None,
            ingested_imports=["import1"],
            metrics=None
        )
        mock_get_ingestion_metrics.assert_not_called()

    @patch('routes.imports.import_utils.get_ingestion_metrics')
    def test_update_ingestion_history_success(self, mock_get_ingestion_metrics):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        mock_metrics = {
            'execution_time': 120,
            'node_count': 1000,
            'edge_count': 2000,
            'obs_count': 500,
            'ts_count': 300,
        }
        mock_get_ingestion_metrics.return_value = mock_metrics

        payload = {
            "workflowId": "wf-123",
            "status": "SUCCESS",
            "stage": "dataflow",
            "jobId": "job-456",
            "importList": [{"importName": "import1"}]
        }

        response = client.post("/imports/ingestion-history", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_get_ingestion_metrics.assert_called_once_with(
            config.PROJECT_ID, config.LOCATION, "job-456"
        )
        mock_spanner_client.update_ingestion_history.assert_called_once_with(
            workflow_id="wf-123",
            status="SUCCESS",
            stage="dataflow",
            job_id="job-456",
            ingested_imports=["import1"],
            metrics=mock_metrics
        )

    @patch('routes.imports.import_utils.get_ingestion_metrics')
    def test_update_ingestion_history_failure(self, mock_get_ingestion_metrics):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        mock_metrics = {
            'execution_time': 120,
            'node_count': 1000,
            'edge_count': 2000,
            'obs_count': 500,
            'ts_count': 300,
        }
        mock_get_ingestion_metrics.return_value = mock_metrics

        payload = {
            "workflowId": "wf-123",
            "status": "FAILURE",
            "stage": "dataflow",
            "jobId": "job-456",
            "importList": [{"importName": "import1"}]
        }

        response = client.post("/imports/ingestion-history", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_get_ingestion_metrics.assert_not_called()
        mock_spanner_client.update_ingestion_history.assert_called_once_with(
            workflow_id="wf-123",
            status="FAILURE",
            stage="dataflow",
            job_id="job-456",
            ingested_imports=["import1"],
            metrics=None
        )

    @patch('routes.imports.import_utils.get_ingestion_metrics')
    def test_update_ingestion_history_retry(self, mock_get_ingestion_metrics):
        mock_spanner_client = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner_client

        mock_metrics = {
            'execution_time': 120,
            'node_count': 1000,
            'edge_count': 2000,
            'obs_count': 500,
            'ts_count': 300,
        }
        mock_get_ingestion_metrics.return_value = mock_metrics

        payload = {
            "workflowId": "wf-123",
            "status": "RETRY",
            "stage": "dataflow",
            "jobId": "job-456",
            "importList": [{"importName": "import1"}]
        }

        response = client.post("/imports/ingestion-history", json=payload)

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_get_ingestion_metrics.assert_called_once_with(
            config.PROJECT_ID, config.LOCATION, "job-456"
        )
        mock_spanner_client.update_ingestion_history.assert_called_once_with(
            workflow_id="wf-123",
            status="RETRY",
            stage="dataflow",
            job_id="job-456",
            ingested_imports=["import1"],
            metrics=mock_metrics
        )

if __name__ == '__main__':
    unittest.main()
