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

from fastapi.testclient import TestClient
from app import app
from dependencies import get_spanner_client, get_storage_client

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
            "import1", "ver_import1", "version-override:test-caller release-comment"
        )
        mock_spanner_client.update_version_history.assert_any_call(
            "import2", "ver_import2", "version-override:test-caller release-comment"
        )
        self.assertEqual(mock_spanner_client.update_version_history.call_count, 2)
        self.assertEqual(mock_spanner_client.update_import_status.call_count, 2)
        
        # Verify get_caller_identity was called exactly once outside of the loop
        mock_get_caller_identity.assert_called_once()

    @patch('routes.aggregation._get_orchestrator')
    def test_aggregation_initiate_success(self, mock_aggregation_utils):
        # Setup mock orchestrator
        mock_instance = MagicMock()
        mock_aggregation_utils.return_value = mock_instance
        mock_instance.get_active_stages.return_value = [1]
        mock_instance.execute_stage.return_value = ["job-1", "job-2"]

        # Call endpoint
        payload = {
            "importList": [{"importName": "USFed_Census"}]
        }
        response = client.post("/aggregation/initiate", json=payload)

        # Assertions
        self.assertEqual(response.status_code, 200)
        state = response.json()
        self.assertEqual(state["status"], "RUNNING")
        self.assertEqual(state["current_stage"], 1)
        self.assertEqual(state["active_job_ids"], ["job-1", "job-2"])
        self.assertEqual(state["import_list"], [{"importName": "USFed_Census"}])

    @patch('routes.aggregation._get_orchestrator')
    def test_aggregation_poll_transition(self, mock_aggregation_utils):
        # Setup mock orchestrator to simulate Stage 1 completion and Stage 2 execution
        mock_instance = MagicMock()
        mock_aggregation_utils.return_value = mock_instance
        mock_instance.get_active_stages.return_value = [1, 2]
        
        # Mock BQ reporting Stage 1 jobs are DONE
        mock_instance.check_jobs_status.return_value = {"status": "DONE"}
        mock_instance.execute_stage.return_value = ["job-stage2-1"]

        # Input state (Stage 1 completed)
        payload = {
            "status": "RUNNING",
            "current_stage": 1,
            "active_job_ids": ["job-1", "job-2"],
            "import_list": [{"importName": "USFed_Census"}]
        }
        
        # Call endpoint
        response = client.post("/aggregation/poll", json=payload)

        # Assertions
        self.assertEqual(response.status_code, 200)
        state = response.json()
        self.assertEqual(state["status"], "RUNNING")
        self.assertEqual(state["current_stage"], 2) # Transitioned to 2!
        self.assertEqual(state["active_job_ids"], ["job-stage2-1"])

    @patch('routes.aggregation._get_orchestrator')
    def test_aggregation_poll_still_running(self, mock_aggregation_utils):
        # Setup mock orchestrator to simulate jobs still in PENDING state
        mock_instance = MagicMock()
        mock_aggregation_utils.return_value = mock_instance
        mock_instance.get_active_stages.return_value = [1]
        
        # Mock BQ reporting Stage 1 jobs are PENDING (still executing)
        mock_instance.check_jobs_status.return_value = {"status": "PENDING"}

        # Input state
        payload = {
            "status": "RUNNING",
            "current_stage": 1,
            "active_job_ids": ["job-1", "job-2"],
            "import_list": [{"importName": "USFed_Census"}]
        }
        
        # Call endpoint
        response = client.post("/aggregation/poll", json=payload)

        # Assertions
        self.assertEqual(response.status_code, 200)
        state = response.json()
        # Verify state is returned unchanged
        self.assertEqual(state["status"], "RUNNING")
        self.assertEqual(state["current_stage"], 1)
        self.assertEqual(state["active_job_ids"], ["job-1", "job-2"])

    @patch('routes.aggregation._get_orchestrator')
    def test_aggregation_run(self, mock_aggregation_utils):
        # Setup mock orchestrator
        mock_instance = MagicMock()
        mock_aggregation_utils.return_value = mock_instance
        mock_instance.get_active_stages.return_value = [1, 2]
        mock_instance.execute_stage.side_effect = lambda stage, imports: [f"job-stage{stage}-1"]

        # Call legacy endpoint
        payload = {
            "importList": [{"importName": "USFed_Census"}]
        }
        response = client.post("/aggregation/run", json=payload)

        # Assertions (should return all jobs from all stages in parallel)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "SUBMITTED")
        self.assertEqual(data["jobIds"], ["job-stage1-1", "job-stage2-1"])


if __name__ == '__main__':
    unittest.main()
