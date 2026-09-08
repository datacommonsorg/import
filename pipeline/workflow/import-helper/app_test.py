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

import base64
import json
import unittest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient
from app import app
from dependencies import get_spanner_client, get_storage_client

client = TestClient(app)


class AppTest(unittest.TestCase):

    def setUp(self):
        app.dependency_overrides.clear()

    def tearDown(self):
        app.dependency_overrides.clear()

    def test_update_import_status_success(self):
        mock_spanner = MagicMock()
        mock_storage = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner
        app.dependency_overrides[get_storage_client] = lambda: mock_storage

        payload = {
            "imports": [
                {
                    "importName": "import1",
                    "status": "STAGING",
                    "latestVersion": "gs://bucket/import1/version1/graph",
                    "graphPath": "graph"
                },
                {
                    "importName": "import2",
                    "status": "FAILURE",
                    "latestVersion": "gs://bucket/import2/version2/graph"
                }
            ],
            "jobId": "job123",
            "executionTime": 120,
            "dataVolume": 1024
        }

        response = client.post("/imports/status", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")

        # Storage should only be updated for the STAGING import
        mock_storage.update_version_file.assert_any_call("import1", "graph", is_staging=True)
        mock_storage.update_version_file.assert_any_call("import1", "graph", is_staging=False)
        mock_storage.update_provenance_file.assert_called_once_with("import1", "graph")
        self.assertEqual(mock_storage.update_import_summary.call_count, 1)

        # Spanner update_import_history should be called for both STAGING and FAILURE
        self.assertEqual(mock_spanner.update_import_history.call_count, 2)
        # Spanner update_import_summary should be called for both
        self.assertEqual(mock_spanner.update_import_summary.call_count, 2)

    def test_update_import_version_success(self):
        mock_spanner = MagicMock()
        mock_storage = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner
        app.dependency_overrides[get_storage_client] = lambda: mock_storage

        mock_storage.get_staging_version.side_effect = lambda name: f"ver_{name}"
        mock_storage.get_import_summary.side_effect = lambda name, version: {
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

        self.assertEqual(mock_storage.update_provenance_file.call_count, 2)
        self.assertEqual(mock_storage.update_version_file.call_count, 2)
        self.assertEqual(mock_spanner.update_import_history.call_count, 2)
        self.assertEqual(mock_spanner.update_import_summary.call_count, 2)

    @patch('routes.imports.import_utils.get_caller_identity')
    def test_update_import_version_override(self, mock_get_caller):
        mock_get_caller.return_value = "tester@google.com"
        mock_spanner = MagicMock()
        mock_storage = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner
        app.dependency_overrides[get_storage_client] = lambda: mock_storage

        mock_storage.get_staging_version.side_effect = lambda name: f"ver_{name}"
        mock_storage.get_import_summary.side_effect = lambda name, version: {
            "importName": name,
            "status": "NOT_STAGING",
            "latestVersion": f"gs://bucket/{name}/{version}.csv"
        }

        payload = {
            "imports": ["import1"],
            "version": "STAGING",
            "comment": "release-comment",
            "override": True
        }

        response = client.post("/imports/version", json=payload)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")

        mock_spanner.update_import_history.assert_called_once_with(
            "import1",
            "gs://bucket/import1/ver_import1.csv",
            "version-override:tester@google.com release-comment",
            workflow_id=None,
            job_id=None,
            status="STAGING"
        )
        self.assertEqual(mock_spanner.update_import_summary.call_count, 1)

    @patch('routes.events.import_utils.invoke_spanner_ingestion_workflow')
    @patch('routes.events.import_utils.check_duplicate', return_value=False)
    @patch('routes.events.config.PROJECT_ID', 'test-project')
    @patch('routes.events.config.LOCATION', 'us-central1')
    def test_handle_feed_event_spanner_ingestion(self, mock_check_dup, mock_invoke):
        mock_spanner = MagicMock()
        mock_storage = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner
        app.dependency_overrides[get_storage_client] = lambda: mock_storage

        notification = {
            "attributes": {
                "transfer_status": "TRANSFER_COMPLETED",
                "import_name": "scripts/us_fed:Rates",
                "import_version": "2026-09-01",
                "post_process": "spanner_ingestion_workflow",
                "graph_path": "/**/*.mcf*"
            },
            "messageId": "msg-123",
            "data": base64.b64encode(b'{"test": "data"}').decode('utf-8')
        }

        response = client.post("/imports/feed", json={"message": notification})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")

        mock_invoke.assert_called_once()
        self.assertEqual(mock_spanner.update_import_summary.call_count, 1)

    @patch('routes.events.import_utils.invoke_import_automation_workflow')
    @patch('routes.events.import_utils.check_duplicate', return_value=False)
    @patch('routes.events.config.PROJECT_ID', 'test-project')
    @patch('routes.events.config.LOCATION', 'us-central1')
    def test_handle_feed_event_import_automation(self, mock_check_dup, mock_invoke):
        mock_spanner = MagicMock()
        mock_storage = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner
        app.dependency_overrides[get_storage_client] = lambda: mock_storage

        notification = {
            "attributes": {
                "transfer_status": "TRANSFER_COMPLETED",
                "import_name": "scripts/us_fed:Rates",
                "import_version": "2026-09-01",
                "post_process": "import_automation_workflow",
                "graph_path": "/**/*.mcf*",
                "import_size": "medium"
            },
            "messageId": "msg-456",
            "data": base64.b64encode(b'{"test": "data"}').decode('utf-8')
        }

        response = client.post("/imports/feed", json={"message": notification})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")

        mock_invoke.assert_called_once()

    def test_database_initialize_endpoint(self):
        mock_spanner = MagicMock()
        app.dependency_overrides[get_spanner_client] = lambda: mock_spanner

        response = client.post("/database/initialize")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["status"], "OK")
        mock_spanner.initialize_database.assert_called_once()

    @patch('clients.spanner.DatabaseAdminClient')
    @patch('google.cloud.spanner.Client')
    def test_spanner_client_initialize_database(self, mock_spanner_client, mock_admin_client):
        from clients.spanner import SpannerClient as HelperSpannerClient

        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "projects/p/instances/i/databases/d"
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        # Snapshot returns no tables
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        mock_admin_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_instance
        mock_operation = MagicMock()
        mock_admin_instance.update_database_ddl.return_value = mock_operation

        spanner_client = HelperSpannerClient("p", "i", "d")
        spanner_client.initialize_database()

        mock_admin_instance.update_database_ddl.assert_called_once()
        mock_operation.result.assert_called_once()
        _, kwargs = mock_admin_instance.update_database_ddl.call_args
        request = kwargs.get('request')
        self.assertEqual(len(request.statements), 2)
        self.assertTrue(any("ImportSummary" in s for s in request.statements))
        self.assertTrue(any("ImportHistory" in s for s in request.statements))


if __name__ == '__main__':
    unittest.main()
