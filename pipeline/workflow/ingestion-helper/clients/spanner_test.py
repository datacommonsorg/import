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
import sys
import os

# Add the current directory to path so we can import spanner_client
sys.path.append(os.path.dirname(__file__))
from google.cloud import spanner
from clients.spanner import SpannerClient, IngestionState, IngestionStage

class TestSpannerClient(unittest.TestCase):

    @patch('google.cloud.spanner.Client')
    def test_acquire_lock_new_row(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db
        
        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect
        
        # Mock execute_sql to return empty results (no row found)
        mock_transaction.execute_sql.return_value = []
        
        client = SpannerClient("project", "instance", "database")
        
        # Run method
        result = client.acquire_lock("workflow-123", 3600)
        
        # Verify
        self.assertTrue(result)
        mock_transaction.execute_update.assert_called_once()
        args, _ = mock_transaction.execute_update.call_args
        self.assertIn("INSERT INTO IngestionLock", args[0])

    @patch('google.cloud.spanner.Client')
    def test_acquire_lock_existing_row(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db
        
        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect
        
        # Mock execute_sql to return existing lock (owner is None)
        mock_transaction.execute_sql.return_value = [[None, None]]
        
        client = SpannerClient("project", "instance", "database")
        
        # Run method
        result = client.acquire_lock("workflow-123", 3600)
        
        # Verify
        self.assertTrue(result)
        mock_transaction.execute_update.assert_called_once()
        args, _ = mock_transaction.execute_update.call_args
        self.assertIn("UPDATE IngestionLock", args[0])

    @patch('google.cloud.spanner.Client')
    def test_revert_import_state(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_txn = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_txn, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        success = client.revert_import_state(
            import_name="imp1",
            new_latest_version_path="gs://bucket/path/v1",
            previous_version="v1",
            workflow_id="wf-123",
            comment="Reverted batch workflow"
        )
        self.assertTrue(success)
        self.assertEqual(mock_txn.execute_update.call_count, 2)

    @patch('google.cloud.spanner.Client')
    def test_get_imports_for_workflow(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_results = MagicMock()
        mock_results.__iter__.return_value = [[["imp1", "imp2"]]]
        mock_snapshot.execute_sql.return_value = mock_results

        client = SpannerClient("project", "instance", "database")
        imports = client.get_imports_for_workflow("wf-123")
        self.assertEqual(imports, ["imp1", "imp2"])

    @patch('google.cloud.spanner.Client')
    def test_get_import_info_with_provided_dict(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        client = SpannerClient("project", "instance", "database")
        import_list = [{
            "importName": "EurostatData",
            "latestVersion": "gs://datcom-prod-imports/scripts/eurostat/2026_08_03T19_03_05_074661_07_00/*/*.mcf"
        }]

        result = client.get_import_info(import_list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["importName"], "EurostatData")
        self.assertEqual(result[0]["latestVersion"], "gs://datcom-prod-imports/scripts/eurostat/2026_08_03T19_03_05_074661_07_00/*/*.mcf")


    @patch('google.cloud.spanner.Client')
    def test_get_import_info_empty_list(self, mock_spanner_client):
        client = SpannerClient("project", "instance", "database")
        self.assertEqual(client.get_import_info([]), [])
        self.assertEqual(client.get_import_info(None), [])

    @patch('google.cloud.spanner.Client')
    def test_get_import_info_skips_when_already_success(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = [
            ["EurostatData", "gs://bucket/v1"]
        ]

        client = SpannerClient("project", "instance", "database")
        import_list = [{
            "importName": "EurostatData",
            "latestVersion": "gs://bucket/v1"
        }]

        result = client.get_import_info(import_list)
        self.assertEqual(len(result), 0)

    @patch('google.cloud.spanner.Client')
    def test_get_import_info_does_not_skip_when_not_in_success_imports(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        client = SpannerClient("project", "instance", "database")
        import_list = [{
            "importName": "NewImport",
            "latestVersion": "gs://bucket/v1"
        }]

        result = client.get_import_info(import_list)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["importName"], "NewImport")

    @patch('google.cloud.spanner.Client')
    def test_get_import_info_filters_successful_versions(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        # Database returns SUCCESS for ImportAlreadyIngested (v1), ImportWithNewVersion (v1), ImportForced (v1)
        mock_snapshot.execute_sql.return_value = [
            ["ImportAlreadyIngested", "gs://bucket/v1"],
            ["ImportWithNewVersion", "gs://bucket/v1"],
            ["ImportForced", "gs://bucket/v1"],
        ]

        client = SpannerClient("project", "instance", "database")
        import_list = [
            # 1. Already ingested with same version -> should be filtered out / skipped
            {
                "importName": "ImportAlreadyIngested",
                "latestVersion": "gs://bucket/v1",
            },
            # 2. Ingested with v1, but requested with v2 -> should NOT be skipped
            {
                "importName": "ImportWithNewVersion",
                "latestVersion": "gs://bucket/v2",
            },
            # 3. Brand new import not in ImportStatus -> should NOT be skipped
            {
                "importName": "ImportBrandNew",
                "latestVersion": "gs://bucket/v1",
            },
        ]

        result = client.get_import_info(import_list)
        result_names = [item["importName"] for item in result]

        self.assertEqual(len(result), 2)
        self.assertNotIn("ImportAlreadyIngested", result_names)
        self.assertIn("ImportWithNewVersion", result_names)
        self.assertIn("ImportBrandNew", result_names)

        # When force_ingestion=True, nothing is filtered out
        result_forced = client.get_import_info(import_list, force_ingestion=True)
        self.assertEqual(len(result_forced), 3)


if __name__ == '__main__':
    unittest.main()


