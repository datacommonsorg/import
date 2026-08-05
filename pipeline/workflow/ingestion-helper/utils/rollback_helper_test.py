# Copyright 2025 Google LLC
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
from unittest.mock import MagicMock
from utils.rollback_helper import revert_import, revert_imports


class TestRollbackHelper(unittest.TestCase):

    def test_revert_import_success(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = [
            "gs://bucket/path/v2/*/*.mcf", "gs://bucket/path/v1/*/*.mcf"
        ]
        mock_spanner.revert_import_state.return_value = True

        status, cur_ver, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertTrue(status)
        self.assertEqual(cur_ver, "gs://bucket/path/v2/*/*.mcf")
        self.assertEqual(prev_ver, "gs://bucket/path/v1/*/*.mcf")
        mock_spanner.get_import_version_history.assert_called_once_with("imp1")
        mock_spanner.revert_import_state.assert_called_once_with(
            import_name="imp1",
            new_latest_version_path="gs://bucket/path/v1/*/*.mcf",
            previous_version="gs://bucket/path/v1/*/*.mcf",
            workflow_id="wf-123",
            comment="Reverted batch workflow (wf-123)"
        )

    def test_revert_import_dry_run(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = [
            "gs://bucket/path/v2/*/*.mcf", "gs://bucket/path/v1/*/*.mcf"
        ]

        status, cur_ver, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123", dry_run=True)

        self.assertTrue(status)
        self.assertEqual(cur_ver, "gs://bucket/path/v2/*/*.mcf")
        self.assertEqual(prev_ver, "gs://bucket/path/v1/*/*.mcf")
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_import_no_history(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = []

        status, cur_ver, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertFalse(status)
        self.assertIsNone(cur_ver)
        self.assertIsNone(prev_ver)
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_import_no_previous_version(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = ["gs://bucket/path/v1/*/*.mcf"]

        status, cur_ver, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertFalse(status)
        self.assertEqual(cur_ver, "gs://bucket/path/v1/*/*.mcf")
        self.assertIsNone(prev_ver)
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_imports_list(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.side_effect = lambda name: [
            "gs://bucket/path/v2/*/*.mcf", "gs://bucket/path/v1/*/*.mcf"
        ] if name == "imp1" else []
        mock_spanner.revert_import_state.return_value = True

        results = revert_imports(mock_spanner, [{"importName": "imp1"}, "imp2"], "wf-123")

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]["reverted"])
        self.assertEqual(results[0]["importName"], "imp1")
        self.assertEqual(results[0]["failedVersion"], "gs://bucket/path/v2/*/*.mcf")
        self.assertEqual(results[0]["restoredVersion"], "gs://bucket/path/v1/*/*.mcf")

        self.assertFalse(results[1]["reverted"])
        self.assertEqual(results[1]["importName"], "imp2")
        self.assertIsNone(results[1]["restoredVersion"])
        mock_spanner.update_ingestion_status.assert_not_called()


if __name__ == '__main__':
    unittest.main()
