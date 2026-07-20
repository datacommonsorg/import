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
            ("v2", "update v2"),
            ("v1", "initial v1")
        ]
        mock_spanner.get_import_latest_version.return_value = "gs://bucket/path/v2"
        mock_spanner.revert_import_state.return_value = True

        status, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertTrue(status)
        self.assertEqual(prev_ver, "v1")
        mock_spanner.get_import_version_history.assert_called_once_with("imp1")
        mock_spanner.get_import_latest_version.assert_called_once_with("imp1")
        mock_spanner.revert_import_state.assert_called_once_with(
            import_name="imp1",
            new_latest_version_path="gs://bucket/path/v1",
            previous_version="v1",
            workflow_id="wf-123",
            comment="Reverted batch workflow (wf-123)"
        )

    def test_revert_import_dry_run(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = [
            ("v2", "update v2"),
            ("v1", "initial v1")
        ]
        mock_spanner.get_import_latest_version.return_value = "gs://bucket/path/v2"

        status, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123", dry_run=True)

        self.assertTrue(status)
        self.assertEqual(prev_ver, "v1")
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_import_no_history(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = []

        status, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertFalse(status)
        self.assertIsNone(prev_ver)
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_import_no_previous_version(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.return_value = [("v1", "initial v1")]

        status, prev_ver = revert_import(mock_spanner, "foo:bar:imp1", "wf-123")

        self.assertFalse(status)
        self.assertIsNone(prev_ver)
        mock_spanner.revert_import_state.assert_not_called()

    def test_revert_imports_list(self):
        mock_spanner = MagicMock()
        mock_spanner.get_import_version_history.side_effect = lambda name: [("v2", "u2"), ("v1", "u1")] if name == "imp1" else []
        mock_spanner.get_import_latest_version.return_value = "gs://bucket/path/v2"
        mock_spanner.revert_import_state.return_value = True

        results = revert_imports(mock_spanner, [{"importName": "imp1"}, "imp2"], "wf-123")

        self.assertEqual(len(results), 2)
        self.assertTrue(results[0]["reverted"])
        self.assertEqual(results[0]["importName"], "imp1")
        self.assertEqual(results[0]["previousVersion"], "v1")

        self.assertFalse(results[1]["reverted"])
        self.assertEqual(results[1]["importName"], "imp2")
        self.assertIsNone(results[1]["previousVersion"])
        mock_spanner.update_ingestion_status.assert_not_called()


if __name__ == '__main__':
    unittest.main()
