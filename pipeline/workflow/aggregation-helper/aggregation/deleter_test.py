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

"""Unit tests for the AggregationDeleter class."""

import unittest
from unittest.mock import MagicMock, patch
from google.cloud import spanner
from aggregation.deleter import AggregationDeleter

class TestAggregationDeleter(unittest.TestCase):
    @patch('aggregation.deleter.spanner.Client')
    def test_delete_aggregated_data(self, mock_spanner_client):
        # Setup mocks
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = mock_db
        
        deleter = AggregationDeleter(
            project_id="proj",
            instance_id="inst",
            database_id="db",
            is_base_dc=True
        )
        
        imports = ["ImportA", "ImportB"]
        deleter.delete_aggregated_data(imports)
        
        # Verify execute_partitioned_dml calls
        self.assertEqual(mock_db.execute_partitioned_dml.call_count, 3)
        
        expected_provenances = ["dc/base/ImportA", "dc/base/ImportB"]
        expected_params = {"provenances": expected_provenances}
        
        # Check first call (Edge)
        call_args = mock_db.execute_partitioned_dml.call_args_list[0]
        self.assertIn("DELETE FROM Edge", call_args[0][0])
        self.assertEqual(call_args[1]["params"], expected_params)
        
        # Check second call (TimeSeries)
        call_args = mock_db.execute_partitioned_dml.call_args_list[1]
        self.assertIn("DELETE FROM TimeSeries", call_args[0][0])
        self.assertEqual(call_args[1]["params"], expected_params)
        
        # Check third call (Cache)
        call_args = mock_db.execute_partitioned_dml.call_args_list[2]
        self.assertIn("DELETE FROM Cache", call_args[0][0])
        self.assertEqual(call_args[1]["params"], expected_params)

    @patch('aggregation.deleter.spanner.Client')
    def test_delete_aggregated_data_empty(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = mock_db
        
        deleter = AggregationDeleter("proj", "inst", "db")
        deleter.delete_aggregated_data([])
        
        mock_db.execute_partitioned_dml.assert_not_called()

    @patch('aggregation.deleter.spanner.Client')
    def test_delete_aggregated_data_not_base_dc(self, mock_spanner_client):
        # Setup mocks
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = mock_db
        
        deleter = AggregationDeleter(
            project_id="proj",
            instance_id="inst",
            database_id="db",
            is_base_dc=False
        )
        
        imports = ["ImportA"]
        deleter.delete_aggregated_data(imports)
        
        expected_provenances = ["ImportA"]
        expected_params = {"provenances": expected_provenances}
        
        call_args = mock_db.execute_partitioned_dml.call_args_list[0]
        self.assertEqual(call_args[1]["params"], expected_params)

if __name__ == '__main__':
    unittest.main()
