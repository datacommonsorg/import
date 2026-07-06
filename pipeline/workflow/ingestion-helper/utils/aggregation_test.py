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

import os
import sys
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from utils.aggregation import AggregationUtils


@patch('utils.aggregation.BigQueryExecutor')
@patch('utils.aggregation.LinkedEdgeGenerator')
@patch('utils.aggregation.ProvenanceSummaryGenerator')
@patch('utils.aggregation.StatVarGroupGenerator')
class TestAggregationUtils(unittest.TestCase):

    def test_run_aggregation(self, mock_svg_gen, mock_prov_gen, mock_edge_gen,
                             mock_executor):
        # Setup mocks
        mock_executor_instance = MagicMock()
        mock_executor.return_value = mock_executor_instance

        mock_edge_gen_instance = MagicMock()
        mock_edge_gen.return_value = mock_edge_gen_instance
        mock_job1 = MagicMock()
        mock_job1.job_id = "job1"
        mock_edge_gen_instance.run_all.return_value = [mock_job1]

        mock_prov_gen_instance = MagicMock()
        mock_prov_gen.return_value = mock_prov_gen_instance
        mock_job2 = MagicMock()
        mock_job2.job_id = "job2"
        mock_prov_gen_instance.run_all.return_value = [mock_job2]

        utils = AggregationUtils(connection_id="conn",
                                 project_id="proj",
                                 instance_id="inst",
                                 database_id="db",
                                 is_base_dc=True)

        import_list = [{'importName': 'import1'}, {'importName': 'import2'}]
        job_ids = utils.run_aggregation(import_list)

        # Verify standard import queries were executed
        self.assertEqual(mock_executor_instance.execute.call_count, 2)

        # Verify generators were called
        mock_edge_gen_instance.run_all.assert_called_once_with(
            ["import1", "import2"])
        mock_prov_gen_instance.run_all.assert_called_once_with(
            ["import1", "import2"])

        self.assertEqual(job_ids, ["job1", "job2"])

    def test_check_aggregation_status(self, mock_svg_gen, mock_prov_gen, mock_edge_gen,
                                      mock_executor):
        mock_executor_instance = MagicMock()
        mock_executor.return_value = mock_executor_instance
        mock_executor_instance.get_jobs_status.return_value = {"status": "DONE"}

        utils = AggregationUtils(connection_id="conn",
                                 project_id="proj",
                                 instance_id="inst",
                                 database_id="db")

        status = utils.check_aggregation_status(["job1", "job2"])
        mock_executor_instance.get_jobs_status.assert_called_once_with(
            ["job1", "job2"])
        self.assertEqual(status, {"status": "DONE"})


if __name__ == '__main__':
    unittest.main()
