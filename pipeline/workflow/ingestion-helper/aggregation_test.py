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
# We need to make sure we can import from the CWD
import sys
import unittest
from unittest.mock import MagicMock
from unittest.mock import patch

sys.path.append(os.path.dirname(__file__))

from aggregation import BigQueryExecutor
from aggregation import LinkedEdgeGenerator
from aggregation import ProvenanceSummaryGenerator
from aggregation import PlaceAggregationGenerator
from aggregation.sql_utils import _escape_sql_literal
from aggregation_utils import AggregationUtils


class TestSQLUtils(unittest.TestCase):

    def test_escape_sql_literal(self):
        self.assertEqual(_escape_sql_literal("simple"), "simple")
        self.assertEqual(_escape_sql_literal("back\\slash"),
                         "back\\\\\\\\slash")
        self.assertEqual(_escape_sql_literal("double\"quote"),
                         "double\\\"quote")
        self.assertEqual(_escape_sql_literal("single'quote"), "single''quote")
        self.assertEqual(_escape_sql_literal("mixed\\'\""),
                         "mixed\\\\\\\\''\\\"")


@patch('aggregation.bq_executor.bigquery.Client')
class TestBigQueryExecutor(unittest.TestCase):

    def test_init(self, mock_bq_client):
        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db",
                                    location="loc")
        mock_bq_client.assert_called_once_with(project="proj", location="loc")
        self.assertEqual(
            executor.get_spanner_destination_uri(),
            "https://spanner.googleapis.com/projects/proj/instances/inst/databases/db"
        )

    def test_init_failure(self, mock_bq_client):
        mock_bq_client.side_effect = Exception("Auth error")
        with self.assertRaises(Exception):
            BigQueryExecutor(connection_id="conn",
                             project_id="proj",
                             instance_id="inst",
                             database_id="db")

    def test_execute_sequential(self, mock_bq_client):
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        mock_job = MagicMock()
        mock_job.job_id = "job_123"
        mock_client_instance.query.return_value = mock_job

        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db",
                                    run_sequential=True)

        job = executor.execute("SELECT 1")

        mock_client_instance.query.assert_called_once_with("SELECT 1",
                                                           job_config=None)
        mock_job.result.assert_called_once()  # Should block in sequential
        self.assertEqual(job.job_id, "job_123")

    def test_execute_async(self, mock_bq_client):
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        mock_job = MagicMock()
        mock_job.job_id = "job_123"
        mock_client_instance.query.return_value = mock_job

        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db",
                                    run_sequential=False)

        job = executor.execute("SELECT 1")

        mock_client_instance.query.assert_called_once_with("SELECT 1",
                                                           job_config=None)
        mock_job.result.assert_not_called()  # Should NOT block in async
        self.assertEqual(job.job_id, "job_123")

    def test_get_jobs_status_success(self, mock_bq_client):
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        mock_job1 = MagicMock()
        mock_job1.state = "DONE"
        mock_job1.error_result = None

        mock_job2 = MagicMock()
        mock_job2.state = "DONE"
        mock_job2.error_result = None

        mock_client_instance.get_job.side_effect = [mock_job1, mock_job2]

        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db")

        status = executor.get_jobs_status(["job1", "job2"])
        self.assertEqual(status, {"status": "DONE"})

    def test_get_jobs_status_running(self, mock_bq_client):
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        mock_job1 = MagicMock()
        mock_job1.state = "DONE"
        mock_job1.error_result = None

        mock_job2 = MagicMock()
        mock_job2.state = "RUNNING"
        mock_job2.error_result = None

        mock_client_instance.get_job.side_effect = [mock_job1, mock_job2]

        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db")

        status = executor.get_jobs_status(["job1", "job2"])
        self.assertEqual(status, {"status": "RUNNING"})

    def test_get_jobs_status_failed(self, mock_bq_client):
        mock_client_instance = MagicMock()
        mock_bq_client.return_value = mock_client_instance

        mock_job1 = MagicMock()
        mock_job1.state = "DONE"
        mock_job1.error_result = {"message": "some error"}

        mock_job2 = MagicMock()
        mock_job2.state = "RUNNING"
        mock_job2.error_result = None

        mock_client_instance.get_job.side_effect = [mock_job1, mock_job2]

        executor = BigQueryExecutor(connection_id="conn",
                                    project_id="proj",
                                    instance_id="inst",
                                    database_id="db")

        status = executor.get_jobs_status(["job1", "job2"])
        self.assertEqual(status["status"], "FAILED")
        self.assertIn("job1 failed", status["error"])
        self.assertEqual(status["failedJobs"], ["job1"])


class TestLinkedEdgeGenerator(unittest.TestCase):

    def setUp(self):
        self.mock_executor = MagicMock()
        self.mock_executor.connection_id = "test-conn"
        self.mock_executor.get_spanner_destination_uri.return_value = "spanner-uri"

    def test_run_all_empty(self):
        generator = LinkedEdgeGenerator(self.mock_executor)
        jobs = generator.run_all([])
        self.assertEqual(jobs, [])
        self.mock_executor.execute.assert_not_called()

    def test_run_all(self):
        generator = LinkedEdgeGenerator(self.mock_executor, is_base_dc=True)

        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        jobs = generator.run_all(["import1", "import2"])

        self.assertEqual(len(jobs), 3)  # Should run 3 queries
        self.assertEqual(self.mock_executor.execute.call_count, 3)

        # Verify queries contain import names and connection id
        calls = self.mock_executor.execute.call_args_list
        for call in calls:
            query = call[0][0]
            self.assertIn("test-conn", query)
            self.assertIn("import1", query)
            self.assertIn("import2", query)
            self.assertIn("spanner-uri", query)
            self.assertIn("dc/base/import1", query)  # Since is_base_dc=True


class TestProvenanceSummaryGenerator(unittest.TestCase):

    def setUp(self):
        self.mock_executor = MagicMock()
        self.mock_executor.connection_id = "test-conn"
        self.mock_executor.get_spanner_destination_uri.return_value = "spanner-uri"

    def test_run_all_empty(self):
        generator = ProvenanceSummaryGenerator(self.mock_executor)
        jobs = generator.run_all([])
        self.assertEqual(jobs, [])
        self.mock_executor.execute.assert_not_called()

    def test_run_all(self):
        generator = ProvenanceSummaryGenerator(self.mock_executor,
                                               is_base_dc=True)

        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        jobs = generator.run_all(["import1"])

        self.assertEqual(len(jobs), 1)
        self.mock_executor.execute.assert_called_once()

        query = self.mock_executor.execute.call_args[0][0]
        self.assertIn("test-conn", query)
        self.assertIn("import1", query)
        self.assertIn("spanner-uri", query)
        self.assertIn("CONCAT('dc/base/', raw.import_name)",
                      query)  # Since is_base_dc=True


class TestPlaceAggregationGenerator(unittest.TestCase):

    def setUp(self):
        self.mock_executor = MagicMock()
        self.mock_executor.connection_id = "test-conn"
        self.mock_executor.get_spanner_destination_uri.return_value = (
            "spanner-uri")

    def test_run_all_empty(self):
        generator = PlaceAggregationGenerator(self.mock_executor)
        jobs = generator.run_all([], "Count_Person", "CensusACS5yrSurvey",
                                 "2020")
        self.assertEqual(jobs, [])
        self.mock_executor.execute.assert_not_called()

    def test_run_all(self):
        generator = PlaceAggregationGenerator(self.mock_executor,
                                              is_base_dc=True)

        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        jobs = generator.run_all(["import1"], "Count_Person",
                                 "CensusACS5yrSurvey", "2020")

        self.assertEqual(len(jobs), 2)
        self.assertEqual(self.mock_executor.execute.call_count, 2)

        query1 = self.mock_executor.execute.call_args_list[0][0][0]
        self.assertIn("EXPORT DATA", query1)
        self.assertIn('spanner_options = \'{"table": "Observation_final_v2"}\'',
                      query1)
        self.assertIn("EXTERNAL_QUERY", query1)
        self.assertIn("test-conn", query1)
        self.assertIn("spanner-uri", query1)
        self.assertIn("import1", query1)
        self.assertIn('"country/USA" AS entity1', query1)
        self.assertIn("TimeSeries_final_v2", query1)
        self.assertIn("LENGTH(entity1) = 8", query1)
        self.assertIn("date = '2020'", query1)

        query2 = self.mock_executor.execute.call_args_list[1][0][0]
        self.assertIn("EXPORT DATA", query2)
        self.assertIn('spanner_options = \'{"table": "Observation_final_v2"}\'',
                      query2)
        self.assertIn("EXTERNAL_QUERY", query2)
        self.assertIn("test-conn", query2)
        self.assertIn("spanner-uri", query2)
        self.assertIn("import1", query2)
        self.assertIn("SUBSTR(ts.entity1, 1, 8) AS entity1", query2)
        self.assertIn("TimeSeries_final_v2", query2)
        self.assertIn("LENGTH(entity1) = 11", query2)
        self.assertIn("date = '2020'", query2)


@patch('aggregation_utils.BigQueryExecutor')
@patch('aggregation_utils.LinkedEdgeGenerator')
@patch('aggregation_utils.ProvenanceSummaryGenerator')
class TestAggregationUtils(unittest.TestCase):

    def test_run_aggregation(self, mock_prov_gen, mock_edge_gen, mock_executor):
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

    def test_check_aggregation_status(self, mock_prov_gen, mock_edge_gen,
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
