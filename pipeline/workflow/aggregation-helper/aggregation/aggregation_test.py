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

from aggregation import BigQueryExecutor
from aggregation import LinkedEdgeGenerator
from aggregation import ProvenanceSummaryGenerator
from aggregation import PlaceAggregationGenerator
from aggregation import EmbeddingGenerator
from aggregation.embedding_generator import EmbeddingSpec
from aggregation.sql_utils import _escape_sql_literal


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
        _ = executor.client
        mock_bq_client.assert_called_once_with(project="proj", location="loc")
        self.assertEqual(
            executor.get_spanner_destination_uri(),
            "https://spanner.googleapis.com/projects/proj/instances/inst/databases/db"
        )

    def test_init_failure(self, mock_bq_client):
        mock_bq_client.side_effect = Exception("Auth error")
        with self.assertRaises(Exception):
            executor = BigQueryExecutor(connection_id="conn",
                             project_id="proj",
                             instance_id="inst",
                             database_id="db")
            _ = executor.client

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
        self.assertIn("'dc/base/import1'",
                      query)  # Since is_base_dc=True


class TestPlaceAggregationGenerator(unittest.TestCase):

    def setUp(self):
        self.mock_executor = MagicMock()
        self.mock_executor.connection_id = "test-conn"
        self.mock_executor.get_spanner_destination_uri.return_value = "spanner-uri"

    def test_aggregate_places_empty_imports(self):
        """Verifies that it returns None immediately if import list is empty."""
        generator = PlaceAggregationGenerator(self.mock_executor)
        job = generator.aggregate_places(
            import_names=[],
            source_type="County",
            destination_type="State"
        )
        self.assertIsNone(job)
        self.mock_executor.execute.assert_not_called()

    def test_aggregate_places_calls_executor(self):
        """Verifies that it constructs a query and delegates execution to the executor."""
        generator = PlaceAggregationGenerator(self.mock_executor)
        
        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        job = generator.aggregate_places(
            import_names=["import1"],
            source_type="County",
            destination_type="State"
        )
        
        # Should return the job returned by the executor
        self.assertEqual(job, mock_job)
        
        # Should have called execute once with a non-empty query string
        self.mock_executor.execute.assert_called_once()
        query = self.mock_executor.execute.call_args[0][0]
        self.assertIsInstance(query, str)
        self.assertTrue(len(query) > 0)


class TestEmbeddingGenerator(unittest.TestCase):

    def setUp(self):
        self.mock_executor = MagicMock()
        self.mock_executor.connection_id = "test-conn"
        self.mock_executor.get_spanner_destination_uri.return_value = "spanner-uri"

    @patch.dict('os.environ', {'ENABLE_EMBEDDINGS': 'true'})
    def test_run_all(self):
        generator = EmbeddingGenerator(self.mock_executor, is_base_dc=True)
        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        specs = [
            EmbeddingSpec(
                embedding_label="test_embedding",
                model_name="TestModel",
                model_endpoint="text-embedding-005",
                task_type="TEST_TASK",
                node_types=["StatVar"],
                node_filter_type="NoFilter"
            )
        ]
        jobs = generator.run_all(specs=specs, embedding_table="CustomEmbeddingTable")

        self.assertEqual(len(jobs), 1)
        self.mock_executor.execute.assert_called_once()
        query = self.mock_executor.execute.call_args[0][0]
        self.assertIn("test-conn", query)
        self.assertIn("spanner-uri", query)
        self.assertIn("TestModel", query)
        self.assertIn("test_embedding", query)
        self.assertIn("CustomEmbeddingTable", query)
        self.assertIn("TEST_TASK", query)

    @patch.dict('os.environ', {'ENABLE_EMBEDDINGS': 'true'})
    @patch('aggregation.embedding_generator._extract_nl_stat_var', return_value={'statVar1': 'sentence1', 'statVar2': 'sentence2'})
    def test_run_all_nl_stat_var(self, mock_extract):
        generator = EmbeddingGenerator(self.mock_executor, is_base_dc=True)
        mock_job = MagicMock()
        self.mock_executor.execute.return_value = mock_job

        specs = [
            EmbeddingSpec(
                embedding_label="test_embedding",
                model_name="TestModel",
                model_endpoint="text-embedding-005",
                task_type="TEST_TASK",
                node_types=["StatVar"],
                node_filter_type="NLStatisticalVariable"
            )
        ]
        jobs = generator.run_all(specs=specs, embedding_table="CustomEmbeddingTable")

        self.assertEqual(len(jobs), 1)
        self.mock_executor.execute.assert_called_once()
        query = self.mock_executor.execute.call_args[0][0]
        job_config = self.mock_executor.execute.call_args[1].get('job_config') or self.mock_executor.execute.call_args.kwargs.get('job_config')
        self.assertIn("FROM UNNEST(@nl_stat_vars)", query)
        self.assertIn("INNER JOIN raw_nodes", query)
        self.assertIsNotNone(job_config)
        struct_params = [p.values for p in job_config.query_parameters if p.name == 'nl_stat_vars'][0]
        
        dcids = []
        sentences = []
        for sp in struct_params:
            dcids.append(sp.struct_values.get("dcid"))
            sentences.append(sp.struct_values.get("sentence"))
                    
        self.assertIn("statVar1", dcids)
        self.assertIn("statVar2", dcids)
        self.assertIn("sentence1", sentences)
        self.assertIn("sentence2", sentences)


if __name__ == '__main__':
    unittest.main()
