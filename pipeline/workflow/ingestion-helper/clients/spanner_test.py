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
    def test_initialize_database_all_exist(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db
        
        # Mock snapshot results (all tables exist)
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = [
            ["table", "Node"],
            ["table", "Edge"],
            ["table", "TimeSeries"],
            ["table", "Observation"],
            ["table", "NodeEmbedding"],
            ["table", "ImportStatus"],
            ["table", "IngestionHistory"],
            ["table", "ImportVersionHistory"],
            ["table", "IngestionLock"],
            ["table", "Cache"],
            ["table", "VariableMetadata"],
            ["index", "NodeEmbeddingIndex"],
            ["index", "InEdge"],
            ["index", "EdgeByProvenance"],
            ["index", "TimeSeriesByProvenance"],
            ["index", "TimeSeriesByEntity1"],
            ["index", "TimeSeriesByEntity2"],
            ["index", "TimeSeriesByEntity3"],
            ["model", "NodeEmbeddingModel"],
        ]
        
        client = SpannerClient("project", "instance", "database")
        
        # Run method
        client.initialize_database()
        
        # Verify update_ddl was NOT called
        mock_db.update_ddl.assert_not_called()

    @patch('clients.spanner.DatabaseAdminClient')
    @patch('google.cloud.spanner.Client')
    def test_initialize_database_none_exist(self, mock_spanner_client,
                                            mock_admin_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "projects/test-project/instances/test-instance/databases/test-db"
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        # Mock DatabaseAdminClient
        mock_admin_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_instance
        mock_operation = MagicMock()
        mock_admin_instance.update_database_ddl.return_value = mock_operation

        # Mock snapshot results (no tables exist)
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        client = SpannerClient("project", "instance", "database")

        def open_side_effect(file_path, mode='r', *args, **kwargs):
            m = MagicMock()
            m.__enter__.return_value.read.return_value = 'CREATE TABLE Node; CREATE TABLE NodeEmbedding; {% for model in models %}CREATE MODEL {{ model.name }} REMOTE OPTIONS (endpoint = \'{{ model.endpoint }}\');{% endfor %}'
            return m

        # Run method with patched open
        with patch('builtins.open', side_effect=open_side_effect):
            client.initialize_database()

        # Verify update_database_ddl WAS called
        mock_admin_instance.update_database_ddl.assert_called_once()
        mock_operation.result.assert_called_once()
        
        # Verify placeholder replacement
        args, kwargs = mock_admin_instance.update_database_ddl.call_args
        request = kwargs.get('request') if kwargs else args[0]
        statements = request.statements
        self.assertEqual(len(statements), 3)
        self.assertEqual(statements[0], "CREATE TABLE Node")
        self.assertEqual(statements[1], "CREATE TABLE NodeEmbedding")
        self.assertIn("projects/project/locations", statements[2])

    @patch('google.cloud.spanner.Client')
    def test_initialize_database_inconsistent_state(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db
        
        # Mock snapshot results (some tables exist)
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = [["table", "Node"]]
        
        client = SpannerClient("project", "instance", "database")
        
        # Run method and expect exception
        with self.assertRaises(RuntimeError):
            client.initialize_database()

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
    def test_seed_database(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        mock_transaction.execute_sql.return_value = []
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")

        # Run method
        client.seed_database()

        # Verify
        mock_transaction.insert.assert_called_once()
        args, kwargs = mock_transaction.insert.call_args
        self.assertEqual(kwargs['table'], 'Node')
        self.assertEqual(kwargs['columns'], ["subject_id", "name", "value", "types", "last_update_timestamp"])
        self.assertEqual(len(kwargs['values']), 5)
        expected_subjects = ["StatisticalVariable", "StatVarGroup", "StatVarObservation", "Topic", "dc/g/Root"]
        expected_names = ["StatisticalVariable", "StatVarGroup", "StatVarObservation", "Topic", "Data Commons Variables"]
        actual_subjects = [val[0] for val in kwargs['values']]
        actual_names = [val[1] for val in kwargs['values']]
        actual_values = [val[2] for val in kwargs['values']]
        self.assertEqual(actual_subjects, expected_subjects)
        self.assertEqual(actual_names, expected_names)
        self.assertEqual(actual_values, expected_subjects)

    @patch('google.cloud.spanner.Client')
    def test_seed_database_already_exists(self, mock_spanner_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        mock_transaction.execute_sql.return_value = [["StatisticalVariable"], ["StatVarGroup"], ["StatVarObservation"], ["Topic"], ["dc/g/Root"]]
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")

        # Run method
        client.seed_database()

        # Verify
        mock_transaction.insert.assert_not_called()

    @patch('clients.spanner.DatabaseAdminClient')
    @patch('google.cloud.spanner.Client')
    def test_initialize_database_multiple_models(self, mock_spanner_client,
                                                 mock_admin_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "projects/test-project/instances/test-instance/databases/test-db"
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        # Mock DatabaseAdminClient
        mock_admin_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_instance
        mock_operation = MagicMock()
        mock_admin_instance.update_database_ddl.return_value = mock_operation

        # Mock snapshot results (no tables exist)
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        client = SpannerClient("project", "instance", "database", models=[
            {"name": "NodeEmbeddingModel", "endpoint": "text-embedding-005"},
            {"name": "NodeEmbeddingModel_multimodalembedding_001", "endpoint": "multimodalembedding-001"}
        ], embedding_space=512)

        schema_ddl = """
        CREATE TABLE Node;
        CREATE TABLE NodeEmbedding (embeddings ARRAY<FLOAT64>(vector_length=>{{ embedding_space }}));
        {% for model in models %}
        CREATE MODEL {{ model.name }} REMOTE OPTIONS (endpoint = '{{ model.endpoint }}');
        {% endfor %}
        """

        def open_side_effect(file_path, mode='r', *args, **kwargs):
            m = MagicMock()
            m.__enter__.return_value.read.return_value = schema_ddl
            return m

        # Run method with patched open
        with patch('builtins.open', side_effect=open_side_effect):
            client.initialize_database()

        # Verify update_database_ddl WAS called
        mock_admin_instance.update_database_ddl.assert_called_once()
        mock_operation.result.assert_called_once()
        
        # Verify placeholder replacement
        args, kwargs = mock_admin_instance.update_database_ddl.call_args
        request = kwargs.get('request') if kwargs else args[0]
        statements = request.statements
        self.assertEqual(len(statements), 4)
        self.assertEqual(statements[0], "CREATE TABLE Node")
        self.assertEqual(statements[1], "CREATE TABLE NodeEmbedding (embeddings ARRAY<FLOAT64>(vector_length=>512))")
        self.assertEqual(
            statements[2],
            "CREATE MODEL NodeEmbeddingModel REMOTE OPTIONS (endpoint = "
            "'//aiplatform.googleapis.com/projects/project/locations/"
            "us-central1/publishers/google/models/text-embedding-005')"
        )
        self.assertEqual(
            statements[3],
            "CREATE MODEL NodeEmbeddingModel_multimodalembedding_001 REMOTE "
            "OPTIONS (endpoint = '//aiplatform.googleapis.com/projects/project/"
            "locations/us-central1/publishers/google/models/multimodalembedding-001')"
        )

    @patch('clients.spanner.DatabaseAdminClient')
    @patch('google.cloud.spanner.Client')
    def test_initialize_database_with_comments(self, mock_spanner_client,
                                               mock_admin_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "projects/test-project/instances/test-instance/databases/test-db"
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        # Mock DatabaseAdminClient
        mock_admin_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_instance
        mock_operation = MagicMock()
        mock_admin_instance.update_database_ddl.return_value = mock_operation

        client = SpannerClient("project", "instance", "database")
        client.check_database_initialized = MagicMock(
            return_value=(
                [], # Missing tables
                ["EdgeByProvenance"], # Missing indexes (forces initialization)
                [] # Missing models
            )
        )

        schema_ddl = """
        -- Comment with semicolon;
        CREATE TABLE Node;
        -- Another comment; with semicolon inside
        CREATE TABLE NodeEmbedding (embeddings ARRAY<FLOAT64>(vector_length=>{{ embedding_space }}));
        """

        def open_side_effect(file_path, mode='r', *args, **kwargs):
            m = MagicMock()
            m.__enter__.return_value.read.return_value = schema_ddl
            return m

        with patch('builtins.open', side_effect=open_side_effect):
            client.initialize_database()

        mock_admin_instance.update_database_ddl.assert_called_once()
        args, kwargs = mock_admin_instance.update_database_ddl.call_args
        request = kwargs.get('request') if kwargs else args[0]
        statements = request.statements
        self.assertEqual(len(statements), 2)
        self.assertEqual(statements[0], "CREATE TABLE Node")
        self.assertEqual(statements[1], "CREATE TABLE NodeEmbedding (embeddings ARRAY<FLOAT64>(vector_length=>768))")

    @patch('clients.spanner.DatabaseAdminClient')
    @patch('google.cloud.spanner.Client')
    def test_initialize_database_custom_table_and_index(self, mock_spanner_client,
                                                       mock_admin_client):
        # Setup mock
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_db.name = "projects/test-project/instances/test-instance/databases/test-db"
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        # Mock DatabaseAdminClient
        mock_admin_instance = MagicMock()
        mock_admin_client.return_value = mock_admin_instance
        mock_operation = MagicMock()
        mock_admin_instance.update_database_ddl.return_value = mock_operation

        # Mock snapshot results (no tables exist)
        mock_snapshot = MagicMock()
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_snapshot.execute_sql.return_value = []

        client = SpannerClient(
            "project", "instance", "database",
            embedding_table="CustomEmbeddingTable",
            embedding_index="CustomEmbeddingIndex"
        )

        schema_ddl = """
        CREATE TABLE Node;
        CREATE TABLE {{ embedding_table }} (
          subject_id STRING(1024) NOT NULL,
          embeddings ARRAY<FLOAT64>(vector_length=>{{ embedding_space }})
        ) PRIMARY KEY(subject_id);
        CREATE VECTOR INDEX {{ embedding_index }} ON {{ embedding_table }}(embeddings);
        """

        def open_side_effect(file_path, mode='r', *args, **kwargs):
            m = MagicMock()
            m.__enter__.return_value.read.return_value = schema_ddl
            return m

        # Run method with patched open
        with patch('builtins.open', side_effect=open_side_effect):
            client.initialize_database()

        # Verify update_database_ddl WAS called
        mock_admin_instance.update_database_ddl.assert_called_once()
        mock_operation.result.assert_called_once()
        
        # Verify placeholders were replaced correctly
        args, kwargs = mock_admin_instance.update_database_ddl.call_args
        request = kwargs.get('request') if kwargs else args[0]
        statements = request.statements
        self.assertEqual(len(statements), 3)
        self.assertEqual(statements[0], "CREATE TABLE Node")
        self.assertEqual(
            statements[1].strip(),
            "CREATE TABLE CustomEmbeddingTable (\n"
            "          subject_id STRING(1024) NOT NULL,\n"
            "          embeddings ARRAY<FLOAT64>(vector_length=>768)\n"
            "        ) PRIMARY KEY(subject_id)"
        )
        self.assertEqual(statements[2].strip(), "CREATE VECTOR INDEX CustomEmbeddingIndex ON CustomEmbeddingTable(embeddings)")

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_v1(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.check_failed_imports = MagicMock(return_value=False)
        client.update_ingestion_history_v1(
            workflow_id="wf-123",
            job_id="job-456",
            ingested_imports=["import1", "import2"],
            metrics={
                'execution_time': 120,
                'node_count': 1000,
                'edge_count': 2000,
                'obs_count': 500
            }
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')
        self.assertEqual(kwargs['columns'], [
            "CompletionTimestamp", "IngestionFailure",
            "WorkflowExecutionID", "DataflowJobID", "IngestedImports",
            "ExecutionTime", "NodeCount", "EdgeCount", "ObservationCount"
        ])
        self.assertEqual(kwargs['values'], [[
            spanner.COMMIT_TIMESTAMP,
            False,
            "wf-123",
            "job-456",
            ["import1", "import2"],
            120,
            1000,
            2000,
            500
        ]])

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_pending(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.PENDING,
            stage=IngestionStage.DATAFLOW,
            ingested_imports=["import1", "import2"]
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')
        self.assertEqual(kwargs['columns'], ["WorkflowExecutionID", "Status", "Stage", "CreationTimestamp", "IngestedImports"])
        self.assertEqual(kwargs['values'], [["wf-123", "PENDING", "dataflow", spanner.COMMIT_TIMESTAMP, ["import1", "import2"]]])

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_running(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.RUNNING,
            stage=IngestionStage.DATAFLOW,
            job_id="job-456"
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')
        self.assertEqual(kwargs['columns'], ["WorkflowExecutionID", "Status", "Stage", "DataflowJobID"])
        self.assertEqual(kwargs['values'], [["wf-123", "RUNNING", "dataflow", "job-456"]])

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_success(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.SUCCESS,
            stage=IngestionStage.DATAFLOW,
            job_id="job-456",
            metrics={
                'execution_time': 120,
                'node_count': 1000,
                'edge_count': 2000,
                'obs_count': 500
            }
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')
        
        self.assertIn("WorkflowExecutionID", kwargs['columns'])
        self.assertIn("Status", kwargs['columns'])
        self.assertIn("Stage", kwargs['columns'])
        self.assertIn("CompletionTimestamp", kwargs['columns'])
        self.assertNotIn("CreationTimestamp", kwargs['columns'])
        self.assertIn("IngestionFailure", kwargs['columns'])
        self.assertIn("DataflowJobID", kwargs['columns'])
        self.assertIn("ExecutionTime", kwargs['columns'])
        self.assertIn("NodeCount", kwargs['columns'])
        
        # Verify the specific values
        wf_idx = kwargs['columns'].index("WorkflowExecutionID")
        status_idx = kwargs['columns'].index("Status")
        failure_idx = kwargs['columns'].index("IngestionFailure")
        job_idx = kwargs['columns'].index("DataflowJobID")
        node_idx = kwargs['columns'].index("NodeCount")
        comp_time_idx = kwargs['columns'].index("CompletionTimestamp")
        
        values = kwargs['values'][0]
        self.assertEqual(values[wf_idx], "wf-123")
        self.assertEqual(values[status_idx], "SUCCESS")
        self.assertEqual(values[failure_idx], False)
        self.assertEqual(values[job_idx], "job-456")
        self.assertEqual(values[node_idx], 1000)
        self.assertEqual(values[comp_time_idx], spanner.COMMIT_TIMESTAMP)

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_failure(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.FAILURE,
            stage=IngestionStage.DATAFLOW,
            job_id="job-456",
            metrics={
                'execution_time': 120,
                'node_count': 1000,
                'edge_count': 2000,
                'obs_count': 500
            }
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')

        self.assertIn("CompletionTimestamp", kwargs['columns'])
        self.assertNotIn("CreationTimestamp", kwargs['columns'])
        self.assertIn("IngestionFailure", kwargs['columns'])
        failure_idx = kwargs['columns'].index("IngestionFailure")
        comp_time_idx = kwargs['columns'].index("CompletionTimestamp")
        values = kwargs['values'][0]
        self.assertEqual(values[failure_idx], True)
        self.assertEqual(values[comp_time_idx], spanner.COMMIT_TIMESTAMP)

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_no_stage(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.RUNNING,
            job_id="job-456"
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')
        self.assertNotIn("Stage", kwargs['columns'])
        self.assertEqual(kwargs['columns'], ["WorkflowExecutionID", "Status", "DataflowJobID"])
        self.assertEqual(kwargs['values'], [["wf-123", "RUNNING", "job-456"]])

    @patch('google.cloud.spanner.Client')
    def test_update_ingestion_history_retry(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_ingestion_history_v2(
            workflow_id="wf-123",
            status=IngestionState.RETRY,
            stage=IngestionStage.DATAFLOW,
            job_id="job-456",
            metrics={
                'execution_time': 120,
                'node_count': 1000,
                'edge_count': 2000,
                'obs_count': 500
            }
        )

        mock_transaction.insert_or_update.assert_called_once()
        _, kwargs = mock_transaction.insert_or_update.call_args
        self.assertEqual(kwargs['table'], 'IngestionHistory')

        self.assertIn("CompletionTimestamp", kwargs['columns'])
        self.assertNotIn("CreationTimestamp", kwargs['columns'])
        self.assertIn("IngestionFailure", kwargs['columns'])
        failure_idx = kwargs['columns'].index("IngestionFailure")
        comp_time_idx = kwargs['columns'].index("CompletionTimestamp")
        values = kwargs['values'][0]
        self.assertEqual(values[failure_idx], True)
        self.assertEqual(values[comp_time_idx], spanner.COMMIT_TIMESTAMP)

    @patch('google.cloud.spanner.Client')
    def test_update_import_version_history(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        import_list = [{"importName": "test_import", "latestVersion": "v1.2.3"}]
        client.update_import_version_history(import_list, workflow_id="wf-123")

        mock_transaction.insert.assert_called_once()
        _, kwargs = mock_transaction.insert.call_args
        self.assertEqual(kwargs['table'], 'ImportVersionHistory')
        self.assertEqual(kwargs['columns'], [
            "ImportName", "Version", "UpdateTimestamp",
            "WorkflowExecutionID", "Status", "ExecutionTime", "NodeCount",
            "EdgeCount", "ObservationCount", "Comment"
        ])
        self.assertEqual(kwargs['values'], [[
            "test_import", "v1.2.3", spanner.COMMIT_TIMESTAMP, "wf-123",
            None, None, None, None, None, "ingestion-workflow:wf-123"
        ]])

    @patch('google.cloud.spanner.Client')
    def test_update_version_history(self, mock_spanner_client):
        mock_instance = MagicMock()
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value = mock_instance
        mock_instance.database.return_value = mock_db

        mock_transaction = MagicMock()
        def run_in_transaction_side_effect(callback, *args, **kwargs):
            return callback(mock_transaction, *args, **kwargs)
        mock_db.run_in_transaction.side_effect = run_in_transaction_side_effect

        client = SpannerClient("project", "instance", "database")
        client.update_version_history("test_import", "v1.2.3", "ingestion-workflow:wf-789", workflow_id="wf-789")

        mock_transaction.insert.assert_called_once()
        _, kwargs = mock_transaction.insert.call_args
        self.assertEqual(kwargs['table'], 'ImportVersionHistory')
        self.assertEqual(kwargs['columns'], [
            "ImportName", "Version", "UpdateTimestamp",
            "WorkflowExecutionID", "Status", "ExecutionTime", "NodeCount",
            "EdgeCount", "ObservationCount", "Comment"
        ])
        self.assertEqual(kwargs['values'], [[
            "test_import", "v1.2.3", spanner.COMMIT_TIMESTAMP, "wf-789",
            None, None, None, None, None, "ingestion-workflow:wf-789"
        ]])

if __name__ == '__main__':
    unittest.main()

