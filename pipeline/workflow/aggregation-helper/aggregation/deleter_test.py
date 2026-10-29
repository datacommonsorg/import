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
from aggregation.deleter import AggregationDeleter


class TestAggregationDeleter(unittest.TestCase):
    @patch("aggregation.deleter.spanner.Client")
    def test_delete_aggregated_data(self, mock_spanner_client):
        # Setup mocks
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter(
            project_id="proj", instance_id="inst", database_id="db", is_base_dc=True
        )

        imports = ["ImportA", "ImportB"]
        deleter.delete_aggregated_data(imports)

        # Verify execute_partitioned_dml calls (order-independent due to parallel execution)
        self.assertEqual(mock_db.execute_partitioned_dml.call_count, 2)

        expected_provenances = ["dc/base/ImportA", "dc/base/ImportB"]
        expected_params = {"provenances": expected_provenances}

        calls = mock_db.execute_partitioned_dml.call_args_list
        executed_sqls = [c[0][0] for c in calls]

        self.assertTrue(any("DELETE FROM Edge" in sql for sql in executed_sqls))
        self.assertTrue(any("DELETE FROM TimeSeries" in sql for sql in executed_sqls))

        for c in calls:
            self.assertEqual(c[1]["params"], expected_params)

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_aggregated_data_empty(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db")
        deleter.delete_aggregated_data([])

        mock_db.execute_partitioned_dml.assert_not_called()

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_aggregated_data_not_base_dc(self, mock_spanner_client):
        # Setup mocks
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter(
            project_id="proj", instance_id="inst", database_id="db", is_base_dc=False
        )

        imports = ["ImportA"]
        deleter.delete_aggregated_data(imports)

        expected_provenances = ["ImportA"]
        expected_params = {"provenances": expected_provenances}

        calls = mock_db.execute_partitioned_dml.call_args_list
        for c in calls:
            self.assertEqual(c[1]["params"], expected_params)

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_aggregated_data_exception_propagates(self, mock_spanner_client):
        """Verifies that an exception raised in a worker thread is re-raised by delete_aggregated_data."""
        mock_db = MagicMock()
        mock_db.execute_partitioned_dml.side_effect = RuntimeError(
            "Spanner deletion error"
        )
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db")
        with self.assertRaises(RuntimeError):
            deleter.delete_aggregated_data(["ImportA"])

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_provenance_summaries(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter(
            project_id="proj", instance_id="inst", database_id="db", is_base_dc=True
        )

        imports = ["ImportA", "ImportB"]
        deleter.delete_provenance_summaries(imports)

        mock_db.execute_partitioned_dml.assert_called_once()
        call_args = mock_db.execute_partitioned_dml.call_args
        sql = call_args[0][0]
        params = call_args[1]["params"]

        self.assertIn("DELETE FROM KeyValueStore", sql)
        self.assertIn("type = 'ProvenanceSummary'", sql)
        self.assertIn("provenance IN UNNEST(@provenances)", sql)
        self.assertEqual(
            params, {"provenances": ["dc/base/ImportA", "dc/base/ImportB"]}
        )

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_provenance_summaries_not_base_dc(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter(
            project_id="proj", instance_id="inst", database_id="db", is_base_dc=False
        )

        imports = ["ImportA"]
        deleter.delete_provenance_summaries(imports)

        mock_db.execute_partitioned_dml.assert_called_once()
        params = mock_db.execute_partitioned_dml.call_args[1]["params"]
        self.assertEqual(params, {"provenances": ["ImportA"]})

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_provenance_summaries_empty(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db")
        deleter.delete_provenance_summaries([])

        mock_db.execute_partitioned_dml.assert_not_called()

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_provenance_summaries_exception(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_db.execute_partitioned_dml.side_effect = RuntimeError(
            "Spanner deletion error"
        )
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db")
        with self.assertRaises(RuntimeError):
            deleter.delete_provenance_summaries(["ImportA"])

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_stat_var_group_edges(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=True)
        deleter.delete_stat_var_group_edges()

        mock_db.execute_partitioned_dml.assert_called_once()
        call_args = mock_db.execute_partitioned_dml.call_args
        sql = call_args[0][0]
        params = call_args[1]["params"]
        self.assertIn("DELETE FROM Edge", sql)
        self.assertIn("STARTS_WITH(provenance, @prefix)", sql)
        self.assertIn("'memberOf'", sql)
        self.assertIn("'specializationOf'", sql)
        self.assertEqual(params, {"prefix": "dc/base/generated/"})

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_linked_edges(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=True)
        deleter.delete_linked_edges(["ImportA"])

        mock_db.execute_partitioned_dml.assert_called_once()
        call_args = mock_db.execute_partitioned_dml.call_args
        sql = call_args[0][0]
        params = call_args[1]["params"]
        self.assertIn("DELETE FROM Edge", sql)
        self.assertIn("provenance IN UNNEST(@provenances)", sql)
        self.assertEqual(params, {"provenances": ["dc/base/generated/ImportA"]})

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_topic_list_edges_base_dc(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = [("literal_node_1",)]
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=True)
        deleter.delete_topic_list_edges()

        # Edge table deletion via partitioned DML
        mock_db.execute_partitioned_dml.assert_called_once()
        edge_call = mock_db.execute_partitioned_dml.call_args
        edge_sql = edge_call[0][0]
        edge_params = edge_call[1]["params"]
        self.assertIn("DELETE FROM Edge", edge_sql)
        self.assertIn("provenance = @provenance", edge_sql)
        self.assertIn("'relevantVariableList'", edge_sql)
        self.assertIn("'memberList'", edge_sql)
        self.assertEqual(
            edge_params, {"provenance": "dc/base/generated/TopicHierarchyLists"}
        )

        # Node table deletion via KeySet transaction
        mock_db.run_in_transaction.assert_called_once()
        mock_tx = MagicMock()
        mock_db.run_in_transaction.call_args[0][0](mock_tx)
        mock_tx.delete.assert_called_once()
        call_args = mock_tx.delete.call_args[0]
        self.assertEqual(call_args[0], "Node")
        self.assertEqual(call_args[1].keys, [["literal_node_1"]])

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_topic_list_edges_dcp(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = [("literal_node_1",)]
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=False)
        deleter.delete_topic_list_edges()

        mock_db.execute_partitioned_dml.assert_called_once()
        edge_call = mock_db.execute_partitioned_dml.call_args
        edge_sql = edge_call[0][0]
        edge_params = edge_call[1]["params"]
        self.assertIn("DELETE FROM Edge", edge_sql)
        self.assertIn("provenance = @provenance", edge_sql)
        self.assertIn("'relevantVariableList'", edge_sql)
        self.assertIn("'memberList'", edge_sql)
        self.assertEqual(edge_params, {"provenance": "generated/TopicHierarchyLists"})

        mock_db.run_in_transaction.assert_called_once()
        mock_tx = MagicMock()
        mock_db.run_in_transaction.call_args[0][0](mock_tx)
        mock_tx.delete.assert_called_once()
        call_args = mock_tx.delete.call_args[0]
        self.assertEqual(call_args[0], "Node")
        self.assertEqual(call_args[1].keys, [["literal_node_1"]])

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_topic_list_edges_empty_nodes(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = []
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=False)
        deleter.delete_topic_list_edges()

        mock_db.execute_partitioned_dml.assert_called_once()
        mock_db.run_in_transaction.assert_not_called()

    @patch("aggregation.deleter.spanner.Client")
    def test_delete_topic_list_edges_chunked_nodes(self, mock_spanner_client):
        mock_db = MagicMock()
        mock_snapshot = MagicMock()
        mock_snapshot.execute_sql.return_value = [
            (f"literal_node_{i}",) for i in range(7500)
        ]
        mock_db.snapshot.return_value.__enter__.return_value = mock_snapshot
        mock_spanner_client.return_value.instance.return_value.database.return_value = (
            mock_db
        )

        deleter = AggregationDeleter("proj", "inst", "db", is_base_dc=False)
        deleter.delete_topic_list_edges()

        mock_db.execute_partitioned_dml.assert_called_once()
        self.assertEqual(mock_db.run_in_transaction.call_count, 2)


if __name__ == "__main__":
    unittest.main()
