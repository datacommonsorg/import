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

"""Unit tests for the AggregationOrchestrator class."""

import os
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import MagicMock, patch

from aggregation.common import CALCULATION_TYPE_PRIORITY
from aggregation.orchestrator import AggregationOrchestrator, CalculationType


VALID_CONFIG_YAML = textwrap.dedent("""\
    calculations:
      - type: PLACE_AGGREGATION
        input_imports:
          - USFed_Census
        output_import: USFed_Census_AggState
        stage: 1
        place_aggregation:
          from_place_types: County
          to_place_types: State

      - type: STAT_VAR_AGGREGATION
        input_imports:
          - USFed_Census
        output_import: USFed_Census_StatVarAgg
        stage: 2
        stat_var_aggregation:
          aggregations:
            - ancestor_sv_id: Count_Person
              source_sv_ids:
                - Count_Person_Male
                - Count_Person_Female
              skip_all_sources_present_check: true

      - type: ENTITY_AGGREGATION
        input_imports:
          - EarthquakeUSGS
        output_import: EarthquakeUSGS_Agg
        stage: 3
        entity_aggregation:
          entity_types:
            - EarthquakeEvent
          location_props:
            - affectedPlace
          date_prop: occurrenceTime
          agg_date_formats:
            - YYYY
          constraints:
            - property: magnitude
              min: 7
              unit: M
""")


@patch('aggregation.orchestrator.BigQueryExecutor')
class TestOrchestratorScanning(unittest.TestCase):
    """Tests stage scanning and active stage resolution methods."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        config_path = os.path.join(self.tmpdir.name, "config.yaml")
        with open(config_path, "w") as f:
            f.write(VALID_CONFIG_YAML)

        self.orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=config_path
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_get_active_stages(self, mock_executor):
        """Tests getting active stages for matching and non-matching imports."""
        stages = self.orchestrator.get_active_stages(["USFed_Census"])
        self.assertEqual(stages, [1, 2])

        stages = self.orchestrator.get_active_stages(["OtherImport"])
        self.assertEqual(stages, [])

    def test_directory_config_loading(self, mock_executor):
        """Tests that orchestrator correctly scans and loads config files from a directory."""
        dir_orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_dir=self.tmpdir.name
        )
        self.assertEqual(len(dir_orchestrator.calculations), 3)

    def test_init_options(self, mock_executor):
        """Tests that run_sequential and poll_interval parameters are properly set and passed."""
        custom_orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_dir=self.tmpdir.name,
            run_sequential=True,
            poll_interval=5
        )
        self.assertEqual(custom_orchestrator.poll_interval, 5)
        self.assertTrue(custom_orchestrator.executor.run_sequential)


@patch('aggregation.orchestrator.BigQueryExecutor')
@patch('aggregation.orchestrator.PlaceAggregationGenerator')
@patch('aggregation.orchestrator.StatVarAggregator')
@patch('aggregation.orchestrator.StatVarCalculationGenerator')
@patch('aggregation.orchestrator.EntityAggregationGenerator')
class TestOrchestratorExecution(unittest.TestCase):
    """Tests stage execution, verifying job submission and synchronization."""

    def setUp(self):
        self.deleter_patcher = patch('aggregation.orchestrator.AggregationDeleter')
        self.mock_deleter = self.deleter_patcher.start()
        self.addCleanup(self.deleter_patcher.stop)

        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        config_path = os.path.join(self.tmpdir.name, "config.yaml")
        with open(config_path, "w") as f:
            f.write(VALID_CONFIG_YAML)

        self.orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=config_path
        )

    def test_run_dry_run_true(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests that run with dry_run=True logs stages without submitting BigQuery jobs."""
        result = self.orchestrator.run(active_imports=["USFed_Census"], dry_run=True)
        self.assertTrue(result.success)
        self.assertIn("USFed_Census", result.import_results)
        self.assertTrue(result.import_results["USFed_Census"].success)

        mock_place_gen.return_value.aggregate_places.assert_not_called()
        mock_sv_agg.return_value.aggregate_stat_vars.assert_not_called()
        self.mock_deleter.return_value.delete_aggregated_data.assert_not_called()

    def test_run_dry_run_false(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests that run with dry_run=False submits BigQuery jobs across stages."""
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-place-1"
        mock_place_gen.return_value.aggregate_places.return_value = mock_job1

        mock_job2 = MagicMock()
        mock_job2.job_id = "job-sv-1"
        mock_sv_agg.return_value.aggregate_stat_vars.return_value = [mock_job2]

        self.orchestrator.executor = MagicMock()
        self.orchestrator.executor.get_jobs_status.return_value = {"status": "DONE"}

        result = self.orchestrator.run(active_imports=["USFed_Census"], dry_run=False)
        self.assertTrue(result.success)
        self.assertIn("USFed_Census", result.import_results)
        self.assertTrue(result.import_results["USFed_Census"].success)

        mock_place_gen.return_value.aggregate_places.assert_called_once_with(
            import_names=["USFed_Census"],
            source_type="County",
            destination_type="State",
            allow_multiple_to_places=False
        )

        mock_sv_agg.return_value.aggregate_stat_vars.assert_called_once_with(
            ancestor_sv="Count_Person",
            source_svs=["Count_Person_Male", "Count_Person_Female"],
            import_names=["USFed_Census"],
            output_import_name="USFed_Census_StatVarAgg",
            skip_all_sources_present_check=True
        )

        # Verify deleter was called with expected outputs
        self.mock_deleter.return_value.delete_aggregated_data.assert_called_once_with(
            ["USFed_Census_AggState", "USFed_Census_StatVarAgg"]
        )

    def test_run_skip_deletions(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests that run with skip_deletions=True does not call deleter."""
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-place-1"
        mock_place_gen.return_value.aggregate_places.return_value = mock_job1

        mock_job2 = MagicMock()
        mock_job2.job_id = "job-sv-1"
        mock_sv_agg.return_value.aggregate_stat_vars.return_value = [mock_job2]

        self.orchestrator.executor = MagicMock()
        self.orchestrator.executor.get_jobs_status.return_value = {"status": "DONE"}

        result = self.orchestrator.run(active_imports=["USFed_Census"], dry_run=False, skip_deletions=True)
        self.assertTrue(result.success)

        # Verify deleter was NOT called
        self.mock_deleter.return_value.delete_aggregated_data.assert_not_called()

    def test_execute_stage(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests manual execution of a specific stage."""
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-place-1"
        mock_place_gen.return_value.aggregate_places.return_value = mock_job1

        jobs = self.orchestrator.execute_stage(1, ["USFed_Census"])

        mock_place_gen.return_value.aggregate_places.assert_called_once_with(
            import_names=["USFed_Census"],
            source_type="County",
            destination_type="State",
            allow_multiple_to_places=False
        )
        self.assertEqual(jobs, [mock_job1])
        self.mock_deleter.return_value.delete_aggregated_data.assert_not_called()

    def test_execute_stage_entity_aggregation(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests manual execution of ENTITY_AGGREGATION stage."""
        mock_job = MagicMock()
        mock_job.job_id = "job-entity-1"
        mock_entity_gen.return_value.aggregate_entities.return_value = [mock_job]

        jobs = self.orchestrator.execute_stage(3, ["EarthquakeUSGS"])
        self.assertEqual(jobs, [mock_job])
        mock_entity_gen.return_value.aggregate_entities.assert_called_once()
        self.mock_deleter.return_value.delete_aggregated_data.assert_not_called()


CHAINED_CONFIG_YAML = textwrap.dedent("""\
    calculations:
      - type: PLACE_AGGREGATION
        input_imports:
          - USFed_Census
        output_import: USFed_Census_AggState
        stage: 1
        place_aggregation:
          from_place_types: County
          to_place_types: State

      - type: PLACE_AGGREGATION
        input_imports:
          - USFed_Census_AggState
        output_import: USFed_Census_AggState_AggCountry
        stage: 2
        place_aggregation:
          from_place_types: State
          to_place_types: Country
""")


@patch('aggregation.orchestrator.BigQueryExecutor')
@patch('aggregation.orchestrator.PlaceAggregationGenerator')
class TestOrchestratorChainedExecution(unittest.TestCase):
    """Tests chained stage execution, verifying job submission and synchronization."""

    def setUp(self):
        self.deleter_patcher = patch('aggregation.orchestrator.AggregationDeleter')
        self.mock_deleter = self.deleter_patcher.start()
        self.addCleanup(self.deleter_patcher.stop)

        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        config_path = os.path.join(self.tmpdir.name, "config.yaml")
        with open(config_path, "w") as f:
            f.write(CHAINED_CONFIG_YAML)

        self.orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=config_path
        )

    def test_run_chained_dry_run_false(self, mock_place_gen, mock_executor_cls):
        """Tests that run with dry_run=False submits BigQuery jobs across chained stages."""
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-place-1"
        
        mock_job2 = MagicMock()
        mock_job2.job_id = "job-place-2"
        
        def aggregate_places_side_effect(import_names, source_type, destination_type, allow_multiple_to_places=False):
            if import_names == ["USFed_Census"]:
                return mock_job1
            elif import_names == ["USFed_Census_AggState"]:
                return mock_job2
            return None
            
        mock_place_gen.return_value.aggregate_places.side_effect = aggregate_places_side_effect

        self.orchestrator.executor = MagicMock()
        self.orchestrator.executor.get_jobs_status.return_value = {"status": "DONE"}

        # We run with ONLY 'USFed_Census' active.
        result = self.orchestrator.run(active_imports=["USFed_Census"], dry_run=False)
        self.assertTrue(result.success)
        
        # Verify that both stage 1 and stage 2 calculations are triggered (call_count = 2).
        self.assertEqual(mock_place_gen.return_value.aggregate_places.call_count, 2)

        # Verify deleter was called with all expected outputs in the chain
        self.mock_deleter.return_value.delete_aggregated_data.assert_called_once_with(
            ["USFed_Census_AggState", "USFed_Census_AggState_AggCountry"]
        )


GLOBAL_CALCS_CONFIG_YAML = textwrap.dedent("""\
    calculations:
      - type: EMBEDDING_GENERATION
        embedding_generation:
          specs: []
""")


@patch('aggregation.orchestrator.BigQueryExecutor')
@patch('aggregation.orchestrator.EmbeddingGenerator')
class TestOrchestratorGlobalCalculations(unittest.TestCase):

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.config_path = os.path.join(self.tmpdir.name, "config.yaml")
        with open(self.config_path, "w") as f:
            f.write(GLOBAL_CALCS_CONFIG_YAML)

    def test_run_global_calcs_dry_run(self, mock_embedding_gen, mock_executor_cls):
        orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=self.config_path,
            enable_embeddings=True,
            embedding_conn_id="test-conn-id",
            bq_dataset_id="test-dataset"
        )
        result = orchestrator.run(active_imports=[], dry_run=True)
        self.assertTrue(result.success)
        self.assertIn("GLOBAL", result.import_results)
        self.assertTrue(result.import_results["GLOBAL"].success)
        mock_embedding_gen.return_value.run_all.assert_not_called()

    def test_run_global_calcs_success(self, mock_embedding_gen, mock_executor_cls):
        orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=self.config_path,
            enable_embeddings=True,
            embedding_conn_id="test-conn-id",
            bq_dataset_id="test-dataset"
        )
        mock_job = MagicMock()
        mock_job.job_id = "global-job-1"
        mock_embedding_gen.return_value.run_all.return_value = [mock_job]

        orchestrator.executor = MagicMock()
        orchestrator.executor.get_jobs_status.return_value = {"status": "DONE"}

        result = orchestrator.run(active_imports=[], dry_run=False)
        self.assertTrue(result.success)
        self.assertIn("GLOBAL", result.import_results)
        self.assertTrue(result.import_results["GLOBAL"].success)
        mock_embedding_gen.return_value.run_all.assert_called_once()
        mock_executor_cls.assert_called_once_with(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            location=None,
            run_sequential=False,
            enable_embeddings=True,
            embedding_conn_id="test-conn-id",
            bq_dataset_id="test-dataset"
        )

    def test_run_global_calcs_failure(self, mock_embedding_gen, mock_executor_cls):
        orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=self.config_path,
            enable_embeddings=True,
            embedding_conn_id="test-conn-id",
            bq_dataset_id="test-dataset"
        )
        mock_embedding_gen.return_value.run_all.side_effect = Exception("Vertex AI quota exceeded")

        result = orchestrator.run(active_imports=[], dry_run=False)
        self.assertFalse(result.success)
        self.assertIn("GLOBAL", result.import_results)
        self.assertFalse(result.import_results["GLOBAL"].success)
        self.assertEqual(result.import_results["GLOBAL"].error_message, "Vertex AI quota exceeded")


ORDERING_CONFIG_YAML = textwrap.dedent("""\
    calculations:
      - type: PLACE_AGGREGATION
        input_imports: ["TestImport"]
        output_import: "TestImport_Agg"
        stage: 1
        place_aggregation:
          from_place_types: County
          to_place_types: State

      - type: PROVENANCE_SUMMARY
        input_imports: ["*"]
        stage: 0

      - type: LINKED_EDGES
        input_imports: ["*"]
        stage: 0

      - type: STAT_VAR_GROUPS
        input_imports: ["*"]
        stage: 0
""")


@patch('aggregation.orchestrator.BigQueryExecutor')
class TestOrchestratorOrdering(unittest.TestCase):
    """Tests for stage 0 prerequisite resolution and deterministic priority ordering."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)

        self.config_path = os.path.join(self.tmpdir.name, "ordering_config.yaml")
        with open(self.config_path, "w") as f:
            f.write(ORDERING_CONFIG_YAML)

        self.orchestrator = AggregationOrchestrator(
            connection_id="conn",
            project_id="proj",
            instance_id="inst",
            database_id="db",
            config_file_path=self.config_path
        )

    def test_active_stages_includes_stage_0(self, mock_executor):
        """Verifies stage 0 is included in active stages for matching imports."""
        active_stages = self.orchestrator.get_active_stages(["TestImport"])
        self.assertEqual(active_stages, [0, 1])

    def test_calculation_priority_sorting(self, mock_executor):
        """Verifies calculations are sorted deterministically by (stage, priority)."""
        types_in_order = [calc["type"] for calc in self.orchestrator.calculations]
        expected_order = [
            "LINKED_EDGES",
            "STAT_VAR_GROUPS",
            "PROVENANCE_SUMMARY",
            "PLACE_AGGREGATION"
        ]
        self.assertEqual(types_in_order, expected_order)


class TestConfigSanity(unittest.TestCase):
    """Sanity checks for calculation configurations and metadata."""

    def test_all_calculation_types_have_priority(self):
        """Ensures all CalculationType enum members have a priority defined in CALCULATION_TYPE_PRIORITY."""
        calc_type_values = {t.value for t in CalculationType}
        priority_keys = set(CALCULATION_TYPE_PRIORITY.keys())
        self.assertSetEqual(
            calc_type_values,
            priority_keys,
            f"Mismatch between CalculationType and CALCULATION_TYPE_PRIORITY. "
            f"Missing: {calc_type_values ^ priority_keys}"
        )


if __name__ == '__main__':
    unittest.main()


