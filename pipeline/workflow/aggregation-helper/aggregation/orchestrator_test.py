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

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from aggregation import AggregationOrchestrator

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

    def test_run_dry_run_true(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests that run with dry_run=True logs stages without submitting BigQuery jobs."""
        result = self.orchestrator.run(active_imports=["USFed_Census"], dry_run=True)
        self.assertTrue(result.success)
        self.assertIn("USFed_Census", result.import_results)
        self.assertTrue(result.import_results["USFed_Census"].success)

        mock_place_gen.return_value.aggregate_places.assert_not_called()
        mock_sv_agg.return_value.aggregate_stat_vars.assert_not_called()

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

    def test_execute_stage_entity_aggregation(self, mock_entity_gen, mock_calc_gen, mock_sv_agg, mock_place_gen, mock_executor_cls):
        """Tests manual execution of ENTITY_AGGREGATION stage."""
        mock_job = MagicMock()
        mock_job.job_id = "job-entity-1"
        mock_entity_gen.return_value.aggregate_entities.return_value = [mock_job]

        jobs = self.orchestrator.execute_stage(3, ["EarthquakeUSGS"])
        self.assertEqual(jobs, [mock_job])
        mock_entity_gen.return_value.aggregate_entities.assert_called_once()


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
        self.tmpdir = tempfile.TemporaryDirectory()
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

    def tearDown(self):
        self.tmpdir.cleanup()

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
        
        # We assert 2, which should fail currently because call_count will be 1.
        self.assertEqual(mock_place_gen.return_value.aggregate_places.call_count, 2)


if __name__ == '__main__':
    unittest.main()
