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

import json
import os
import sys
import tempfile
import textwrap
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from aggregation import AggregationOrchestrator

VALID_CONFIG_YAML = textwrap.dedent("""\
    aggregations:
      - type: linked_edges
        imports: ["*"]
        stage: 1

      - type: place
        source_type: County
        destination_type: State
        allow_multiple_to_places: false
        imports: ["USFed_Census"]
        stage: 1

      - type: place
        source_type: State
        destination_type: Country
        imports: ["*"]
        stage: 2
        disabled: true

      - type: stat_var
        ancestor_sv_id: Count_Person
        source_sv_ids: ["Count_Person_Male", "Count_Person_Female"]
        skip_all_sources_present_check: true
        imports: ["USFed_Census"]
        stage: 2
""")


@patch('aggregation.orchestrator.BigQueryExecutor')
class TestOrchestratorScanning(unittest.TestCase):
    """Tests the stage scanning and active stage resolution methods."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        config_path = os.path.join(self.tmpdir.name, "aggregation.yaml")
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

    def test_has_stage(self, mock_executor):
        """Tests the has_stage method for active, disabled, and non-matching stages."""
        self.assertTrue(self.orchestrator.has_stage(1, ["AnyImport"]))
        self.assertTrue(self.orchestrator.has_stage(1, ["USFed_Census"]))

        self.assertFalse(self.orchestrator.has_stage(2, ["OtherImport"]))
        self.assertTrue(self.orchestrator.has_stage(2, ["USFed_Census"]))

        self.assertFalse(self.orchestrator.has_stage(3, ["USFed_Census"]))

    def test_get_active_stages(self, mock_executor):
        """Tests that get_active_stages correctly extracts, filters, and sorts active stages."""
        stages = self.orchestrator.get_active_stages(active_imports=["USFed_Census"])
        self.assertEqual(stages, [1, 2])

        stages = self.orchestrator.get_active_stages(active_imports=["OtherImport"])
        self.assertEqual(stages, [1])


@patch('aggregation.orchestrator.BigQueryExecutor')
@patch('aggregation.orchestrator.PlaceAggregationGenerator')
@patch('aggregation.orchestrator.StatVarAggregator')
@patch('aggregation.orchestrator.LinkedEdgeGenerator')
@patch('aggregation.orchestrator.ProvenanceSummaryGenerator')
@patch('aggregation.orchestrator.StatVarGroupGenerator')
class TestOrchestratorExecution(unittest.TestCase):
    """Tests stage execution, verifying parallel job submission and routing.

    These tests execute stages, so they mock the executor and all five generators
    to verify correct parameters are passed and jobs are collected.
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        config_path = os.path.join(self.tmpdir.name, "aggregation.yaml")
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

    def test_execute_stage_1(self, mock_svg_gen, mock_prov_gen, mock_edge_gen, 
                             mock_sv_agg, mock_place_gen, mock_executor):
        """Tests executing Stage 1, verifying parallel job submission and wildcard resolution."""
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-edge-1"
        mock_edge_gen.return_value.run_all.return_value = [mock_job1]

        mock_job2 = MagicMock()
        mock_job2.job_id = "job-place-1"
        mock_place_gen.return_value.aggregate_places.return_value = mock_job2

        job_ids = self.orchestrator.execute_stage(stage_num=1, active_imports=["USFed_Census"])

        self.assertEqual(len(job_ids), 2)
        self.assertIn("job-edge-1", job_ids)
        self.assertIn("job-place-1", job_ids)

        mock_edge_gen.return_value.run_all.assert_called_once_with(["USFed_Census"])
        
        mock_place_gen.return_value.aggregate_places.assert_called_once_with(
            import_names=["USFed_Census"],
            source_type="County",
            destination_type="State",
            allow_multiple_to_places=False
        )

    def test_execute_stage_2_with_disabled_and_filtering(self, mock_svg_gen, mock_prov_gen, mock_edge_gen, 
                                                         mock_sv_agg, mock_place_gen, mock_executor):
        """Tests Stage 2, verifying that disabled steps are skipped and non-matching imports are filtered."""
        mock_job_sv = MagicMock()
        mock_job_sv.job_id = "job-sv-1"
        mock_sv_agg.return_value.aggregate_stat_vars.return_value = [mock_job_sv]

        job_ids = self.orchestrator.execute_stage(stage_num=2, active_imports=["OtherImport"])
        self.assertEqual(len(job_ids), 0)
        mock_place_gen.return_value.aggregate_places.assert_not_called()
        mock_sv_agg.return_value.aggregate_stat_vars.assert_not_called()

        job_ids = self.orchestrator.execute_stage(stage_num=2, active_imports=["USFed_Census"])
        
        self.assertEqual(job_ids, ["job-sv-1"])
        mock_place_gen.return_value.aggregate_places.assert_not_called()
        mock_sv_agg.return_value.aggregate_stat_vars.assert_called_once_with(
            ancestor_sv="Count_Person",
            source_svs=["Count_Person_Male", "Count_Person_Female"],
            import_names=["USFed_Census"],
            output_import_name=None,
            skip_all_sources_present_check=True
        )

    def test_execute_stage_unsupported_type(self, *mocks):
        """Tests that an unsupported aggregation step type raises ValueError."""
        unimplemented_config = textwrap.dedent("""\
            aggregations:
              - type: entity
                entity_types: ["MortalityEvent"]
                location_props: ["location"]
                imports: ["*"]
                stage: 1
        """)
        
        with tempfile.TemporaryDirectory() as local_tmpdir:
            local_config_path = os.path.join(local_tmpdir, "aggregation.yaml")
            with open(local_config_path, "w") as f:
                f.write(unimplemented_config)
            
            local_orchestrator = AggregationOrchestrator(
                connection_id="conn",
                project_id="proj",
                instance_id="inst",
                database_id="db",
                config_file_path=local_config_path
            )
            
            with self.assertRaises(ValueError) as ctx:
                local_orchestrator.execute_stage(stage_num=1, active_imports=["USFed_Census"])
            self.assertIn("Unsupported or unimplemented aggregation step type: entity", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
