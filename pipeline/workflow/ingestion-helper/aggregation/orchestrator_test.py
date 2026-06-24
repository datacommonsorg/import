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

import json
import os
import sys
import unittest
from unittest.mock import MagicMock, patch, mock_open

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from aggregation import AggregationOrchestrator


# Sample valid YAML config for testing
VALID_CONFIG_YAML = """
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
"""


@patch('aggregation.orchestrator.BigQueryExecutor')
@patch('aggregation.orchestrator.PlaceAggregationGenerator')
@patch('aggregation.orchestrator.StatVarAggregator')
@patch('aggregation.orchestrator.LinkedEdgeGenerator')
@patch('aggregation.orchestrator.ProvenanceSummaryGenerator')
@patch('aggregation.orchestrator.StatVarGroupGenerator')
class TestAggregationOrchestrator(unittest.TestCase):

    def setUp(self):
        # Load the actual schema for validation tests (now inside aggregation/)
        schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        with open(schema_path, "r") as f:
            self.schema_json = json.load(f)

    def _get_mock_open(self, yaml_content):
        """Helper to mock open() calls for both the config YAML and the schema JSON."""
        def side_effect(path, *args, **kwargs):
            if "schema.json" in path:
                return mock_open(read_data=json.dumps(self.schema_json))().__enter__()
            elif "aggregation.yaml" in path:
                return mock_open(read_data=yaml_content)().__enter__()
            raise FileNotFoundError(f"Mock open not configured for: {path}")
        return side_effect



    @patch('builtins.open')
    def test_has_stage(self, mock_file_open, *mocks):
        """Tests the has_stage method for active, disabled, and non-matching stages."""
        mock_file_open.side_effect = self._get_mock_open(VALID_CONFIG_YAML)
        utils = AggregationOrchestrator(connection_id="conn", project_id="proj", instance_id="inst", database_id="db")

        # Stage 1: Has linked_edges (wildcard) and place (USFed_Census)
        self.assertTrue(utils.has_stage(1, ["AnyImport"]))
        self.assertTrue(utils.has_stage(1, ["USFed_Census"]))

        # Stage 2: Has place (disabled: true) and stat_var (USFed_Census)
        # If active import is "OtherImport", Stage 2 has no active aggregations (stat_var doesn't match, place is disabled)
        self.assertFalse(utils.has_stage(2, ["OtherImport"]))
        # If active import is "USFed_Census", Stage 2 has stat_var active
        self.assertTrue(utils.has_stage(2, ["USFed_Census"]))

        # Stage 3: Does not exist in config
        self.assertFalse(utils.has_stage(3, ["USFed_Census"]))

    @patch('builtins.open')
    def test_get_active_stages(self, mock_file_open, *mocks):
        """Tests that get_active_stages correctly extracts, filters, and sorts active stages."""
        mock_file_open.side_effect = self._get_mock_open(VALID_CONFIG_YAML)

        utils = AggregationOrchestrator(connection_id="conn", project_id="proj", instance_id="inst", database_id="db")

        # 1. For active import "USFed_Census":
        # Stage 1 (linked_edges, place) and Stage 2 (stat_var) have active steps.
        # The place rollup in Stage 2 is disabled, but the stat_var step is enabled and active.
        # Therefore, active stages should be [1, 2].
        stages = utils.get_active_stages(active_imports=["USFed_Census"])
        self.assertEqual(stages, [1, 2])

        # 2. For active import "OtherImport":
        # Stage 1 (linked_edges) matches via wildcard.
        # Stage 2 (place rollup is disabled, stat_var does not match "OtherImport").
        # Therefore, only Stage 1 is active. Active stages should be [1].
        stages = utils.get_active_stages(active_imports=["OtherImport"])
        self.assertEqual(stages, [1])

    @patch('builtins.open')
    def test_execute_stage_1(self, mock_file_open, 
                             mock_svg_gen, mock_prov_gen, mock_edge_gen, 
                             mock_sv_agg, mock_place_gen, mock_executor):
        """Tests executing Stage 1, verifying parallel job submission and wildcard resolution."""
        mock_file_open.side_effect = self._get_mock_open(VALID_CONFIG_YAML)
        
        # Setup generator mocks to return mock jobs
        mock_job1 = MagicMock()
        mock_job1.job_id = "job-edge-1"
        mock_edge_gen.return_value.run_all.return_value = [mock_job1]

        mock_job2 = MagicMock()
        mock_job2.job_id = "job-place-1"
        mock_place_gen.return_value.aggregate_places.return_value = mock_job2

        utils = AggregationOrchestrator(connection_id="conn", project_id="proj", instance_id="inst", database_id="db")

        # Execute Stage 1 for active imports: ["USFed_Census"]
        job_ids = utils.execute_stage(stage_num=1, active_imports=["USFed_Census"])

        # Assertions
        self.assertEqual(len(job_ids), 2)
        self.assertIn("job-edge-1", job_ids)
        self.assertIn("job-place-1", job_ids)

        # Verify linked_edges ran for ALL active imports (wildcard '*')
        mock_edge_gen.return_value.run_all.assert_called_once_with(["USFed_Census"])
        
        # Verify place rollup ran for matching import "USFed_Census"
        mock_place_gen.return_value.aggregate_places.assert_called_once_with(
            import_names=["USFed_Census"],
            source_type="County",
            destination_type="State",
            allow_multiple_to_places=False
        )

    @patch('builtins.open')
    def test_execute_stage_2_with_disabled_and_filtering(self, mock_file_open, 
                                                         mock_svg_gen, mock_prov_gen, mock_edge_gen, 
                                                         mock_sv_agg, mock_place_gen, mock_executor):
        """Tests Stage 2, verifying that disabled steps are skipped and non-matching imports are filtered."""
        mock_file_open.side_effect = self._get_mock_open(VALID_CONFIG_YAML)

        # Setup mock for Stage 2 stat_var job
        mock_job_sv = MagicMock()
        mock_job_sv.job_id = "job-sv-1"
        mock_sv_agg.return_value.aggregate_stat_vars.return_value = [mock_job_sv]

        utils = AggregationOrchestrator(connection_id="conn", project_id="proj", instance_id="inst", database_id="db")

        # 1. Run for an import that does NOT match stat_var ("OtherImport")
        # Since the place rollup in Stage 2 is disabled, no jobs should run at all.
        job_ids = utils.execute_stage(stage_num=2, active_imports=["OtherImport"])
        self.assertEqual(len(job_ids), 0)
        mock_place_gen.return_value.aggregate_places.assert_not_called()
        mock_sv_agg.return_value.aggregate_stat_vars.assert_not_called()

        # 2. Run for matching import "USFed_Census"
        # The disabled place rollup should still be skipped, but the stat_var aggregation should execute.
        job_ids = utils.execute_stage(stage_num=2, active_imports=["USFed_Census"])
        
        self.assertEqual(job_ids, ["job-sv-1"])
        mock_place_gen.return_value.aggregate_places.assert_not_called() # Still skipped because disabled: true
        mock_sv_agg.return_value.aggregate_stat_vars.assert_called_once_with(
            ancestor_sv="Count_Person",
            source_svs=["Count_Person_Male", "Count_Person_Female"],
            import_names=["USFed_Census"],
            output_import_name=None,
            skip_all_sources_present_check=True
        )

    @patch('builtins.open')
    def test_execute_stage_unsupported_type(self, mock_file_open, *mocks):
        """Tests that an unsupported aggregation step type raises ValueError."""
        # Use 'entity' which is valid in schema but unimplemented in orchestrator
        unimplemented_config = """
        aggregations:
          - type: entity
            entity_types: ["MortalityEvent"]
            location_props: ["location"]
            imports: ["*"]
            stage: 1
        """
        mock_file_open.side_effect = self._get_mock_open(unimplemented_config)

        utils = AggregationOrchestrator(connection_id="conn", project_id="proj", instance_id="inst", database_id="db")

        # Running Stage 1 should raise ValueError due to unimplemented "entity" type
        with self.assertRaises(ValueError) as ctx:
            utils.execute_stage(stage_num=1, active_imports=["USFed_Census"])
        self.assertIn("Unsupported or unimplemented aggregation step type: entity", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
