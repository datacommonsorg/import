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
"""Integration E2E tests for aggregation deletions in Spanner.

Covers:
- AggregationOrchestrator deletions (verifying that existing aggregated data is
  deleted before running new aggregations, and that the skip_deletions flag
  properly bypasses this behavior).

NOTE: This script is intended for local testing purposes only and is NOT
currently part of the CI pipeline.

WARNING: Running these tests will DELETE all existing data in the target
database tables (Cache, Observation, TimeSeries, Edge, and Node) as part of
the setUp and tearDown phases. Do NOT run this against any database whose
data you do not want to lose (e.g., production, staging, or active development databases)!

Before running this script, you MUST ensure configuration variables (in `base.py`
or set via environment variables such as PROJECT_ID, SPANNER_INSTANCE_ID,
SPANNER_DATABASE_ID, and BQ_CONNECTION_ID) point to your specific test environment.

How to run:
1. Ensure your local environment is authenticated (gcloud auth application-default login).
2. Set the environment variables or verify the config in `aggregation/e2e_tests/base.py`.
3. Run the following command from `import/pipeline/workflow/aggregation-helper`:
    uv run pytest aggregation/e2e_tests/deletions_test.py -s
"""

import unittest
import logging
from google.cloud import spanner
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
)

class AggregationDeletionsIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for verification of aggregation deletions."""

    def test_deletions_default_behavior(self):
        """Tests that existing aggregated data is deleted by default before run."""
        import_name = 'USFed_DeletionsTest'
        output_import_name = f'{import_name}_AggState'
        
        # 1. Setup Place Topology
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_edge('geoId/06075', 'typeOf', 'County', import_name)
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', import_name)
        
        # 2. Setup Raw Data (will be aggregated)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 100.0, import_name=import_name)
        
        # 3. Setup Stale Aggregated Data (should be deleted)
        # We add an observation for a dummy place 'geoId/99' that is NOT in the raw data
        # under the output import provenance.
        self.add_observation('Count_Person', 'geoId/99', '2010', 500.0, import_name=output_import_name)
        # Add a stale edge under output import provenance
        self.add_edge('geoId/99', 'typeOf', 'State', output_import_name)
        
        self.flush_to_spanner()
        
        # Verify stale data exists before run
        with self.database.snapshot(multi_use=True) as snapshot:
            stale_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/99' AND variable_measured = 'Count_Person'"
            ))
            self.assertEqual(len(stale_obs), 1)
            self.assertEqual(float(stale_obs[0][0]), 500.0)
            
            stale_edge = list(snapshot.execute_sql(
                "SELECT object_id FROM Edge WHERE subject_id = 'geoId/99' AND predicate = 'typeOf'"
            ))
            self.assertEqual(len(stale_edge), 1)

        # 4. Run Orchestrator (skip_deletions=False by default)
        calculations = [
            {
                "name": "Deletions Test Rollup",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name], skip_deletions=False)
        self.assertTrue(res.success)
        
        # 5. Verify Spanner State after run
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. New aggregated data SHOULD exist (geoId/06, value 100)
            new_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND variable_measured = 'Count_Person' AND date = '2020'"
            ))
            self.assertEqual(len(new_obs), 1)
            self.assertEqual(float(new_obs[0][0]), 100.0)
            
            # B. Stale aggregated data SHOULD BE GONE (deleted by deleter)
            stale_obs_after = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/99'"
            ))
            self.assertEqual(len(stale_obs_after), 0)
            
            stale_edge_after = list(snapshot.execute_sql(
                "SELECT object_id FROM Edge WHERE subject_id = 'geoId/99'"
            ))
            self.assertEqual(len(stale_edge_after), 0)
            
            # C. Raw data SHOULD STILL EXIST
            raw_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06075' AND variable_measured = 'Count_Person'"
            ))
            self.assertEqual(len(raw_obs), 1)
            self.assertEqual(float(raw_obs[0][0]), 100.0)

    def test_deletions_skip_behavior(self):
        """Tests that existing aggregated data is NOT deleted when skip_deletions=True."""
        import_name = 'USFed_DeletionsSkipTest'
        output_import_name = f'{import_name}_AggState'
        
        # 1. Setup Place Topology
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_edge('geoId/06075', 'typeOf', 'County', import_name)
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', import_name)
        
        # 2. Setup Raw Data
        self.add_observation('Count_Person', 'geoId/06075', '2020', 100.0, import_name=import_name)
        
        # 3. Setup Stale Aggregated Data
        self.add_observation('Count_Person', 'geoId/99', '2010', 500.0, import_name=output_import_name)
        self.add_edge('geoId/99', 'typeOf', 'State', output_import_name)
        
        self.flush_to_spanner()
        
        # 4. Run Orchestrator (skip_deletions=True)
        calculations = [
            {
                "name": "Deletions Skip Test Rollup",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name], skip_deletions=True)
        self.assertTrue(res.success)
        
        # 5. Verify Spanner State after run
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. New aggregated data SHOULD exist
            new_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND variable_measured = 'Count_Person' AND date = '2020'"
            ))
            self.assertEqual(len(new_obs), 1)
            self.assertEqual(float(new_obs[0][0]), 100.0)
            
            # B. Stale aggregated data SHOULD STILL EXIST (skipped deletion)
            stale_obs_after = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/99'"
            ))
            self.assertEqual(len(stale_obs_after), 1)
            self.assertEqual(float(stale_obs_after[0][0]), 500.0)
            
            stale_edge_after = list(snapshot.execute_sql(
                "SELECT object_id FROM Edge WHERE subject_id = 'geoId/99'"
            ))
            self.assertEqual(len(stale_edge_after), 1)

    def test_deletions_scoped(self):
        """Tests that deletions only target output imports of active/expanded imports."""
        import_a = 'USFed_DeletionsTestA'
        output_a = f'{import_a}_AggState'
        import_b = 'USFed_DeletionsTestB'
        output_b = f'{import_b}_AggState'
        
        # 1. Setup Place Topology (using a shared topology import name to avoid duplicate edges)
        topo_import = 'USGeo_Topology'
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', topo_import)
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_edge('geoId/06075', 'typeOf', 'County', topo_import)
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', topo_import)
        
        # 2. Setup Raw Data for both A and B (using distinct facet_ids to avoid key conflicts)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 100.0, import_name=import_a, facet_id='facet_a')
        self.add_observation('Count_Person', 'geoId/06075', '2020', 200.0, import_name=import_b, facet_id='facet_b')
        
        # 3. Setup Stale Aggregated Data for both A and B
        self.add_observation('Count_Person', 'geoId/99', '2010', 500.0, import_name=output_a, facet_id='facet_out_a')
        self.add_edge('geoId/99', 'typeOf', 'State', output_a)
        
        self.add_observation('Count_Person', 'geoId/88', '2010', 600.0, import_name=output_b, facet_id='facet_out_b')
        self.add_edge('geoId/88', 'typeOf', 'State', output_b)
        
        self.flush_to_spanner()
        
        # 4. Run Orchestrator with ONLY import_a active
        calculations = [
            {
                "name": "Rollup A",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_a],
                "output_import": output_a,
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            },
            {
                "name": "Rollup B",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_b],
                "output_import": output_b,
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            }
        ]
        
        # Run only for import_a
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_a], skip_deletions=False)
        self.assertTrue(res.success)
        
        # 5. Verify Spanner State
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. New aggregated data for A SHOULD exist
            obs_a = list(snapshot.execute_sql(
                f"SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND variable_measured = 'Count_Person' AND facet_id IN (SELECT facet_id FROM TimeSeries WHERE provenance = 'dc/base/{output_a}')"
            ))
            self.assertEqual(len(obs_a), 1)
            self.assertEqual(float(obs_a[0][0]), 100.0)
            
            # B. Stale aggregated data for A SHOULD BE GONE
            stale_obs_a = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/99'"
            ))
            self.assertEqual(len(stale_obs_a), 0)
            
            # C. Stale aggregated data for B SHOULD STILL EXIST (since import_b was not active)
            stale_obs_b = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/88'"
            ))
            self.assertEqual(len(stale_obs_b), 1)
            self.assertEqual(float(stale_obs_b[0][0]), 600.0)
            
            # D. New aggregated data for B SHOULD NOT exist
            new_obs_b = list(snapshot.execute_sql(
                f"SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND variable_measured = 'Count_Person' AND facet_id IN (SELECT facet_id FROM TimeSeries WHERE provenance = 'dc/base/{output_b}')"
            ))
            self.assertEqual(len(new_obs_b), 0)

if __name__ == '__main__':
    unittest.main()
