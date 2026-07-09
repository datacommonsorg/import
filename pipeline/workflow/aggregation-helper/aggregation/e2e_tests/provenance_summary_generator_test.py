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
"""Integration E2E tests for Data Commons ProvenanceSummaryGenerator.

Covers:
- ProvenanceSummaryGenerator (generating summary statistics in Cache table for each
  provenance, including min/max values, place type summaries, top places,
  robustness to non-numeric values and dangling observations, and handling multiple imports).

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
    uv run pytest aggregation/e2e_tests/provenance_summary_generator_test.py -s
"""

import unittest
from collections.abc import Mapping
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
    BQ_CONNECTION_ID,
    BQ_LOCATION,
)
from aggregation import BigQueryExecutor, ProvenanceSummaryGenerator


class ProvenanceSummaryGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for ProvenanceSummaryGenerator."""

    def test_provenance_summary_aggregation(self):
        """Tests run_provenance_summary_aggregation.
        
        Setup:
          Nodes: geoId/06 (California, State), geoId/36 (New York, State)
          Edges: typeOf edges
          Observations:
            Count_Person at geoId/06 on 2020 = 100
            Count_Person at geoId/06 on 2021 = 110
            Count_Person at geoId/36 on 2020 = 200
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('geoId/36', 'New York', types=['State'])
        self.add_node('State', 'State Class', types=['Class'])
        
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_edge('geoId/36', 'typeOf', 'State', import_name)
        
        # Add TimeSeries explicitly
        self.add_timeseries('Count_Person', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_timeseries('Count_Person', 'geoId/36', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        # Add observations
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0)
        self.add_observation('Count_Person', 'geoId/06', '2021', 110.0)
        self.add_observation('Count_Person', 'geoId/36', '2020', 200.0)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Provenance Summary Aggregation",
                "type": "PROVENANCE_SUMMARY",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner Cache table
        with self.database.snapshot() as snapshot:
            query = """
                SELECT type, key, provenance, value 
                FROM Cache 
                WHERE type = 'ProvenanceSummary'
            """
            results = list(snapshot.execute_sql(query))
            
            self.assertEqual(len(results), 1)
            row = results[0]
            self.assertEqual(row[0], 'ProvenanceSummary')
            self.assertEqual(row[1], 'Count_Person')
            
            expected_provenance = f'dc/base/{import_name}' if self.is_base_dc else import_name
            self.assertEqual(row[2], expected_provenance)
            
            # Verify JSON value
            value_json = row[3]
            self.assertIsInstance(value_json, Mapping)
            
            self.assertEqual(value_json['import_name'], import_name)
            self.assertEqual(value_json['observation_count'], 3.0)
            self.assertEqual(value_json['time_series_count'], 2.0)
            
            series_summary = value_json['series_summary']
            self.assertEqual(len(series_summary), 1)
            summary = series_summary[0]
            
            self.assertEqual(summary['earliest_date'], '2020')
            self.assertEqual(summary['latest_date'], '2021')
            self.assertEqual(summary['min_value'], 100.0)
            self.assertEqual(summary['max_value'], 200.0)
            self.assertEqual(summary['observation_count'], 3.0)
            self.assertEqual(summary['time_series_count'], 2.0)
            
            series_key = summary['series_key']
            self.assertEqual(series_key['measurement_method'], 'CensusACS5yrSurvey')
            self.assertEqual(series_key['observation_period'], 'P1Y')
            self.assertEqual(series_key['unit'], '1')
            self.assertEqual(series_key['scaling_factor'], '1')
            self.assertEqual(series_key['is_dc_aggregate'], False)
            
            # Place type summary
            pts = summary['place_type_summary']
            self.assertIsNotNone(pts)
            self.assertIn('State', pts)
            state_summary = pts['State']
            self.assertEqual(state_summary['place_count'], 2.0)
            self.assertEqual(state_summary['min_value'], 100.0)
            self.assertEqual(state_summary['max_value'], 200.0)
            
            top_places = state_summary['top_places']
            self.assertEqual(len(top_places), 2)
            self.assertEqual(top_places[0]['dcid'], 'geoId/06')
            self.assertEqual(top_places[0]['name'], 'California')
            self.assertEqual(top_places[1]['dcid'], 'geoId/36')
            self.assertEqual(top_places[1]['name'], 'New York')

    def test_provenance_summary_aggregation_duplicate_types(self):
        """Tests run_provenance_summary_aggregation when entities have duplicate typeOf edges.

        This verifies that duplicate typeOf edges (e.g., from different imports)
        do not cause the observation count to be duplicated in the Cache.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Setup mock data: 1 place with 2 observations
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('State', 'State Class', types=['Class'])
        
        # Add DUPLICATE typeOf edges with different provenances (simulating real DB)
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_edge('geoId/06', 'typeOf', 'State', 'AnotherImport')
        
        # Add TimeSeries and Observations (2 observations total)
        self.add_timeseries('Count_Person', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0)
        self.add_observation('Count_Person', 'geoId/06', '2021', 110.0)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Provenance Summary Duplicate Types",
                "type": "PROVENANCE_SUMMARY",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner Cache table
        with self.database.snapshot() as snapshot:
            query = """
                SELECT type, key, provenance, value 
                FROM Cache 
                WHERE type = 'ProvenanceSummary'
            """
            results = list(snapshot.execute_sql(query))
            
            self.assertEqual(len(results), 1)
            row = results[0]
            value_json = row[3]
            
            # The observation count MUST be 2.0 (the true count), NOT 4.0 (which would happen if duplicated)
            self.assertEqual(value_json['observation_count'], 2.0)
            self.assertEqual(value_json['time_series_count'], 1.0)
            
            series_summary = value_json['series_summary']
            self.assertEqual(len(series_summary), 1)
            summary = series_summary[0]
            
            self.assertEqual(summary['observation_count'], 2.0)
            self.assertEqual(summary['time_series_count'], 1.0)
            
            # Place type summary should also be correct
            pts = summary['place_type_summary']
            self.assertIsNotNone(pts)
            self.assertIn('State', pts)
            state_summary = pts['State']
            self.assertEqual(state_summary['place_count'], 1.0)
            self.assertEqual(state_summary['min_value'], 100.0)
            self.assertEqual(state_summary['max_value'], 110.0)

    def test_provenance_summary_robustness_non_numeric_values(self):
        """Tests run_provenance_summary_aggregation with non-numeric values.
        
        Setup:
          Nodes: geoId/06 (California, State), geoId/36 (New York, State)
          Edges: typeOf edges
          Observations:
            Count_Person at geoId/06 on 2020 = 100.0 (Valid)
            Count_Person at geoId/06 on 2021 = "bad_value" (Malformed)
            Count_Person at geoId/36 on 2020 = 200.0 (Valid)
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('geoId/36', 'New York', types=['State'])
        self.add_node('State', 'State Class', types=['Class'])
        
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_edge('geoId/36', 'typeOf', 'State', import_name)
        
        # Add TimeSeries explicitly
        self.add_timeseries('Count_Person', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_timeseries('Count_Person', 'geoId/36', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        # Add observations
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0)
        # We use a string for the value to simulate malformed data in Spanner
        self.add_observation('Count_Person', 'geoId/06', '2021', 'bad_value')
        self.add_observation('Count_Person', 'geoId/36', '2020', 200.0)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Provenance Summary Non-Numeric Values",
                "type": "PROVENANCE_SUMMARY",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        with self.database.snapshot() as snapshot:
            query = """
                SELECT type, key, provenance, value
                FROM Cache
                WHERE type = 'ProvenanceSummary'
            """
            results = list(snapshot.execute_sql(query))
            
            self.assertEqual(len(results), 1)
            row = results[0]
            value_json = row[3]
            self.assertIsInstance(value_json, Mapping)
            
            # Overall stats
            self.assertEqual(value_json['observation_count'], 3.0) # Counts all 3 rows
            self.assertEqual(value_json['time_series_count'], 2.0)
            
            series_summary = value_json['series_summary']
            self.assertEqual(len(series_summary), 1)
            summary = series_summary[0]
            
            # Min/Max should ignore "bad_value" and use 100 and 200
            self.assertEqual(summary['min_value'], 100.0)
            self.assertEqual(summary['max_value'], 200.0)
            self.assertEqual(summary['observation_count'], 3.0)
            self.assertEqual(summary['time_series_count'], 2.0)
            
            # Place type summary
            pts = summary['place_type_summary']
            self.assertIsNotNone(pts)
            self.assertIn('State', pts)
            state_summary = pts['State']
            self.assertEqual(state_summary['place_count'], 2.0)
            self.assertEqual(state_summary['min_value'], 100.0)
            self.assertEqual(state_summary['max_value'], 200.0)

    def test_provenance_summary_robustness_dangling_observations(self):
        """Tests run_provenance_summary_aggregation with dangling observations (missing metadata).
        
        Setup:
          Observations:
            Count_Person at geoId/06 on 2020 = 100.0 (Normal: has Node and typeOf)
            Count_Person at geoId/99 on 2020 = 150.0 (Dangling 1: has typeOf, but NO Node)
            Count_Person at geoId/88 on 2020 = 250.0 (Dangling 2: has Node, but NO typeOf)
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Normal Node
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        
        # 2. Dangling 1: We only add typeOf edge, NO node
        self.add_edge('geoId/99', 'typeOf', 'State', import_name)
        
        # 3. Dangling 2: We only add node, NO typeOf edge
        self.add_node('geoId/88', 'Unknown Place', types=['State'])
        
        self.add_node('State', 'State Class', types=['Class'])
        
        # Add TimeSeries explicitly
        self.add_timeseries('Count_Person', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_timeseries('Count_Person', 'geoId/99', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_timeseries('Count_Person', 'geoId/88', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        # Add observations for all 3
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0)
        self.add_observation('Count_Person', 'geoId/99', '2020', 150.0)
        self.add_observation('Count_Person', 'geoId/88', '2020', 250.0)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Provenance Summary Dangling Observations",
                "type": "PROVENANCE_SUMMARY",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        with self.database.snapshot() as snapshot:
            query = """
                SELECT type, key, provenance, value
                FROM Cache
                WHERE type = 'ProvenanceSummary'
            """
            results = list(snapshot.execute_sql(query))
            
            self.assertEqual(len(results), 1)
            row = results[0]
            value_json = row[3]
            self.assertIsInstance(value_json, Mapping)
            
            # Overall stats include ALL observations
            self.assertEqual(value_json['observation_count'], 3.0)
            self.assertEqual(value_json['time_series_count'], 3.0)
            
            series_summary = value_json['series_summary']
            self.assertEqual(len(series_summary), 1)
            summary = series_summary[0]
            
            self.assertEqual(summary['min_value'], 100.0)
            self.assertEqual(summary['max_value'], 250.0)
            self.assertEqual(summary['observation_count'], 3.0)
            self.assertEqual(summary['time_series_count'], 3.0)
            
            # Place type summary for State
            # Should ONLY include geoId/06 and geoId/99 (which have typeOf=State)
            # geoId/88 should be excluded because it has no typeOf edge.
            pts = summary['place_type_summary']
            self.assertIsNotNone(pts)
            self.assertIn('State', pts)
            state_summary = pts['State']
            self.assertEqual(state_summary['place_count'], 2.0)
            self.assertEqual(state_summary['min_value'], 100.0)
            self.assertEqual(state_summary['max_value'], 150.0) # geoId/99 is 150, geoId/88 (250) is excluded
            
            top_places = state_summary['top_places']
            self.assertEqual(len(top_places), 2)
            
            # geoId/06 (California)
            self.assertEqual(top_places[0]['dcid'], 'geoId/06')
            self.assertEqual(top_places[0]['name'], 'California')
            
            # geoId/99 (Dangling: name should be None or empty, but must not crash)
            self.assertEqual(top_places[1]['dcid'], 'geoId/99')
            self.assertIsNone(top_places[1]['name']) # BigQuery LEFT JOIN returns NULL for missing name

    def test_provenance_summary_aggregation_multiple_imports(self):
        """Tests run_all with multiple imports at once."""
        import_1 = 'Import_Alpha'
        import_2 = 'Import_Beta'
        
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_1)
        self.add_edge('geoId/06', 'typeOf', 'State', import_2)
        
        self.add_timeseries('Count_Person', 'geoId/06', 'Census', 'P1Y', '1', '1', import_1)
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0, import_name=import_1)
        
        self.add_timeseries('Count_HousingUnit', 'geoId/06', 'Census', 'P1Y', '1', '1', import_2)
        self.add_observation('Count_HousingUnit', 'geoId/06', '2020', 50.0, import_name=import_2)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Provenance Summary Multiple Imports",
                "type": "PROVENANCE_SUMMARY",
                "stage": 1,
                "input_imports": [import_1, import_2]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_1, import_2])
        self.assertTrue(res.success)
        
        with self.database.snapshot() as snapshot:
            query = """
                SELECT type, key, provenance, value
                FROM Cache
                WHERE type = 'ProvenanceSummary'
                ORDER BY provenance, key
            """
            results = list(snapshot.execute_sql(query))
            self.assertEqual(len(results), 2)
            
            prov_1 = f'dc/base/{import_1}' if self.is_base_dc else import_1
            prov_2 = f'dc/base/{import_2}' if self.is_base_dc else import_2
            
            self.assertEqual(results[0][2], prov_1)
            self.assertEqual(results[0][1], 'Count_Person')
            
            self.assertEqual(results[1][2], prov_2)
            self.assertEqual(results[1][1], 'Count_HousingUnit')


class ProvenanceSummaryGeneratorCustomDcTest(ProvenanceSummaryGeneratorIntegrationTest):
    is_base_dc = False
