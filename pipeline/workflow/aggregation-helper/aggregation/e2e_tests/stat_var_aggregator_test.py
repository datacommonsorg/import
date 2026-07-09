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
"""Integration E2E tests for Data Commons StatVarAggregator.

Covers:
- StatVarAggregator (aggregating raw statistical variables into a summarized
  ancestor statistical variable across cohorts/facets, strict vs. lenient mode
  for missing source variables, multiple cohorts, and handling null/empty
  measurementMethod values).

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
    uv run pytest aggregation/e2e_tests/stat_var_aggregator_test.py -s
"""

import unittest
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
    BQ_CONNECTION_ID,
    BQ_LOCATION,
)
from aggregation import BigQueryExecutor, StatVarAggregator


class StatVarAggregatorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarAggregator."""

    def test_aggregate_stat_vars_success(self):
        """Tests successful aggregation when all sources are present."""
        import_name = 'CensusACS5YearSurvey_Test'
        output_import_name = f'{import_name}_StatVarAgg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"
        
        # 1. Setup mock data
        self.add_timeseries('SV_A', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_timeseries('SV_B', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, import_name=import_name)
        self.add_observation('SV_B', 'geoId/06', '2020', 20.0, import_name=import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "StatVar Aggregation Success",
                "type": "STAT_VAR_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_aggregation": {
                    "aggregations": [
                        {
                            "ancestor_sv_id": "SV_Parent",
                            "source_sv_ids": ["SV_A", "SV_B"],
                            "skip_all_sources_present_check": False
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner (using multi_use=True to allow multiple queries)
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify TimeSeries
            ts_query = """
                SELECT variable_measured, extra_entities_id, facet_id, facet, provenance
                FROM TimeSeries
                WHERE variable_measured = 'SV_Parent'
            """
            ts_results = list(snapshot.execute_sql(ts_query))
            self.assertEqual(len(ts_results), 1)
            ts_row = ts_results[0]
            self.assertEqual(ts_row[0], 'SV_Parent')
            self.assertEqual(ts_row[1], '')
            
            # Verify facet JSON has updated measurementMethod and provenance
            # Spanner client automatically parses JSON columns into dicts
            facet_json = ts_row[3]
            self.assertEqual(facet_json['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            self.assertEqual(facet_json['provenance'], expected_provenance)
            self.assertEqual(facet_json['isDcAggregate'], True)
            
            # Verify stored provenance column
            self.assertEqual(ts_row[4], expected_provenance)
            
            # Verify Observation
            obs_query = """
                SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value
                FROM Observation
                WHERE variable_measured = 'SV_Parent'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            obs_row = obs_results[0]
            self.assertEqual(obs_row[0], 'SV_Parent')
            self.assertEqual(obs_row[1], 'geoId/06')
            self.assertEqual(obs_row[2], '')
            self.assertEqual(obs_row[3], ts_row[2]) # facet_id should match
            self.assertEqual(obs_row[4], '2020')
            self.assertEqual(float(obs_row[5]), 30.0) # Compare as float

    def test_aggregate_stat_vars_missing_source_strict(self):
        """Tests that aggregation is skipped if a source is missing in strict mode."""
        import_name = 'CensusACS5YearSurvey_Test'
        output_import_name = f'{import_name}_StatVarAgg'
        
        # 1. Setup mock data: SV_B is missing
        self.add_timeseries('SV_A', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, import_name=import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "StatVar Aggregation Strict Missing",
                "type": "STAT_VAR_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_aggregation": {
                    "aggregations": [
                        {
                            "ancestor_sv_id": "SV_Parent",
                            "source_sv_ids": ["SV_A", "SV_B"],
                            "skip_all_sources_present_check": False
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify no observations are created
        with self.database.snapshot() as snapshot:
            obs_query = """
                SELECT COUNT(*)
                FROM Observation
                WHERE variable_measured = 'SV_Parent'
            """
            count = list(snapshot.execute_sql(obs_query))[0][0]
            self.assertEqual(count, 0)

    def test_aggregate_stat_vars_missing_source_lenient(self):
        """Tests that aggregation is performed even if a source is missing in lenient mode."""
        import_name = 'CensusACS5YearSurvey_Test'
        output_import_name = f'{import_name}_StatVarAgg'
        
        # 1. Setup mock data: SV_B is missing
        self.add_timeseries('SV_A', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, import_name=import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "StatVar Aggregation Lenient Missing",
                "type": "STAT_VAR_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_aggregation": {
                    "aggregations": [
                        {
                            "ancestor_sv_id": "SV_Parent",
                            "source_sv_ids": ["SV_A", "SV_B"],
                            "skip_all_sources_present_check": True
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify observation is created with SV_A's value
        with self.database.snapshot() as snapshot:
            obs_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'SV_Parent'
            """
            results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(results), 1)
            self.assertEqual(float(results[0][0]), 10.0) # Compare as float

    def test_aggregate_stat_vars_multiple_cohorts(self):
        """Tests that different facets are aggregated separately."""
        import_name = 'CensusACS5YearSurvey_Test'
        output_import_name = f'{import_name}_StatVarAgg'
        
        # 1. Setup mock data:
        # Cohort 1 (Census): SV_A = 10, SV_B = 20
        self.add_timeseries('SV_A', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name, facet_id='facet_census')
        self.add_timeseries('SV_B', 'geoId/06', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name, facet_id='facet_census')
        
        # Cohort 2 (Survey): SV_A = 100, SV_B = 20
        self.add_timeseries('SV_A', 'geoId/06', 'OtherSurvey', 'P1Y', '1', '1', import_name, facet_id='facet_survey')
        self.add_timeseries('SV_B', 'geoId/06', 'OtherSurvey', 'P1Y', '1', '1', import_name, facet_id='facet_survey')
        
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_B', 'geoId/06', '2020', 20.0, import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_A', 'geoId/06', '2020', 100.0, import_name=import_name, facet_id='facet_survey')
        self.add_observation('SV_B', 'geoId/06', '2020', 20.0, import_name=import_name, facet_id='facet_survey')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "StatVar Aggregation Multiple Cohorts",
                "type": "STAT_VAR_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_aggregation": {
                    "aggregations": [
                        {
                            "ancestor_sv_id": "SV_Parent",
                            "source_sv_ids": ["SV_A", "SV_B"],
                            "skip_all_sources_present_check": False
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner: should have 2 distinct aggregated TimeSeries and Observations
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify TimeSeries
            ts_query = """
                SELECT facet_id, facet
                FROM TimeSeries
                WHERE variable_measured = 'SV_Parent'
            """
            ts_results = list(snapshot.execute_sql(ts_query))
            self.assertEqual(len(ts_results), 2)
            
            # Verify Observations
            obs_query = """
                SELECT facet_id, value
                FROM Observation
                WHERE variable_measured = 'SV_Parent'
                ORDER BY CAST(value AS FLOAT64)
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 2)
            # Order by value (numerically sorted by Spanner): 30.0 should be first, 120.0 second
            self.assertEqual(float(obs_results[0][1]), 30.0)
            self.assertEqual(float(obs_results[1][1]), 120.0)

    def test_aggregate_stat_vars_null_method(self):
        """Tests successful aggregation when the source has a NULL/empty measurementMethod."""
        import_name = 'CensusACS5YearSurvey_Test'
        output_import_name = f'{import_name}_StatVarAgg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"
        
        # 1. Setup mock data: method=None (maps to NULL in Spanner JSON)
        self.add_timeseries('SV_A', 'geoId/06', None, 'P1Y', '1', '1', import_name)
        self.add_timeseries('SV_B', 'geoId/06', None, 'P1Y', '1', '1', import_name)
        
        # add_observation automatically calls add_timeseries, so we pass method=None here too
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method=None, import_name=import_name)
        self.add_observation('SV_B', 'geoId/06', '2020', 20.0, method=None, import_name=import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "StatVar Aggregation Null Method",
                "type": "STAT_VAR_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_aggregation": {
                    "aggregations": [
                        {
                            "ancestor_sv_id": "SV_Parent",
                            "source_sv_ids": ["SV_A", "SV_B"],
                            "skip_all_sources_present_check": False
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify TimeSeries
            ts_query = """
                SELECT variable_measured, extra_entities_id, facet_id, facet, provenance
                FROM TimeSeries
                WHERE variable_measured = 'SV_Parent'
            """
            ts_results = list(snapshot.execute_sql(ts_query))
            self.assertEqual(len(ts_results), 1)
            ts_row = ts_results[0]
            self.assertEqual(ts_row[0], 'SV_Parent')
            
            # Verify facet JSON has measurementMethod = 'DataCommonsAggregate'
            facet_json = ts_row[3]
            self.assertEqual(facet_json['measurementMethod'], 'DataCommonsAggregate') # Aligned!
            self.assertEqual(facet_json['provenance'], expected_provenance)
            self.assertEqual(facet_json['isDcAggregate'], True)
            
            # Verify Observation
            obs_query = """
                SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value
                FROM Observation
                WHERE variable_measured = 'SV_Parent'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            obs_row = obs_results[0]
            self.assertEqual(obs_row[0], 'SV_Parent')
            self.assertEqual(obs_row[1], 'geoId/06')
            self.assertEqual(obs_row[3], ts_row[2]) # facet_id should match!
            self.assertEqual(obs_row[4], '2020')
            self.assertEqual(float(obs_row[5]), 30.0)


class StatVarAggregatorCustomDcTest(StatVarAggregatorIntegrationTest):
    is_base_dc = False
