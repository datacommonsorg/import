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
"""Integration E2E tests for Data Commons StatVarSeriesAggregator.

Covers:
- StatVarSeriesAggregator (aggregating series data across models and over time,
  multi-round calculations such as anomalies and ensembles, temporal aggregations
  from daily to monthly/yearly using mean/max/min/sum operators, and count
  threshold exceptions over time such as days above or below specific thresholds).

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
    uv run pytest aggregation/e2e_tests/stat_var_series_aggregator_test.py -s
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
from aggregation import BigQueryExecutor, StatVarSeriesAggregator


class StatVarSeriesAggregatorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarSeriesAggregator."""

    def test_multiround_aggregation(self):
        """Tests multi-round aggregation: Round 1 (Anomalies) -> Round 2 (Ensembles)."""
        import_name = 'NASA_NEXGDDP_Test'
        round1_output_import = f'{import_name}_AggrDiffStats'
        round2_output_import = f'{import_name}_AggrStatsAcrossModels'
        
        prefix = "dc/base/" if self.is_base_dc else ""
        r1_expected_provenance = f"{prefix}{round1_output_import}"
        r2_expected_provenance = f"{prefix}{round2_output_import}"

        place_id = 'geoId/5363000'

        # 1. Setup mock data
        # Baseline: 2015-07
        self.add_observation('Max_Temperature', place_id, '2015-07', 30.0, method='Model_A', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2015-07', 32.0, method='Model_B', import_name=import_name, facet_id='facet_b')
        self.add_observation('Max_Temperature', place_id, '2015-07', 28.0, method='Model_C', import_name=import_name, facet_id='facet_c')

        # Projection: 2050-07
        self.add_observation('Max_Temperature', place_id, '2050-07', 33.0, method='Model_A', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07', 36.0, method='Model_B', import_name=import_name, facet_id='facet_b')
        self.add_observation('Max_Temperature', place_id, '2050-07', 30.0, method='Model_C', import_name=import_name, facet_id='facet_c')

        # Pre-base-date observation: 2010-07 (should NOT produce DifferenceRelativeToBaseDate201507 filtering year > base_date_year)
        self.add_observation('Max_Temperature', place_id, '2010-07', 29.0, method='Model_A', import_name=import_name, facet_id='facet_a')

        self.flush_to_spanner()

        calculations_config = [
            {
                "name": "Round 1 Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": round1_output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {"max_diff_across_measurement_methods": {}},
                        {
                            "diff_relative_to_base_date": {
                                "dates": ["2015-07"]
                            }
                        }
                    ]
                }
            },
            {
                "name": "Round 2 Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 2,
                "input_imports": [round1_output_import],
                "output_import": round2_output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "stats_across_models": {
                                "sv_regex": "^DifferenceRelativeToBaseDate.*",
                                "aggregation_ops": [
                                    "OPERATOR_MEDIAN",
                                    "OPERATOR_PERCENTILE10",
                                    "OPERATOR_PERCENTILE90"
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify Round 1 Results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify TimeSeries for DifferenceAcrossModels
            ts_diff_query = """
                SELECT facet_id, facet
                FROM TimeSeries
                WHERE variable_measured = 'DifferenceAcrossModels_Max_Temperature'
            """
            ts_diff_results = list(snapshot.execute_sql(ts_diff_query))
            self.assertEqual(len(ts_diff_results), 1)
            self.assertEqual(ts_diff_results[0][1]['provenance'], r1_expected_provenance)
            self.assertEqual(ts_diff_results[0][1]['measurementMethod'], 'dcAggregate/DifferenceAcrossModels')

            # Verify TimeSeries for DifferenceRelativeToBaseDate
            ts_rel_query = """
                SELECT facet_id, facet
                FROM TimeSeries
                WHERE variable_measured = 'DifferenceRelativeToBaseDate201507_Max_Temperature'
            """
            ts_rel_results = list(snapshot.execute_sql(ts_rel_query))
            self.assertEqual(len(ts_rel_results), 3) # One for each model (A, B, C)
            
            # Verify Observations for DifferenceAcrossModels (Max Diff in 2050 should be 36 - 30 = 6)
            obs_diff_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'DifferenceAcrossModels_Max_Temperature'
                  AND date = '2050-07'
            """
            obs_diff_results = list(snapshot.execute_sql(obs_diff_query))
            self.assertEqual(len(obs_diff_results), 1)
            self.assertEqual(float(obs_diff_results[0][0]), 6.0)

            # Verify Observations for DifferenceRelativeToBaseDate (Anomaly in 2050)
            # Model A: 33 - 30 = 3
            # Model B: 36 - 32 = 4
            # Model C: 30 - 28 = 2
            obs_rel_query = """
                SELECT facet_id, value
                FROM Observation
                WHERE variable_measured = 'DifferenceRelativeToBaseDate201507_Max_Temperature'
                  AND date = '2050-07'
                ORDER BY CAST(value AS FLOAT64)
            """
            obs_rel_results = list(snapshot.execute_sql(obs_rel_query))
            self.assertEqual(len(obs_rel_results), 3)
            self.assertEqual(float(obs_rel_results[0][1]), 2.0) # Model C
            self.assertEqual(float(obs_rel_results[1][1]), 3.0) # Model A
            self.assertEqual(float(obs_rel_results[2][1]), 4.0) # Model B

            # Verify that pre-base-date observation (2010-07) was cleanly filtered out per C++ parity
            obs_pre_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'DifferenceRelativeToBaseDate201507_Max_Temperature'
                  AND date = '2010-07'
            """
            obs_pre_results = list(snapshot.execute_sql(obs_pre_query))
            self.assertEqual(len(obs_pre_results), 0)

            # 4. Verify Round 2 (Ensemble) Results in Spanner
            ts_ensemble_query = """
                SELECT variable_measured, facet
                FROM TimeSeries
                WHERE variable_measured LIKE '%AcrossModels_DifferenceRelativeToBaseDate201507_Max_Temperature'
                ORDER BY variable_measured
            """
            ts_ensemble_results = list(snapshot.execute_sql(ts_ensemble_query))
            self.assertEqual(len(ts_ensemble_results), 3)
            
            for row in ts_ensemble_results:
                self.assertEqual(row[1]['provenance'], r2_expected_provenance)
                self.assertEqual(row[1]['isDcAggregate'], True)

            obs_ensemble_query = """
                SELECT variable_measured, value
                FROM Observation
                WHERE variable_measured LIKE '%AcrossModels_DifferenceRelativeToBaseDate201507_Max_Temperature'
                  AND date = '2050-07'
                ORDER BY variable_measured
            """
            obs_ensemble_results = list(snapshot.execute_sql(obs_ensemble_query))
            self.assertEqual(len(obs_ensemble_results), 3)
            
            # Sorted by variable_measured:
            # 1. MedianAcrossModels_...
            # 2. Percentile10AcrossModels_...
            # 3. Percentile90AcrossModels_...
            self.assertEqual(obs_ensemble_results[0][0], 'MedianAcrossModels_DifferenceRelativeToBaseDate201507_Max_Temperature')
            self.assertEqual(float(obs_ensemble_results[0][1]), 3.0)
            
            self.assertEqual(obs_ensemble_results[1][0], 'Percentile10AcrossModels_DifferenceRelativeToBaseDate201507_Max_Temperature')
            self.assertEqual(float(obs_ensemble_results[1][1]), 2.0)
            
            self.assertEqual(obs_ensemble_results[2][0], 'Percentile90AcrossModels_DifferenceRelativeToBaseDate201507_Max_Temperature')
            self.assertEqual(float(obs_ensemble_results[2][1]), 4.0)

    def test_temporal_aggregation(self):
        """Tests temporal aggregation (aggr_over_time): daily to monthly/yearly."""
        import_name = 'NASA_NEXGDDP_Test_Temporal'
        output_import = f'{import_name}_AggrTemporal'
        place_id = 'geoId/5363000'
        
        # 1. Setup mock daily data for 2050
        self.add_observation('Max_Temperature', place_id, '2050-07-01', 30.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-02', 32.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-03', 34.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')
        
        self.flush_to_spanner()
        
        # 2. Run aggregator
        calculations_config = [
            {
                "name": "Round 1 Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "aggr_over_time": {
                                "time_range": {
                                    "input_obs_period": "P1D",
                                    "output_obs_period": "P1M"
                                },
                                "sv_configs": [
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "aggregation_op": "OPERATOR_MEAN",
                                        "use_input_sv_for_output": True
                                    }
                                ]
                            }
                        },
                        {
                            "aggr_over_time": {
                                "time_range": {
                                    "input_obs_period": "P1D",
                                    "output_obs_period": "P1Y"
                                },
                                "sv_configs": [
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "aggregation_op": "OPERATOR_MAX",
                                        "use_input_sv_for_output": False
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify Results in Spanner
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"
        
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify Monthly Mean
            ts_monthly_query = f"""
                SELECT facet
                FROM TimeSeries
                WHERE variable_measured = 'Max_Temperature'
                  AND facet_id = CAST(FARM_FINGERPRINT('{expected_provenance}^Model_A^P1M^1^1^true') AS STRING)
            """
            ts_monthly = list(snapshot.execute_sql(ts_monthly_query))
            self.assertEqual(len(ts_monthly), 1)
            self.assertEqual(ts_monthly[0][0]['observationPeriod'], 'P1M')
            
            obs_monthly_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'Max_Temperature'
                  AND date = '2050-07'
            """
            obs_monthly = list(snapshot.execute_sql(obs_monthly_query))
            self.assertEqual(len(obs_monthly), 1)
            self.assertEqual(float(obs_monthly[0][0]), 32.0)
            
            # Verify Yearly Max
            ts_yearly_query = f"""
                SELECT facet
                FROM TimeSeries
                WHERE variable_measured = 'HighestValue_Max_Temperature'
                  AND facet_id = CAST(FARM_FINGERPRINT('{expected_provenance}^Model_A^P1Y^1^1^true') AS STRING)
            """
            ts_yearly = list(snapshot.execute_sql(ts_yearly_query))
            self.assertEqual(len(ts_yearly), 1)
            self.assertEqual(ts_yearly[0][0]['observationPeriod'], 'P1Y')
            
            obs_yearly_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'HighestValue_Max_Temperature'
                  AND date = '2050'
            """
            obs_yearly = list(snapshot.execute_sql(obs_yearly_query))
            self.assertEqual(len(obs_yearly), 1)
            self.assertEqual(float(obs_yearly[0][0]), 34.0)

    def test_threshold_exception(self):
        """Tests threshold exception counting (count_threshold)."""
        import_name = 'NASA_NEXGDDP_Test_Threshold'
        output_import = f'{import_name}_AggrThreshold'
        place_id = 'geoId/5363000'
        
        # 1. Setup mock daily data for 2050
        self.add_observation('Max_Temperature', place_id, '2050-07-01', 299.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-02', 301.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-03', 298.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-08-01', 302.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-08-02', 295.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        
        self.flush_to_spanner()
        
        calculations_config = [
            {
                "name": "Threshold Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "count_threshold_exception_over_time": {
                                "time_range": {
                                    "input_obs_period": "P1D",
                                    "output_obs_period": "P1Y"
                                },
                                "thresholds": [
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "threshold_value": 300.0,
                                        "comparison": "OPERATOR_GE",
                                        "unit": "Kelvin"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify Results in Spanner
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"
        expected_sv = "NumberOfDays_300KelvinOrMore_Max_Temperature"
        
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify TimeSeries (unit removed, period P1Y)
            ts_query = f"""
                SELECT facet
                FROM TimeSeries
                WHERE variable_measured = '{expected_sv}'
                  AND facet_id = CAST(FARM_FINGERPRINT('{expected_provenance}^Model_A^P1Y^1^^true') AS STRING)
            """
            ts_results = list(snapshot.execute_sql(ts_query))
            self.assertEqual(len(ts_results), 1)
            facet = ts_results[0][0]
            self.assertEqual(facet['observationPeriod'], 'P1Y')
            self.assertNotIn('unit', facet)
            
            # Verify Observation
            obs_query = f"""
                SELECT value
                FROM Observation
                WHERE variable_measured = '{expected_sv}'
                  AND date = '2050'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            self.assertEqual(int(obs_results[0][0]), 2)

    def test_base_date_range_aggregation(self):
        """Tests diff_relative_to_base_date with a date range (start_date/end_date)."""
        import_name = 'NASA_NEXGDDP_Test_Range'
        output_import = f'{import_name}_AggrRange'
        place_id = 'geoId/5363000'

        # Baseline observations across 2015-01 to 2015-12 -> average (27 + 30 + 33) / 3 = 30.0
        self.add_observation('Max_Temperature', place_id, '2015-01', 27.0, method='Model_A', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2015-07', 30.0, method='Model_A', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2015-08', 33.0, method='Model_A', import_name=import_name, facet_id='facet_a')

        # Projection observation in 2050-07 -> 33 - 30 = 3.0
        self.add_observation('Max_Temperature', place_id, '2050-07', 33.0, method='Model_A', import_name=import_name, facet_id='facet_a')

        self.flush_to_spanner()

        calculations_config = [
            {
                "name": "Base Date Range Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "diff_relative_to_base_date": {
                                "date_specs": [
                                    {"start_date": "2015-01", "end_date": "2015-12"}
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            obs_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'DifferenceRelativeToBaseDate201501to201512_Max_Temperature'
                  AND date = '2050-07'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            self.assertEqual(float(obs_results[0][0]), 3.0)

    def test_temporal_aggregation_min_and_sum(self):
        """Tests aggr_over_time with OPERATOR_MIN and OPERATOR_SUM."""
        import_name = 'NASA_NEXGDDP_Test_MinSum'
        output_import = f'{import_name}_AggrMinSum'
        place_id = 'geoId/5363000'

        # Daily observations for 2050-07
        self.add_observation('Max_Temperature', place_id, '2050-07-01', 30.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-02', 32.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-03', 34.0, method='Model_A', period='P1D', import_name=import_name, facet_id='facet_a')

        self.flush_to_spanner()

        calculations_config = [
            {
                "name": "Min and Sum Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "aggr_over_time": {
                                "time_range": {
                                    "input_obs_period": "P1D",
                                    "output_obs_period": "P1Y"
                                },
                                "sv_configs": [
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "aggregation_op": "OPERATOR_MIN",
                                        "use_input_sv_for_output": False
                                    },
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "aggregation_op": "OPERATOR_SUM",
                                        "use_input_sv_for_output": False
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            obs_min_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'LowestValue_Max_Temperature'
                  AND date = '2050'
            """
            obs_min = list(snapshot.execute_sql(obs_min_query))
            self.assertEqual(len(obs_min), 1)
            self.assertEqual(float(obs_min[0][0]), 30.0)

            obs_sum_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'AggregateSum_Max_Temperature'
                  AND date = '2050'
            """
            obs_sum = list(snapshot.execute_sql(obs_sum_query))
            self.assertEqual(len(obs_sum), 1)
            self.assertEqual(float(obs_sum[0][0]), 96.0)

    def test_threshold_exception_or_less(self):
        """Tests count_threshold_exception_over_time with OPERATOR_LE (OrLess)."""
        import_name = 'NASA_NEXGDDP_Test_ThresLE'
        output_import = f'{import_name}_AggrThresLE'
        place_id = 'geoId/5363000'

        # 5 daily observations: 299.0, 301.0, 298.0, 302.0, 295.0 -> 2 are <= 298.0 (298.0 and 295.0)
        self.add_observation('Max_Temperature', place_id, '2050-07-01', 299.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-02', 301.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-07-03', 298.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-08-01', 302.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')
        self.add_observation('Max_Temperature', place_id, '2050-08-02', 295.0, method='Model_A', period='P1D', unit='Kelvin', import_name=import_name, facet_id='facet_a')

        self.flush_to_spanner()

        calculations_config = [
            {
                "name": "Threshold Or Less Series Aggregation",
                "type": "STAT_VAR_SERIES_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "stat_var_series_aggregation": {
                    "aggr_funcs": [
                        {
                            "count_threshold_exception_over_time": {
                                "time_range": {
                                    "input_obs_period": "P1D",
                                    "output_obs_period": "P1Y"
                                },
                                "thresholds": [
                                    {
                                        "sv_regex": "^Max_Temperature$",
                                        "threshold_value": 298.0,
                                        "comparison": "OPERATOR_LE",
                                        "unit": "Kelvin"
                                    }
                                ]
                            }
                        }
                    ]
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            obs_query = """
                SELECT value
                FROM Observation
                WHERE variable_measured = 'NumberOfDays_298KelvinOrLess_Max_Temperature'
                  AND date = '2050'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            self.assertEqual(int(obs_results[0][0]), 2)


class StatVarSeriesAggregatorCustomDcTest(StatVarSeriesAggregatorIntegrationTest):
    is_base_dc = False
