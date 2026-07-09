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
"""Integration E2E tests for Data Commons StatVarCalculationGenerator.

Covers:
- StatVarCalculationGenerator (performing mathematical calculations across pairs of
  statistical variables such as DIVIDE, MULTIPLY, ADD, and SUBTRACT, dynamic
  prefix naming resolution, facet filtering, zero denominator safe skipping,
  missing input alignment across places/dates, extra_entities_id alignment, and
  import_name_regex filtering).

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
    uv run pytest aggregation/e2e_tests/stat_var_calculation_generator_test.py -s
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
from aggregation import BigQueryExecutor


class StatVarCalculationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarCalculationGenerator."""

    def test_calculate_divide_and_multiply(self):
        """Tests DIVIDE and MULTIPLY operations, and application of multipliers."""
        import_name = 'Energy_Import_Test'
        output_import_name = 'Energy_StatVarCalculation'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"

        # 1. Setup mock data
        # SV1: Emissions (10.0 MetricTonCO2e)
        self.add_observation('Annual_Emissions_GreenhouseGas_NonBiogenic', 'geoId/06', '2020', 10.0, 
                             method='dcAggregate/EPA_GHGRP', unit='MetricTonCO2e', import_name=import_name)
        # SV2: Generation (2.0 GigawattHour)
        self.add_observation('Annual_Generation_Electricity', 'geoId/06', '2020', 2.0, 
                             method='CensusACS5yrSurvey', unit='GigawattHour', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator with DIVIDE and MULTIPLY calculations
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {
                    'sv_regex': 'Annual_Emissions_GreenhouseGas_NonBiogenic',
                    'measurement_method_regex': 'dcAggregate/EPA_GHGRP',
                    'facet_info': {'unit': 'MetricTonCO2e', 'observation_period': 'P1Y'}
                },
                'input2': {
                    'sv_regex': 'Annual_Generation_Electricity',
                    'measurement_method_regex': 'CensusACS5yrSurvey',
                    'facet_info': {'unit': 'GigawattHour', 'observation_period': 'P1Y'}
                },
                'multiplier': 100.0,
                'output': {
                    'sv': 'Annual_Emissions_GreenhouseGas_NonBiogenic_Per_Annual_Generation_Electricity',
                    'measurement_method': 'EPA_GHGRP_EIA_Electricity',
                    'facet_info': {
                        'unit': 'MetricTonCO2ePerGigawattHour',
                        'observation_period': 'P1Y'
                    }
                }
            },
            {
                'operation': 'MULTIPLY',
                'input1': {
                    'sv_regex': 'Annual_Emissions_GreenhouseGas_NonBiogenic',
                    'measurement_method_regex': 'dcAggregate/EPA_GHGRP',
                    'facet_info': {'unit': 'MetricTonCO2e', 'observation_period': 'P1Y'}
                },
                'input2': {
                    'sv_regex': 'Annual_Generation_Electricity',
                    'measurement_method_regex': 'CensusACS5yrSurvey',
                    'facet_info': {'unit': 'GigawattHour', 'observation_period': 'P1Y'}
                },
                'multiplier': 0.5,
                'output': {
                    'sv': 'Emissions_Times_Generation',
                    'measurement_method': 'EPA_GHGRP_EIA_Electricity_Mult',
                    'facet_info': {
                        'unit': 'MetricTonCO2eGigawattHour',
                        'observation_period': 'P1Y'
                    }
                }
            }
        ]

        calculations_config = [
            {
                "name": "Divide and Multiply Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. Verify DIVIDE Results
            ts_query_div = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Annual_Emissions_GreenhouseGas_NonBiogenic_Per_Annual_Generation_Electricity' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_div = list(snapshot.execute_sql(ts_query_div))
            self.assertEqual(len(res_ts_div), 1)
            facet_id_div = res_ts_div[0][0]
            facet_json_div = res_ts_div[0][1]
            self.assertEqual(facet_json_div['measurementMethod'], 'EPA_GHGRP_EIA_Electricity')
            self.assertEqual(facet_json_div['provenance'], expected_provenance)
            self.assertEqual(facet_json_div['unit'], 'MetricTonCO2ePerGigawattHour')
            self.assertEqual(facet_json_div['isDcAggregate'], True)

            obs_query_div = """
                SELECT value, facet_id FROM Observation 
                WHERE variable_measured = 'Annual_Emissions_GreenhouseGas_NonBiogenic_Per_Annual_Generation_Electricity'
                  AND entity1 = 'geoId/06' AND date = '2020'
            """
            res_obs_div = list(snapshot.execute_sql(obs_query_div))
            self.assertEqual(len(res_obs_div), 1)
            # (10.0 / 2.0) * 100.0 = 500.0
            self.assertEqual(float(res_obs_div[0][0]), 500.0)
            self.assertEqual(res_obs_div[0][1], facet_id_div)

            # B. Verify MULTIPLY Results
            ts_query_mult = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Emissions_Times_Generation' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_mult = list(snapshot.execute_sql(ts_query_mult))
            self.assertEqual(len(res_ts_mult), 1)
            facet_id_mult = res_ts_mult[0][0]
            facet_json_mult = res_ts_mult[0][1]
            self.assertEqual(facet_json_mult['measurementMethod'], 'EPA_GHGRP_EIA_Electricity_Mult')
            self.assertEqual(facet_json_mult['unit'], 'MetricTonCO2eGigawattHour')

            obs_query_mult = """
                SELECT value, facet_id FROM Observation 
                WHERE variable_measured = 'Emissions_Times_Generation'
                  AND entity1 = 'geoId/06' AND date = '2020'
            """
            res_obs_mult = list(snapshot.execute_sql(obs_query_mult))
            self.assertEqual(len(res_obs_mult), 1)
            # (10.0 * 2.0) * 0.5 = 10.0
            self.assertEqual(float(res_obs_mult[0][0]), 10.0)
            self.assertEqual(res_obs_mult[0][1], facet_id_mult)

    def test_calculate_add_and_subtract(self):
        """Tests ADD and SUBTRACT operations and ensures multipliers are ignored."""
        import_name = 'Math_Import_Test'
        output_import_name = 'Math_StatVarCalculation'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"

        # 1. Setup mock data
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('SV_B', 'geoId/06', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator with ADD and SUBTRACT (setting a multiplier that should be ignored)
        calculations = [
            {
                'operation': 'ADD',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'observation_period': 'P1Y'}},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'observation_period': 'P1Y'}},
                'multiplier': 99.0, # Should be ignored
                'output': {
                    'sv': 'SV_Add_Result',
                    'measurement_method': 'Add_Method',
                    'facet_info': {'observation_period': 'P1Y'}
                }
            },
            {
                'operation': 'SUBTRACT',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'observation_period': 'P1Y'}},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'observation_period': 'P1Y'}},
                'multiplier': 99.0, # Should be ignored
                'output': {
                    'sv': 'SV_Sub_Result',
                    'measurement_method': 'Sub_Method',
                    'facet_info': {'observation_period': 'P1Y'}
                }
            }
        ]

        calculations_config = [
            {
                "name": "Add and Subtract Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. Verify ADD (10 + 2 = 12)
            res_add = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE variable_measured = 'SV_Add_Result' AND entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(res_add), 1)
            self.assertEqual(float(res_add[0][0]), 12.0)

            # B. Verify SUBTRACT (10 - 2 = 8)
            res_sub = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE variable_measured = 'SV_Sub_Result' AND entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(res_sub), 1)
            self.assertEqual(float(res_sub[0][0]), 8.0)

            # C. Verify TimeSeries metadata
            res_ts_add = list(snapshot.execute_sql(
                "SELECT facet FROM TimeSeries WHERE variable_measured = 'SV_Add_Result'"
            ))
            self.assertEqual(len(res_ts_add), 1)
            facet_json_add = res_ts_add[0][0]
            self.assertEqual(facet_json_add['provenance'], expected_provenance)
            self.assertEqual(facet_json_add['observationPeriod'], 'P1Y')
            self.assertEqual(facet_json_add['measurementMethod'], 'Add_Method')
            self.assertTrue(facet_json_add['isDcAggregate'])

            res_ts_sub = list(snapshot.execute_sql(
                "SELECT facet FROM TimeSeries WHERE variable_measured = 'SV_Sub_Result'"
            ))
            self.assertEqual(len(res_ts_sub), 1)
            facet_json_sub = res_ts_sub[0][0]
            self.assertEqual(facet_json_sub['provenance'], expected_provenance)
            self.assertEqual(facet_json_sub['observationPeriod'], 'P1Y')
            self.assertEqual(facet_json_sub['measurementMethod'], 'Sub_Method')
            self.assertTrue(facet_json_sub['isDcAggregate'])

    def test_calculate_dynamic_name_resolution(self):
        """Tests complex Climate temperature dynamic SV and MM prefix naming rules."""
        import_name = 'Climate_Import_Test'
        output_import_name = 'Climate_StatVarCalculation'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"

        # 1. Setup mock data
        # SV1: Model Temperature (starts with Temperature, MM has cmip prefix)
        self.add_observation('Temperature_SSP5', 'geoId/06', '2020', 25.0, 
                             method='dcAggregate/NASA_Mean_CMIP6_ModelX', unit='Celsius', import_name=import_name)
        # SV2: Baseline Temperature
        self.add_observation('Mean_Temperature', 'geoId/06', '2020', 20.0, 
                             method='dcAggregate/NASAGSOD_NASAGHCN_EPA', unit='Celsius', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator with dynamic prefix output configuration
        calculations = [
            {
                'operation': 'SUBTRACT',
                'input1': {
                    'sv_regex': '^Temperature(_SSP[0-9]+)*$',
                    'measurement_method_regex': '^dcAggregate/NASA_Mean_CMIP6_.*',
                    'facet_info': {'unit': 'Celsius', 'observation_period': 'P1Y'}
                },
                'input2': {
                    'sv_regex': '^Mean_Temperature$',
                    'measurement_method_regex': 'dcAggregate/NASAGSOD_NASAGHCN_EPA',
                    'facet_info': {'unit': 'Celsius', 'observation_period': 'P1Y'}
                },
                'output': {
                    'sv_prefix': 'DifferenceRelativeToObservationalData_',
                    'measurement_method_prefix': 'dcAggregate/NASA_Mean_CMIP6_WithBaseAs_',
                    'facet_info': {
                        'unit': 'Celsius',
                        'observation_period': 'P1Y'
                    }
                }
            }
        ]

        calculations_config = [
            {
                "name": "Dynamic Name Resolution Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # Expected Dynamically Resolved Names:
        # SV: DifferenceRelativeToObservationalData_ + Mean_ (since SV1 starts with Temperature) + Temperature_SSP5 + _ + ModelX
        # MM: dcAggregate/NASA_Mean_CMIP6_WithBaseAs_ + NASAGSOD_NASAGHCN_EPA
        expected_sv = 'DifferenceRelativeToObservationalData_Mean_Temperature_SSP5_ModelX'
        expected_mm = 'dcAggregate/NASA_Mean_CMIP6_WithBaseAs_NASAGSOD_NASAGHCN_EPA'

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            ts_res = list(snapshot.execute_sql(
                f"SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = '{expected_sv}'"
            ))
            self.assertEqual(len(ts_res), 1)
            facet_json = ts_res[0][1]
            self.assertEqual(facet_json['measurementMethod'], expected_mm)
            self.assertEqual(facet_json['provenance'], expected_provenance)

            obs_res = list(snapshot.execute_sql(
                f"SELECT value, facet_id FROM Observation WHERE variable_measured = '{expected_sv}' AND entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(obs_res), 1)
            # 25.0 - 20.0 = 5.0
            self.assertEqual(float(obs_res[0][0]), 5.0)
            self.assertEqual(obs_res[0][1], ts_res[0][0])

    def test_calculate_facet_filtering(self):
        """Tests that calculations strictly filter by the facet constraints in the config."""
        import_name = 'Facet_Import_Test'
        output_import_name = 'Facet_StatVarCalculation'

        # 1. Setup mock data: SV1 has TWO facets, but only one matches the unit filter in the config
        # Matching Facet (MetricTonCO2e)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', unit='MetricTonCO2e', import_name=import_name, facet_id='facet_metric')
        # Non-matching Facet (Pounds)
        self.add_observation('SV_A', 'geoId/06', '2020', 22046.0, method='CensusACS5yrSurvey', unit='Pounds', import_name=import_name, facet_id='facet_pounds')

        # SV2: 2.0 GigawattHour
        self.add_observation('SV_B', 'geoId/06', '2020', 2.0, method='CensusACS5yrSurvey', unit='GigawattHour', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator with config specifying input1 must have unit 'MetricTonCO2e'
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'unit': 'MetricTonCO2e'}},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey', 'facet_info': {'unit': 'GigawattHour'}},
                'multiplier': 100.0,
                'output': {
                    'sv': 'Calculated_Ratio',
                    'measurement_method': 'Ratio_Method',
                    'facet_info': {'unit': 'MetricTonCO2ePerGigawattHour'}
                }
            }
        ]

        calculations_config = [
            {
                "name": "Facet Filtering Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results
        with self.database.snapshot() as snapshot:
            res = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE variable_measured = 'Calculated_Ratio' AND entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(res), 1)
            # Must use the MetricTonCO2e value: (10.0 / 2.0) * 100.0 = 500.0
            # If it incorrectly paired with Pounds: (22046.0 / 2.0) * 100.0 = 1,102,300.0
            self.assertEqual(float(res[0][0]), 500.0)

    def test_calculate_zero_denominator_handling(self):
        """Tests that division by zero does not crash the query, and rows are safely skipped."""
        import_name = 'Zero_Import_Test'
        output_import_name = 'Zero_StatVarCalculation'

        # 1. Setup mock data
        # Place A: Denominator is 0.0 (Should be skipped)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('SV_B', 'geoId/06', '2020', 0.0, method='CensusACS5yrSurvey', import_name=import_name)

        # Place B: Normal values (Should succeed)
        self.add_observation('SV_A', 'geoId/36', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('SV_B', 'geoId/36', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'multiplier': 1.0,
                'output': {
                    'sv': 'Calculated_Ratio',
                    'measurement_method': 'Ratio_Method'
                }
            }
        ]

        calculations_config = [
            {
                "name": "Zero Denominator Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            # Place B (geoId/36) must succeed: 10.0 / 2.0 = 5.0
            res_b = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE variable_measured = 'Calculated_Ratio' AND entity1 = 'geoId/36'"
            ))
            self.assertEqual(len(res_b), 1)
            self.assertEqual(float(res_b[0][0]), 5.0)

            # Place A (geoId/06) must be skipped entirely
            res_a = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE variable_measured = 'Calculated_Ratio' AND entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(res_a), 0)

    def test_calculate_missing_inputs_alignment(self):
        """Tests that calculations only occur when BOTH input variables are present for a given place/date."""
        import_name = 'Missing_Import_Test'
        output_import_name = 'Missing_StatVarCalculation'

        # 1. Setup mock data
        # Place A (2020): Has BOTH inputs -> Should succeed
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('SV_B', 'geoId/06', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_name)

        # Place B (2020): Has only SV_A -> Should be skipped
        self.add_observation('SV_A', 'geoId/36', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name)

        # Place C (2020): Has only SV_B -> Should be skipped
        self.add_observation('SV_B', 'geoId/48', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_name)

        # Place A (2021): Has only SV_A -> Should be skipped
        self.add_observation('SV_A', 'geoId/06', '2021', 12.0, method='CensusACS5yrSurvey', import_name=import_name)
        
        self.flush_to_spanner()

        # 2. Run generator
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'multiplier': 1.0,
                'output': {
                    'sv': 'Calculated_Ratio',
                    'measurement_method': 'Ratio_Method'
                }
            }
        ]

        calculations_config = [
            {
                "name": "Missing Inputs Alignment Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            # Query all generated observations
            query = "SELECT entity1, date, value FROM Observation WHERE variable_measured = 'Calculated_Ratio' ORDER BY entity1, date"
            results = list(snapshot.execute_sql(query))
            
            # Only Place A (geoId/06) at '2020' must exist!
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], 'geoId/06')
            self.assertEqual(results[0][1], '2020')
            self.assertEqual(float(results[0][2]), 5.0)

    def test_calculate_extra_entities_alignment(self):
        """Tests that calculations align and compute separately across different extra_entities_ids."""
        import_name = 'Extra_Import_Test'
        output_import_name = 'Extra_StatVarCalculation'

        # 1. Setup mock data
        # Series A: extra_entities_id = 'entityA' (Has both inputs -> Succeeded)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name, extra_entities_id='entityA')
        self.add_observation('SV_B', 'geoId/06', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_name, extra_entities_id='entityA')

        # Series B: extra_entities_id = 'entityB' (Only SV_A -> Skipped)
        self.add_observation('SV_A', 'geoId/06', '2020', 20.0, method='CensusACS5yrSurvey', import_name=import_name, extra_entities_id='entityB')
        
        self.flush_to_spanner()

        # 2. Run generator
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {'sv_regex': 'SV_A', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'input2': {'sv_regex': 'SV_B', 'measurement_method_regex': 'CensusACS5yrSurvey'},
                'multiplier': 1.0,
                'output': {
                    'sv': 'Calculated_Ratio',
                    'measurement_method': 'Ratio_Method'
                }
            }
        ]

        calculations_config = [
            {
                "name": "Extra Entities Alignment Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            query = "SELECT extra_entities_id, value FROM Observation WHERE variable_measured = 'Calculated_Ratio' AND entity1 = 'geoId/06'"
            results = list(snapshot.execute_sql(query))
            
            # Only entityA must have a result: 10.0 / 2.0 = 5.0
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], 'entityA')
            self.assertEqual(float(results[0][1]), 5.0)

    def test_calculate_import_name_regex_filtering(self):
        """Tests that import_name_regex in the input spec correctly filters input observations by provenance."""
        import_alpha = 'Import_Alpha'
        import_beta = 'Import_Beta'
        output_import_name = 'Filtered_StatVarCalculation'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import_name}"

        # 1. Setup mock data
        # SV_A in Import_Alpha (value = 10.0)
        self.add_observation('SV_A', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_alpha, facet_id='facet_alpha')
        # SV_A in Import_Beta (value = 20.0) - this should be ignored!
        self.add_observation('SV_A', 'geoId/06', '2020', 20.0, method='CensusACS5yrSurvey', import_name=import_beta, facet_id='facet_beta')
        # SV_B in Import_Alpha (value = 2.0)
        self.add_observation('SV_B', 'geoId/06', '2020', 2.0, method='CensusACS5yrSurvey', import_name=import_alpha, facet_id='facet_alpha')

        self.flush_to_spanner()

        # 2. Run generator with import_name_regex on input1
        calculations = [
            {
                'operation': 'DIVIDE',
                'input1': {
                    'sv_regex': 'SV_A',
                    'measurement_method_regex': 'CensusACS5yrSurvey',
                    'import_name_regex': '.*Alpha$'  # Should only match Import_Alpha
                },
                'input2': {
                    'sv_regex': 'SV_B',
                    'measurement_method_regex': 'CensusACS5yrSurvey'
                },
                'multiplier': 1.0,
                'output': {
                    'sv': 'Calculated_Ratio',
                    'measurement_method': 'Ratio_Method'
                }
            }
        ]

        calculations_config = [
            {
                "name": "Import Name Regex Filtering Calculation",
                "type": "STAT_VAR_CALCULATION",
                "stage": 1,
                "input_imports": [import_alpha, import_beta],
                "output_import": output_import_name,
                "stat_var_calculation": {
                    "calculations": calculations
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations_config, active_imports=[import_alpha, import_beta])
        self.assertTrue(res.success)

        # 3. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            # We expect exactly one result: 10.0 (from Alpha) / 2.0 = 5.0
            # If it used Beta, it would be 20.0 / 2.0 = 10.0 (or we might have duplicate rows)
            query = "SELECT value FROM Observation WHERE variable_measured = 'Calculated_Ratio' AND entity1 = 'geoId/06'"
            results = list(snapshot.execute_sql(query))
            
            self.assertEqual(len(results), 1)
            self.assertEqual(float(results[0][0]), 5.0)

            # Verify TimeSeries facet has correct provenance
            res_ts = list(snapshot.execute_sql(
                "SELECT facet FROM TimeSeries WHERE variable_measured = 'Calculated_Ratio'"
            ))
            self.assertEqual(len(res_ts), 1)
            facet_json = res_ts[0][0] # Already a dict
            self.assertEqual(facet_json['provenance'], expected_provenance)


class StatVarCalculationGeneratorCustomDcTest(StatVarCalculationGeneratorIntegrationTest):
    is_base_dc = False
