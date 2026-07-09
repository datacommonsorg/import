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
"""Integration E2E tests for Data Commons SuperEnumAggregationGenerator.

Covers:
- SuperEnumAggregationGenerator (aggregating child enums into parent enums across
  hierarchies such as SchoolGradeLevelEnum and DetailedLevelOfSchoolEnum, handling
  multi-facet isolation, ignoring non-aggregatable variables or denominators,
  preserving existing dcAggregate/ prefixes, multi-entity time series entities
  JSON preservation, multi-import dynamic provenance filtering, multi-whitelisted
  properties, and curated StatVar direct mapping).
- SuperEnumSQLHelpersTest (testing BigQuery SQL helper UDFs: DC_BASE32_ENCODE,
  IS_MEASURED_PROP_AGGREGATABLE, and GET_AGGR_STRATEGY).

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
    uv run pytest aggregation/e2e_tests/super_enum_aggregation_generator_test.py -s
"""

import unittest
from typing import Any
from google.cloud import bigquery
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
    BQ_CONNECTION_ID,
    BQ_LOCATION,
)
from aggregation import BigQueryExecutor, SuperEnumAggregationGenerator
from aggregation.super_enum_aggregation_generator import (
    get_dc_base32_encode_sql,
    get_is_measured_prop_aggregatable_sql,
    get_aggr_strategy_sql,
)


class SuperEnumAggregationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for SuperEnumAggregationGenerator."""

    def test_super_enum_aggregation_success(self):
        """Verifies successful aggregation of child enums into a parent enum."""
        import_name = 'CensusACS5YearSurvey_Test'
        
        # --- 1. SETUP SCHEMA ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('Grade2', 'Grade 2', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        self.add_edge('Grade2', 'specializationOf', 'PrimarySchool', import_name)
        
        # --- 2. SETUP STATVARS ---
        self.add_node('SV_G1', 'Students in Grade 1', types=['StatisticalVariable'])
        self.add_edge('SV_G1', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G1', 'populationType', 'Person', import_name)
        self.add_edge('SV_G1', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G1', 'statType', 'measuredValue', import_name)
        self.add_edge('SV_G1', 'schoolGradeLevel', 'Grade1', import_name)
        
        self.add_node('SV_G2', 'Students in Grade 2', types=['StatisticalVariable'])
        self.add_edge('SV_G2', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G2', 'populationType', 'Person', import_name)
        self.add_edge('SV_G2', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G2', 'statType', 'measuredValue', import_name)
        self.add_edge('SV_G2', 'schoolGradeLevel', 'Grade2', import_name)
        
        # --- 3. SETUP OBSERVATIONS ---
        self.add_observation('SV_G1', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_G2', 'geoId/06', '2020', 20.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_G1', 'geoId/36', '2020', 30.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_G2', 'geoId/36', '2020', 40.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Success",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS ---
        with self.database.snapshot(multi_use=True) as snapshot:
            # Find the target SV
            query_sv = """
                SELECT subject_id 
                FROM Edge 
                WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool'
            """
            sv_results = list(snapshot.execute_sql(query_sv))
            self.assertEqual(len(sv_results), 1)
            target_sv = sv_results[0][0]
            
            # Verify target SV properties
            query_props = f"""
                SELECT predicate, object_id 
                FROM Edge 
                WHERE subject_id = '{target_sv}'
            """
            props = {r[0]: r[1] for r in snapshot.execute_sql(query_props)}
            self.assertEqual(props.get('populationType'), 'Person')
            self.assertEqual(props.get('measuredProperty'), 'count')
            self.assertEqual(props.get('statType'), 'measuredValue')
            self.assertEqual(props.get('typeOf'), 'StatisticalVariable')
            
            # Verify Observations for geoId/06 (CA): 10 + 20 = 30
            query_ts_ca = f"""
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' 
                  AND entity1 = 'geoId/06'
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'
            """
            res_ts_ca = list(snapshot.execute_sql(query_ts_ca))
            self.assertEqual(len(res_ts_ca), 1)
            facet_ca = res_ts_ca[0][0]
            
            query_obs_ca = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_ca}'
            """
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_ca))[0][0]), 30.0)
            
            # Verify Observations for geoId/36 (NY): 30 + 40 = 70
            query_ts_ny = f"""
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' 
                  AND entity1 = 'geoId/36'
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'
            """
            res_ts_ny = list(snapshot.execute_sql(query_ts_ny))
            self.assertEqual(len(res_ts_ny), 1)
            facet_ny = res_ts_ny[0][0]
            
            query_obs_ny = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/36' AND date = '2020' AND facet_id = '{facet_ny}'
            """
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_ny))[0][0]), 70.0)

    def test_super_enum_aggregation_multi_facet(self):
        """Verifies that different facets are aggregated and isolated correctly."""
        import_name = 'CensusACS5YearSurvey_Test'
        
        # --- 1. SETUP SCHEMA ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('Grade2', 'Grade 2', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        self.add_edge('Grade2', 'specializationOf', 'PrimarySchool', import_name)
        
        # --- 2. SETUP STATVARS ---
        self.add_node('SV_G1', 'Students in Grade 1', types=['StatisticalVariable'])
        self.add_edge('SV_G1', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G1', 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge('SV_G1', 'populationType', 'Person', import_name)
        self.add_edge('SV_G1', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G1', 'statType', 'measuredValue', import_name)
        
        self.add_node('SV_G2', 'Students in Grade 2', types=['StatisticalVariable'])
        self.add_edge('SV_G2', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G2', 'schoolGradeLevel', 'Grade2', import_name)
        self.add_edge('SV_G2', 'populationType', 'Person', import_name)
        self.add_edge('SV_G2', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G2', 'statType', 'measuredValue', import_name)
        
        # --- 3. SETUP OBSERVATIONS (Two different facets for the same place/date)
        # Facet 1: Census (geoId/06, 2020: 10 + 20 = 30)
        self.add_observation('SV_G1', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        self.add_observation('SV_G2', 'geoId/06', '2020', 20.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        # Facet 2: Other (geoId/06, 2020: 100 + 200 = 300)
        self.add_observation('SV_G1', 'geoId/06', '2020', 100.0, method='OtherSurvey', import_name=import_name, facet_id='facet_other')
        self.add_observation('SV_G2', 'geoId/06', '2020', 200.0, method='OtherSurvey', import_name=import_name, facet_id='facet_other')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Multi-Facet",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS ---
        with self.database.snapshot(multi_use=True) as snapshot:
            query_sv = "SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool'"
            target_sv = list(snapshot.execute_sql(query_sv))[0][0]
            
            # Verify Facet 1 (Census) Aggregation: 10 + 20 = 30
            query_ts_census = f"""
                SELECT facet_id FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06'
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'
            """
            facet_census = list(snapshot.execute_sql(query_ts_census))[0][0]
            query_obs_census = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_census}'
            """
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_census))[0][0]), 30.0)
            
            # Verify Facet 2 (Other) Aggregation: 100 + 200 = 300
            query_ts_other = f"""
                SELECT facet_id FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06'
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/OtherSurvey'
            """
            facet_other = list(snapshot.execute_sql(query_ts_other))[0][0]
            query_obs_other = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_other}'
            """
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_other))[0][0]), 300.0)

    def test_super_enum_aggregation_ignored(self):
        """Verifies that non-aggregatable SVs and SVs with denominators are ignored."""
        import_name = 'CensusACS5YearSurvey_Test'
        
        # --- 1. SETUP SCHEMA ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        
        # --- 2. SETUP STATVARS ---
        # SV_NonAgg (Invalid: measuredProperty 'income' is not aggregatable)
        self.add_node('SV_NonAgg', 'Income in Grade 1', types=['StatisticalVariable'])
        self.add_edge('SV_NonAgg', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_NonAgg', 'populationType', 'Person', import_name)
        self.add_edge('SV_NonAgg', 'measuredProperty', 'income', import_name)
        self.add_edge('SV_NonAgg', 'statType', 'measuredValue', import_name)
        self.add_edge('SV_NonAgg', 'schoolGradeLevel', 'Grade1', import_name)
        
        # SV_Denom (Invalid: has measurementDenominator)
        self.add_node('SV_Denom', 'Students in Grade 1 with Denom', types=['StatisticalVariable'])
        self.add_edge('SV_Denom', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_Denom', 'populationType', 'Person', import_name)
        self.add_edge('SV_Denom', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_Denom', 'statType', 'measuredValue', import_name)
        self.add_edge('SV_Denom', 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge('SV_Denom', 'measurementDenominator', 'SV_Pop', import_name)
        
        # --- 3. SETUP OBSERVATIONS ---
        self.add_observation('SV_NonAgg', 'geoId/06', '2020', 50.0, import_name=import_name)
        self.add_observation('SV_Denom', 'geoId/06', '2020', 60.0, import_name=import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Ignored",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS (No target SV should be created)
        with self.database.snapshot(multi_use=True) as snapshot:
            query_sv = "SELECT COUNT(DISTINCT subject_id) FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool'"
            self.assertEqual(list(snapshot.execute_sql(query_sv))[0][0], 0)

    def test_super_enum_aggregation_existing_dc_aggregate_prefix(self):
        """Verifies that an existing dcAggregate/ prefix on measurementMethod is preserved and not duplicated."""
        import_name = 'CensusACS5YearSurvey_Test'
        
        # --- 1. SETUP SCHEMA ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        
        # --- 2. SETUP STATVARS ---
        self.add_node('SV_G1', 'Students in Grade 1', types=['StatisticalVariable'])
        self.add_edge('SV_G1', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G1', 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge('SV_G1', 'populationType', 'Person', import_name)
        self.add_edge('SV_G1', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G1', 'statType', 'measuredValue', import_name)
        
        # --- 3. SETUP OBSERVATIONS WITH EXISTING dcAggregate/ PREFIX ---
        self.add_observation('SV_G1', 'geoId/06', '2020', 10.0, method='dcAggregate/CensusACS5yrSurvey', import_name=import_name, facet_id='facet_census')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Existing Prefix",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS ---
        with self.database.snapshot(multi_use=True) as snapshot:
            query_sv = "SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool'"
            target_sv = list(snapshot.execute_sql(query_sv))[0][0]
            
            # Verify that measurementMethod is 'dcAggregate/CensusACS5yrSurvey' (not 'dcAggregate/dcAggregate/...')
            query_ts = f"""
                SELECT JSON_VALUE(facet, '$.measurementMethod') 
                FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06'
            """
            methods = [r[0] for r in snapshot.execute_sql(query_ts)]
            self.assertEqual(len(methods), 1)
            self.assertEqual(methods[0], 'dcAggregate/CensusACS5yrSurvey')

    def test_super_enum_aggregation_multi_entity_timeseries(self):
        """Verifies that entity2 and entity3 in TimeSeries.entities JSON are preserved during aggregation."""
        import_name = 'CensusACS5YearSurvey_Test'
        
        # --- 1. SETUP SCHEMA ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        
        # --- 2. SETUP STATVARS ---
        self.add_node('SV_G1', 'Students in Grade 1', types=['StatisticalVariable'])
        self.add_edge('SV_G1', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_G1', 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge('SV_G1', 'populationType', 'Person', import_name)
        self.add_edge('SV_G1', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_G1', 'statType', 'measuredValue', import_name)
        
        # --- 3. SETUP OBSERVATIONS WITH MULTI-ENTITY TIMESERIES ---
        multi_entities = {'entity1': 'geoId/06', 'entity2': 'geoId/36'}
        self.add_observation('SV_G1', 'geoId/06', '2020', 100.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_multi', entities=multi_entities)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Multi-Entity TimeSeries",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS ---
        with self.database.snapshot(multi_use=True) as snapshot:
            query_sv = "SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool'"
            target_sv = list(snapshot.execute_sql(query_sv))[0][0]
            
            query_ts = f"""
                SELECT JSON_VALUE(entities, '$.entity1'), JSON_VALUE(entities, '$.entity2') 
                FROM TimeSeries 
                WHERE variable_measured = '{target_sv}' AND entity1 = 'geoId/06'
            """
            results = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(results), 1)
            self.assertEqual(results[0][0], 'geoId/06')
            self.assertEqual(results[0][1], 'geoId/36')

    def test_super_enum_aggregation_multi_import_provenance_filtering(self):
        """Verifies dynamic provenance attribution across multiple imports"""
        import_alpha = 'CensusACS_Alpha_Test'
        import_beta = 'CensusACS_Beta_Test'
        import_ignored = 'Unrelated_Ignored_Test'
        
        # --- 1. SETUP SCHEMA ACROSS ALL IMPORTS ---
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_alpha)
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_beta)
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_ignored)
        
        # --- 2. SETUP STATVARS ---
        # Alpha StatVar
        self.add_node('SV_Alpha', 'Students in Grade 1 Alpha', types=['StatisticalVariable'])
        self.add_edge('SV_Alpha', 'typeOf', 'StatisticalVariable', import_alpha)
        self.add_edge('SV_Alpha', 'schoolGradeLevel', 'Grade1', import_alpha)
        self.add_edge('SV_Alpha', 'populationType', 'Person', import_alpha)
        self.add_edge('SV_Alpha', 'measuredProperty', 'count', import_alpha)
        self.add_edge('SV_Alpha', 'statType', 'measuredValue', import_alpha)
        
        # Beta StatVar
        self.add_node('SV_Beta', 'Students in Grade 1 Beta', types=['StatisticalVariable'])
        self.add_edge('SV_Beta', 'typeOf', 'StatisticalVariable', import_beta)
        self.add_edge('SV_Beta', 'schoolGradeLevel', 'Grade1', import_beta)
        self.add_edge('SV_Beta', 'populationType', 'Household', import_beta)
        self.add_edge('SV_Beta', 'measuredProperty', 'count', import_beta)
        self.add_edge('SV_Beta', 'statType', 'measuredValue', import_beta)
        
        # Ignored StatVar (from Unrelated_Ignored_Test)
        self.add_node('SV_Ignored', 'Students in Grade 1 Ignored', types=['StatisticalVariable'])
        self.add_edge('SV_Ignored', 'typeOf', 'StatisticalVariable', import_ignored)
        self.add_edge('SV_Ignored', 'schoolGradeLevel', 'Grade1', import_ignored)
        self.add_edge('SV_Ignored', 'populationType', 'Person', import_ignored)
        self.add_edge('SV_Ignored', 'measuredProperty', 'count', import_ignored)
        self.add_edge('SV_Ignored', 'statType', 'measuredValue', import_ignored)
        
        # --- 3. SETUP OBSERVATIONS ---
        self.add_observation('SV_Alpha', 'geoId/06', '2020', 10.0, method='CensusACS5yrSurvey', import_name=import_alpha, facet_id='facet_alpha')
        self.add_observation('SV_Beta', 'geoId/06', '2020', 20.0, method='CensusACS5yrSurvey', import_name=import_beta, facet_id='facet_beta')
        self.add_observation('SV_Ignored', 'geoId/06', '2020', 500.0, method='CensusACS5yrSurvey', import_name=import_ignored, facet_id='facet_ignored')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Multi-Import Provenance Filtering",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_alpha, import_beta]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_alpha, import_beta])
        self.assertTrue(res.success)
            
        # --- 5. VERIFY RESULTS ---
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify Item #4: Zero generated edges for Unrelated_Ignored_Test
            prefix = "dc/base/" if self.is_base_dc else ""
            query_ignored = f"SELECT COUNT(*) FROM Edge WHERE provenance = '{prefix}{import_ignored}_SuperEnum'"
            self.assertEqual(list(snapshot.execute_sql(query_ignored))[0][0], 0)
            
            # Verify Item #5: Alpha generated edges get Alpha_SuperEnum provenance
            query_alpha = f"SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool' AND provenance = '{prefix}{import_alpha}_SuperEnum'"
            alpha_results = list(snapshot.execute_sql(query_alpha))
            self.assertEqual(len(alpha_results), 1)
            target_sv_alpha = alpha_results[0][0]
            
            # Verify Item #5: Beta generated edges get Beta_SuperEnum provenance
            query_beta = f"SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool' AND provenance = '{prefix}{import_beta}_SuperEnum'"
            beta_results = list(snapshot.execute_sql(query_beta))
            self.assertEqual(len(beta_results), 1)
            target_sv_beta = beta_results[0][0]
            
            # Verify observations are correctly separated under each target StatVar
            query_obs_alpha = f"SELECT value FROM Observation WHERE variable_measured = '{target_sv_alpha}' AND entity1 = 'geoId/06'"
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_alpha))[0][0]), 10.0)
            
            query_obs_beta = f"SELECT value FROM Observation WHERE variable_measured = '{target_sv_beta}' AND entity1 = 'geoId/06'"
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_beta))[0][0]), 20.0)

    def test_super_enum_aggregation_multi_whitelisted_properties(self):
        """Verifies Item #6: StatVars with multiple whitelisted properties create distinct target StatVars independently."""
        import_name = 'CensusACS_MultiProp_Test'
        
        # 1. Setup two whitelisted hierarchy structures
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        
        self.add_node('DetailedLevelOfSchoolEnum', 'Detailed Level', types=['Class'])
        self.add_node('MiddleSchool_6', 'Grade 6 Middle', types=['DetailedLevelOfSchoolEnum'])
        self.add_node('MiddleSchool', 'Middle School General', types=['DetailedLevelOfSchoolEnum'])
        self.add_edge('MiddleSchool_6', 'specializationOf', 'MiddleSchool', import_name)
        
        # 2. Setup a single StatVar with BOTH whitelisted properties
        self.add_node('SV_MultiProp', 'Students in Grade 1 and Middle School 6', types=['StatisticalVariable'])
        self.add_edge('SV_MultiProp', 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge('SV_MultiProp', 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge('SV_MultiProp', 'detailedLevelOfSchool', 'MiddleSchool_6', import_name)
        self.add_edge('SV_MultiProp', 'populationType', 'Person', import_name)
        self.add_edge('SV_MultiProp', 'measuredProperty', 'count', import_name)
        self.add_edge('SV_MultiProp', 'statType', 'measuredValue', import_name)
        
        # 3. Setup observation
        self.add_observation('SV_MultiProp', 'geoId/06', '2020', 100.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_multi')
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Multi-Whitelisted Properties",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # 5. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            prefix = "dc/base/" if self.is_base_dc else ""
            prov = f"{prefix}{import_name}_SuperEnum"
            
            # Target 1: schoolGradeLevel aggregated to PrimarySchool (detailedLevelOfSchool remains MiddleSchool_6)
            q1 = f"SELECT subject_id FROM Edge WHERE predicate = 'schoolGradeLevel' AND object_id = 'PrimarySchool' AND provenance = '{prov}'"
            res1 = list(snapshot.execute_sql(q1))
            self.assertEqual(len(res1), 1)
            target_1 = res1[0][0]
            
            # Verify Target 1 kept original detailedLevelOfSchool
            q1_other = f"SELECT object_id FROM Edge WHERE subject_id = '{target_1}' AND predicate = 'detailedLevelOfSchool'"
            self.assertEqual(list(snapshot.execute_sql(q1_other))[0][0], 'MiddleSchool_6')
            
            # Target 2: detailedLevelOfSchool aggregated to MiddleSchool (schoolGradeLevel remains Grade1)
            q2 = f"SELECT subject_id FROM Edge WHERE predicate = 'detailedLevelOfSchool' AND object_id = 'MiddleSchool' AND provenance = '{prov}'"
            res2 = list(snapshot.execute_sql(q2))
            self.assertEqual(len(res2), 1)
            target_2 = res2[0][0]
            
            # Verify Target 2 kept original schoolGradeLevel
            q2_other = f"SELECT object_id FROM Edge WHERE subject_id = '{target_2}' AND predicate = 'schoolGradeLevel'"
            self.assertEqual(list(snapshot.execute_sql(q2_other))[0][0], 'Grade1')
            
            # Verify Target 1 and Target 2 are distinct nodes
            self.assertNotEqual(target_1, target_2)

    def test_super_enum_aggregation_curated_statvar_mapping(self):
        """Verifies Item #7: If a curated StatVar matches the target properties, map directly to it without creating dc/ nodes or edges."""
        import_name = 'CensusACS_CuratedMap_Test'
        
        # 1. Setup hierarchy
        self.add_node('SchoolGradeLevelEnum', 'School Grade Level', types=['Class'])
        self.add_node('Grade1', 'Grade 1', types=['SchoolGradeLevelEnum'])
        self.add_node('PrimarySchool', 'Primary School', types=['SchoolGradeLevelEnum'])
        self.add_edge('Grade1', 'specializationOf', 'PrimarySchool', import_name)
        
        # 2. Setup a pre-existing CURATED StatVar in Spanner (no dc/ prefix)
        curated_id = 'Curated_PrimarySchool_Count'
        self.add_node(curated_id, 'Curated Primary School Count', types=['StatisticalVariable'])
        self.add_edge(curated_id, 'typeOf', 'StatisticalVariable', 'dc/base/CuratedSchema')
        self.add_edge(curated_id, 'schoolGradeLevel', 'PrimarySchool', 'dc/base/CuratedSchema')
        self.add_edge(curated_id, 'populationType', 'Person', 'dc/base/CuratedSchema')
        self.add_edge(curated_id, 'measuredProperty', 'count', 'dc/base/CuratedSchema')
        self.add_edge(curated_id, 'statType', 'measuredValue', 'dc/base/CuratedSchema')
        
        # 3. Setup source StatVar that will aggregate exactly into PrimarySchool + Person + count + measuredValue
        source_id = 'SV_Source_Grade1'
        self.add_node(source_id, 'Source Grade 1 Count', types=['StatisticalVariable'])
        self.add_edge(source_id, 'typeOf', 'StatisticalVariable', import_name)
        self.add_edge(source_id, 'schoolGradeLevel', 'Grade1', import_name)
        self.add_edge(source_id, 'populationType', 'Person', import_name)
        self.add_edge(source_id, 'measuredProperty', 'count', import_name)
        self.add_edge(source_id, 'statType', 'measuredValue', import_name)
        
        # 4. Setup observation on source StatVar
        self.add_observation(source_id, 'geoId/06', '2020', 1234.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_curated')
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Super Enum Aggregation Curated StatVar Mapping",
                "type": "SUPER_ENUM_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
            
        # 6. Verify results
        with self.database.snapshot(multi_use=True) as snapshot:
            prefix = "dc/base/" if self.is_base_dc else ""
            prov = f"{prefix}{import_name}_SuperEnum"
            
            # Verify NO auto-generated dc/... edges were created for this aggregation
            query_dc_edges = f"SELECT COUNT(*) FROM Edge WHERE provenance = '{prov}'"
            self.assertEqual(list(snapshot.execute_sql(query_dc_edges))[0][0], 0)
            
            # Verify Observation was mapped directly to the CURATED StatVar ID (`Curated_PrimarySchool_Count`)
            query_obs = f"SELECT value FROM Observation WHERE variable_measured = '{curated_id}' AND entity1 = 'geoId/06'"
            obs_results = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(obs_results), 1)
            self.assertAlmostEqual(float(obs_results[0][0]), 1234.0)


class SuperEnumAggregationGeneratorCustomDcTest(SuperEnumAggregationGeneratorIntegrationTest):
    is_base_dc = False


class SuperEnumSQLHelpersTest(unittest.TestCase):
    """Unit tests for SuperEnum BigQuery SQL helper UDFs."""

    @classmethod
    def setUpClass(cls):
        cls.bq_client = bigquery.Client(project=PROJECT_ID)

    def run_query(self, query: str) -> Any:
        """Helper to run a query and return the single result."""
        query_job = self.bq_client.query(query)
        results = list(query_job.result())
        return results[0][0]

    def test_dc_base32_encode(self):
        """Tests the DC_BASE32_ENCODE SQL helper."""
        # Known cases
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(0)"), "0")
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(1)"), "1")
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(31)"), "e")
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(32)"), "01")
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(12345)"), "t1d")
        
        # Negative / Large integer cases (treated as unsigned 64-bit)
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(-1)"), "eeeeeeeeeeeeh") # Max uint64
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(-2)"), "zeeeeeeeeeeeh") # Max uint64 - 1
        self.assertEqual(self.run_query(f"{get_dc_base32_encode_sql()} SELECT DC_BASE32_ENCODE(-9223372036854775808)"), "0000000000008") # Min int64

    def test_is_measured_prop_aggregatable(self):
        """Tests the IS_MEASURED_PROP_AGGREGATABLE SQL helper."""
        # Helper to avoid repeating SQL block
        def check(prop):
            return self.run_query(f"{get_is_measured_prop_aggregatable_sql()} SELECT IS_MEASURED_PROP_AGGREGATABLE('{prop}')")

        self.assertTrue(check('count'))
        self.assertTrue(check('gdpCount'))
        self.assertTrue(check('amount'))
        self.assertTrue(check('retailDrugDistribution'))
        self.assertFalse(check('income'))
        self.assertFalse(check('gdp'))

    def test_get_aggr_strategy(self):
        """Tests the GET_AGGR_STRATEGY SQL helper."""
        # Helper to avoid repeating SQL block
        def get_strategy(stat_type, prop):
            q = f"{get_is_measured_prop_aggregatable_sql()} {get_aggr_strategy_sql()} SELECT GET_AGGR_STRATEGY('{stat_type}', '{prop}')"
            return self.run_query(q)

        # Aggregatable properties
        self.assertEqual(get_strategy('measuredValue', 'count'), 'SUM')
        self.assertEqual(get_strategy('minValue', 'count'), 'MIN')
        self.assertEqual(get_strategy('maxValue', 'count'), 'MAX')
        self.assertEqual(get_strategy('meanValue', 'count'), 'NONE') # Not supported for aggregatable
        
        # Non-aggregatable properties (special cases)
        self.assertEqual(get_strategy('measuredValue', 'income'), 'NONE')
        self.assertEqual(get_strategy('measuredValue', 'lifetimeContractionProbability'), 'MEAN')
        self.assertEqual(get_strategy('meanValue', 'concentration'), 'MEAN')
        self.assertEqual(get_strategy('meanValue', 'gdp'), 'NONE')
        self.assertEqual(get_strategy('kurtosis', 'precipitation'), 'MEAN')
        self.assertEqual(get_strategy('kurtosis', 'gdp'), 'NONE')
        self.assertEqual(get_strategy('measuredValue', 'retailDrugDistribution'), 'SUM')


if __name__ == '__main__':
    unittest.main()
