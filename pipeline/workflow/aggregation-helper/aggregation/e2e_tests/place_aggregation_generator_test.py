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
"""Integration E2E tests for Data Commons PlaceAggregationGenerator.

Covers:
- PlaceAggregationGenerator (aggregating statistical observations up administrative
  hierarchies such as County->State, State->Country, international hierarchies,
  multi-facet isolation, missing/orphan parent topology robustness, one-to-many
  multi-parent rollups, and multi-level chained rollups).

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
    uv run pytest aggregation/e2e_tests/place_aggregation_generator_test.py -s
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
from aggregation import BigQueryExecutor, PlaceAggregationGenerator


class PlaceAggregationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration tests for PlaceAggregationGenerator."""

    def add_place(self, place_id, place_type, parent_id=None, name=None, import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a place node and its basic topology (typeOf and containment) to mock lists."""
        self.add_node(place_id, name, types=[place_type])
        self.add_edge(place_id, 'typeOf', place_type, import_name)
        if parent_id:
            self.add_edge(place_id, 'containedInPlace', parent_id, import_name)

    def add_containment(self, child_id, parent_id, import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a manual containedInPlace edge between a child and parent."""
        self.add_edge(child_id, 'containedInPlace', parent_id, import_name)

    # --- Test Scenarios ---

    def test_aggregate_places_hierarchical_chain(self):
        """Pattern 1: Hierarchical Chains & Multi-Level Topologies.

        Verifies:
        - Multi-level chained rollups: County -> State (Stage 1) -> Country (Stage 2).
        - Multi-variable aggregation (Count_Person and Count_Farm).
        - Multi-date aggregation (2020 and 2021).
        - Candidate parent type filtering (County contained in both State and Country selects State FIRST).
        - Idempotent measurement method naming ('dcAggregate/CensusACS5yrSurvey' without double prefixing).
        """
        import_name = 'USFed_ConstantMaturityRates_Test'

        # Topology: Counties -> States (CA, NY) -> Country (USA)
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/36061', 'County', parent_id='geoId/36') # NY County
        self.add_place('geoId/06', 'State', parent_id='country/USA')
        self.add_place('geoId/36', 'State', parent_id='country/USA')
        self.add_place('country/USA', 'Country')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')
        self.add_place('Country', 'Class')

        # Also link California Counties directly to country/USA (lexicographically before geoId/06)
        # to verify candidate parent filtering when allow_multiple_to_places = False
        self.add_containment('geoId/06075', 'country/USA')
        self.add_containment('geoId/06001', 'country/USA')

        # Observations for Count_Person:
        # Year 2020: SF 800k + Alameda 1.6M = 2.4M (CA), NY County = 20.0M (NY)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/36061', '2020', 20000000.0, import_name=import_name, facet_id='facet1')

        # Year 2021: SF 900k + Alameda 1.7M = 2.6M (CA)
        self.add_observation('Count_Person', 'geoId/06075', '2021', 900000.0, import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/06001', '2021', 1700000.0, import_name=import_name, facet_id='facet1')

        # Observations for Count_Farm:
        # Year 2020: SF 10 + Alameda 20 = 30
        self.add_observation('Count_Farm', 'geoId/06075', '2020', 10.0, method='CensusOfAgriculture', import_name=import_name, facet_id='facet_farm')
        self.add_observation('Count_Farm', 'geoId/06001', '2020', 20.0, method='CensusOfAgriculture', import_name=import_name, facet_id='facet_farm')

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Stage 1: County to State",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggState",
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            },
            {
                "name": "Stage 2: State to Country",
                "type": "PLACE_AGGREGATION",
                "stage": 2,
                "input_imports": [f"{import_name}_AggState"],
                "output_import": f"{import_name}_AggState_AggCountry",
                "place_aggregation": {
                    "from_place_types": "State",
                    "to_place_types": "Country",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # --- Verification Block A: Stage 1 State Rollup (Count_Person) ---
            query_ts_ca = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_ca = list(snapshot.execute_sql(query_ts_ca))
            self.assertEqual(len(res_ts_ca), 1)
            facet_id_agg_ca = res_ts_ca[0][0]
            facet_json_ca = res_ts_ca[0][1]
            self.assertEqual(facet_json_ca['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            expected_prov_ca = f"dc/base/{import_name}_AggState" if self.is_base_dc else f"{import_name}_AggState"
            self.assertEqual(facet_json_ca['provenance'], expected_prov_ca)

            # Year 2020: 2.4M
            query_obs_ca_2020 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg_ca}'"
            res_obs_ca_2020 = list(snapshot.execute_sql(query_obs_ca_2020))
            self.assertEqual(len(res_obs_ca_2020), 1)
            self.assertAlmostEqual(float(res_obs_ca_2020[0][0]), 2400000.0)

            # Year 2021: 2.6M
            query_obs_ca_2021 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2021' AND facet_id = '{facet_id_agg_ca}'"
            res_obs_ca_2021 = list(snapshot.execute_sql(query_obs_ca_2021))
            self.assertEqual(len(res_obs_ca_2021), 1)
            self.assertAlmostEqual(float(res_obs_ca_2021[0][0]), 2600000.0)

            # --- Verification Block B: Stage 1 State Rollup (Count_Farm) ---
            query_ts_farm = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Farm' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_farm = list(snapshot.execute_sql(query_ts_farm))
            self.assertEqual(len(res_ts_farm), 1)
            facet_id_farm = res_ts_farm[0][0]
            self.assertEqual(res_ts_farm[0][1]['measurementMethod'], 'dcAggregate/CensusOfAgriculture')

            query_obs_farm = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Farm' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_farm}'"
            res_obs_farm = list(snapshot.execute_sql(query_obs_farm))
            self.assertEqual(len(res_obs_farm), 1)
            self.assertAlmostEqual(float(res_obs_farm[0][0]), 30.0)

            # --- Verification Block C: Stage 2 Country Chained Rollup ---
            query_ts_usa = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'country/USA'
            """
            res_ts_usa = list(snapshot.execute_sql(query_ts_usa))
            self.assertEqual(len(res_ts_usa), 1)
            facet_id_agg_usa = res_ts_usa[0][0]
            facet_json_usa = res_ts_usa[0][1]
            # Idempotent measurement method (NOT dcAggregate/dcAggregate/...)
            self.assertEqual(facet_json_usa['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            expected_prov_usa = f"dc/base/{import_name}_AggState_AggCountry" if self.is_base_dc else f"{import_name}_AggState_AggCountry"
            self.assertEqual(facet_json_usa['provenance'], expected_prov_usa)

            # Country Observation: CA (2.4M) + NY (20.0M) = 22.4M
            query_obs_usa = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'country/USA' AND date = '2020' AND facet_id = '{facet_id_agg_usa}'"
            res_obs_usa = list(snapshot.execute_sql(query_obs_usa))
            self.assertEqual(len(res_obs_usa), 1)
            self.assertAlmostEqual(float(res_obs_usa[0][0]), 22400000.0)

    def test_aggregate_places_facets_and_dimensions(self):
        """Pattern 2: Multi-Facet & Multi-Entity Dimensions.

        Verifies:
        - Multi-facet isolation: Independent rollups for 5yr vs 1yr survey methods for the same variable.
        - Multi-entity slicing: Preservation of extra_entities_id ('gender/Male') and entity2 in entities JSON.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'

        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # 1. Multi-Facet Data: Count_Person across 5yr Survey (2.4M) and 1yr Survey (2.7M)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_5yr')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_5yr')
        self.add_observation('Count_Person', 'geoId/06075', '2020', 900000.0, method='CensusACS1yrSurvey', import_name=import_name, facet_id='facet_1yr')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1800000.0, method='CensusACS1yrSurvey', import_name=import_name, facet_id='facet_1yr')

        # 2. Multi-Entity Dimension Data: Count_Person_Male sliced by gender/Male (400k + 600k = 1.0M)
        entities_c1 = {'entity1': 'geoId/06075', 'entity2': 'gender/Male'}
        entities_c2 = {'entity1': 'geoId/06001', 'entity2': 'gender/Male'}
        self.add_observation(
            'Count_Person_Male', 'geoId/06075', '2020', 400000.0,
            method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_male',
            extra_entities_id='gender/Male', entities=entities_c1
        )
        self.add_observation(
            'Count_Person_Male', 'geoId/06001', '2020', 600000.0,
            method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet_male',
            extra_entities_id='gender/Male', entities=entities_c2
        )

        self.flush_to_spanner()

        calculations = [
            {
                "name": "County to State Facets & Dimensions Rollup",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggState",
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # --- Verification Block A: Multi-Facet Isolation ---
            # 5yr Survey (2.4M)
            query_ts_5yr = """
                SELECT facet_id FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' 
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'
            """
            res_ts_5yr = list(snapshot.execute_sql(query_ts_5yr))
            self.assertEqual(len(res_ts_5yr), 1)
            facet_id_5yr = res_ts_5yr[0][0]

            query_obs_5yr = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_5yr}'"
            res_obs_5yr = list(snapshot.execute_sql(query_obs_5yr))
            self.assertEqual(len(res_obs_5yr), 1)
            self.assertAlmostEqual(float(res_obs_5yr[0][0]), 2400000.0)

            # 1yr Survey (2.7M)
            query_ts_1yr = """
                SELECT facet_id FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' 
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS1yrSurvey'
            """
            res_ts_1yr = list(snapshot.execute_sql(query_ts_1yr))
            self.assertEqual(len(res_ts_1yr), 1)
            facet_id_1yr = res_ts_1yr[0][0]
            self.assertNotEqual(facet_id_5yr, facet_id_1yr)

            query_obs_1yr = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_1yr}'"
            res_obs_1yr = list(snapshot.execute_sql(query_obs_1yr))
            self.assertEqual(len(res_obs_1yr), 1)
            self.assertAlmostEqual(float(res_obs_1yr[0][0]), 2700000.0)

            # --- Verification Block B: Multi-Entity Dimension Slicing ---
            query_ts_male = """
                SELECT facet_id, extra_entities_id, entities 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person_Male' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_male = list(snapshot.execute_sql(query_ts_male))
            self.assertEqual(len(res_ts_male), 1)
            facet_id_male = res_ts_male[0][0]
            self.assertEqual(res_ts_male[0][1], 'gender/Male')
            self.assertEqual(res_ts_male[0][2]['entity1'], 'geoId/06')
            self.assertEqual(res_ts_male[0][2]['entity2'], 'gender/Male')

            query_obs_male = f"SELECT value, extra_entities_id FROM Observation WHERE variable_measured = 'Count_Person_Male' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_male}'"
            res_obs_male = list(snapshot.execute_sql(query_obs_male))
            self.assertEqual(len(res_obs_male), 1)
            self.assertAlmostEqual(float(res_obs_male[0][0]), 1000000.0)
            self.assertEqual(res_obs_male[0][1], 'gender/Male')

    def test_aggregate_places_containment_policies(self):
        """Pattern 3: Spatial Containment & Robustness Policies.

        Verifies:
        - allow_multiple_to_places = False (primary parent selection).
        - allow_multiple_to_places = True (multi-parent rollup across overlapping grids).
        - Orphan county robustness (unconnected child places do not break pipeline or corrupt totals).
        """
        import_name = 'USFed_ConstantMaturityRates_Test'

        # SF is contained in BOTH CA (geoId/06) and NY (geoId/36)
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06999', 'County') # Orphan county (no parent)
        self.add_place('geoId/06', 'State')
        self.add_place('geoId/36', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')
        self.add_containment('geoId/06075', 'geoId/36')

        # SF = 800k, Alameda = 1.6M, Orphan = 500k
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06999', '2020', 500000.0, method='CensusACS5yrSurvey', import_name=import_name)

        self.flush_to_spanner()

        # --- Phase 1: allow_multiple_to_places = False ---
        calculations1 = [
            {
                "name": "Containment Policy Allow Multiple False",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggState",
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res1 = self.run_orchestrator(calculations=calculations1, active_imports=[import_name])
        self.assertTrue(res1.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # CA gets exactly 2.4M (SF 800k + Alameda 1.6M, ignores 500k orphan)
            query_ts_ca = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            facet_id_ca = list(snapshot.execute_sql(query_ts_ca))[0][0]
            query_obs_ca = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_ca}'"
            res_obs_ca = list(snapshot.execute_sql(query_obs_ca))
            self.assertAlmostEqual(float(res_obs_ca[0][0]), 2400000.0)

            # NY gets 0 rows (single parent rule selects CA)
            query_ts_ny = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'"
            self.assertEqual(len(list(snapshot.execute_sql(query_ts_ny))), 0)

        # Clear and re-populate for Phase 2
        self.clear_tables()
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('geoId/36', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')
        self.add_containment('geoId/06075', 'geoId/36')
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.flush_to_spanner()

        # --- Phase 2: allow_multiple_to_places = True ---
        calculations2 = [
            {
                "name": "Containment Policy Allow Multiple True",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggState",
                "place_aggregation": {
                    "from_place_types": "County",
                    "to_place_types": "State",
                    "allow_multiple_to_places": True
                }
            }
        ]
        res2 = self.run_orchestrator(calculations=calculations2, active_imports=[import_name])
        self.assertTrue(res2.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # CA = 2.4M
            query_ts_ca2 = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            facet_id_ca2 = list(snapshot.execute_sql(query_ts_ca2))[0][0]
            query_obs_ca2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_ca2}'"
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_ca2))[0][0]), 2400000.0)

            # NY now receives SF's 800k population
            query_ts_ny2 = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'"
            res_ts_ny2 = list(snapshot.execute_sql(query_ts_ny2))
            self.assertEqual(len(res_ts_ny2), 1)
            facet_id_ny2 = res_ts_ny2[0][0]
            query_obs_ny2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/36' AND date = '2020' AND facet_id = '{facet_id_ny2}'"
            self.assertAlmostEqual(float(list(snapshot.execute_sql(query_obs_ny2))[0][0]), 800000.0)

    def test_aggregate_places_custom_place_types(self):
        """Pattern 4: Custom & International Place Hierarchies.

        Verifies rollups on non-standard international administrative hierarchies (Commune -> Department)
        and custom import provenance prefixing.
        """
        import_name = 'INSEE_Census_Test'

        self.add_place('place/FR_75056', 'Commune', parent_id='place/FR_75', import_name=import_name)
        self.add_place('place/FR_92050', 'Commune', parent_id='place/FR_92', import_name=import_name)
        self.add_place('place/FR_75', 'Department', import_name=import_name)
        self.add_place('place/FR_92', 'Department', import_name=import_name)
        self.add_place('Commune', 'Class', import_name=import_name)
        self.add_place('Department', 'Class', import_name=import_name)

        self.add_observation('Count_Person', 'place/FR_75056', '2020', 2100000.0, method='INSEE_Census', import_name=import_name)
        self.add_observation('Count_Person', 'place/FR_92050', '2020', 90000.0, method='INSEE_Census', import_name=import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Commune to Department Place Rollup",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggDepartment",
                "place_aggregation": {
                    "from_place_types": "Commune",
                    "to_place_types": "Department",
                    "allow_multiple_to_places": False
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            query_ts1 = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'place/FR_75'
            """
            res_ts1 = list(snapshot.execute_sql(query_ts1))
            self.assertEqual(len(res_ts1), 1)
            facet_id_agg1 = res_ts1[0][0]
            facet_json1 = res_ts1[0][1]
            self.assertEqual(facet_json1['measurementMethod'], 'dcAggregate/INSEE_Census')
            expected_prov = f"dc/base/{import_name}_AggDepartment" if self.is_base_dc else f"{import_name}_AggDepartment"
            self.assertEqual(facet_json1['provenance'], expected_prov)

            query_obs1 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'place/FR_75' AND date = '2020' AND facet_id = '{facet_id_agg1}'"
            res_obs1 = list(snapshot.execute_sql(query_obs1))
            self.assertEqual(len(res_obs1), 1)
            self.assertAlmostEqual(float(res_obs1[0][0]), 2100000.0)


class PlaceAggregationGeneratorCustomDcTest(PlaceAggregationGeneratorIntegrationTest):
    is_base_dc = False
