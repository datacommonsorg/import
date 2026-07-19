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

    # --- Test Cases ---

    def test_aggregate_places_county_to_state(self):
        """Pattern 1: Standard Administrative Hierarchy (County -> State)."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Variable 1 (Count_Person) - Year 2020 (800k + 1.6M = 2.4M)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, import_name=import_name, facet_id='facet1')

        # Variable 1 (Count_Person) - Year 2021 (900k + 1.7M = 2.6M)
        self.add_observation('Count_Person', 'geoId/06075', '2021', 900000.0, import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/06001', '2021', 1700000.0, import_name=import_name, facet_id='facet1')

        # Variable 2 (Count_Farm) - Year 2020 (10 + 20 = 30)
        self.add_observation('Count_Farm', 'geoId/06075', '2020', 10.0, method='CensusOfAgriculture', import_name=import_name, facet_id='facet_farm')
        self.add_observation('Count_Farm', 'geoId/06001', '2020', 20.0, method='CensusOfAgriculture', import_name=import_name, facet_id='facet_farm')

        self.flush_to_spanner()

        calculations = [
            {
                "name": "County to State Place Rollup",
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
            # A. Verify the new parent TimeSeries was dynamically created for Count_Person!
            query_ts1 = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts1 = list(snapshot.execute_sql(query_ts1))
            self.assertEqual(len(res_ts1), 1)
            facet_id_agg1 = res_ts1[0][0]
            facet_json1 = res_ts1[0][1]
            self.assertEqual(facet_json1['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            
            expected_prov1 = f"dc/base/{import_name}_AggState" if self.is_base_dc else f"{import_name}_AggState"
            self.assertEqual(facet_json1['provenance'], expected_prov1)

            # B. Verify Count_Person 2020 Observation (2.4M)
            query_obs1_2020 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg1}'
            """
            res_obs1_2020 = list(snapshot.execute_sql(query_obs1_2020))
            self.assertEqual(len(res_obs1_2020), 1)
            self.assertAlmostEqual(float(res_obs1_2020[0][0]), 2400000.0)

            # C. Verify Count_Person 2021 Observation (2.6M)
            query_obs1_2021 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2021' 
                  AND facet_id = '{facet_id_agg1}'
            """
            res_obs1_2021 = list(snapshot.execute_sql(query_obs1_2021))
            self.assertEqual(len(res_obs1_2021), 1)
            self.assertAlmostEqual(float(res_obs1_2021[0][0]), 2600000.0)

            # D. Verify the new parent TimeSeries was dynamically created for Count_Farm!
            query_ts2 = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Farm' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts2 = list(snapshot.execute_sql(query_ts2))
            self.assertEqual(len(res_ts2), 1)
            facet_id_agg2 = res_ts2[0][0]
            facet_json2 = res_ts2[0][1]
            self.assertEqual(facet_json2['measurementMethod'], 'dcAggregate/CensusOfAgriculture')
            
            expected_prov2 = f"dc/base/{import_name}_AggState" if self.is_base_dc else f"{import_name}_AggState"
            self.assertEqual(facet_json2['provenance'], expected_prov2)

            # E. Verify Count_Farm 2020 Observation (30)
            query_obs2 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Farm' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg2}'
            """
            res_obs2 = list(snapshot.execute_sql(query_obs2))
            self.assertEqual(len(res_obs2), 1)
            self.assertAlmostEqual(float(res_obs2[0][0]), 30.0)

    def test_aggregate_places_state_to_country(self):
        """Pattern 1: Standard Administrative Hierarchy (State -> Country)."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06', 'State', parent_id='country/USA')
        self.add_place('geoId/36', 'State', parent_id='country/USA')
        self.add_place('country/USA', 'Country')
        self.add_place('State', 'Class')
        self.add_place('Country', 'Class')

        self.add_observation('Count_Person', 'geoId/06', '2020', 2400000.0, import_name=import_name)
        self.add_observation('Count_Person', 'geoId/36', '2020', 20000000.0, import_name=import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "State to Country Place Rollup",
                "type": "PLACE_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": f"{import_name}_AggCountry",
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
            # A. Verify Country TimeSeries exists
            query_ts = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'country/USA'
            """
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]
            facet_json = res_ts[0][1]
            self.assertEqual(facet_json['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            
            expected_prov = f"dc/base/{import_name}_AggCountry" if self.is_base_dc else f"{import_name}_AggCountry"
            self.assertEqual(facet_json['provenance'], expected_prov)

            # B. Verify Country Observation (22.4M)
            query_obs = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'country/USA' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg}'
            """
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 22400000.0)

    def test_aggregate_places_international(self):
        """Pattern 3: Non-US / Custom Administrative Hierarchy (Internationalization)."""
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
            # Verify Paris (place/FR_75)
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

            query_obs1 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'place/FR_75' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg1}'
            """
            res_obs1 = list(snapshot.execute_sql(query_obs1))
            self.assertEqual(len(res_obs1), 1)
            self.assertAlmostEqual(float(res_obs1[0][0]), 2100000.0)

    def test_aggregate_places_multi_facet(self):
        """Execution Pattern: Multi-Facet / Multi-Method Isolation."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Facet 1: 5yr Survey (800k + 1.6M = 2.4M)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet1')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name, facet_id='facet1')

        # Facet 2: 1yr Survey (900k + 1.8M = 2.7M)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 900000.0, method='CensusACS1yrSurvey', import_name=import_name, facet_id='facet2')
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1800000.0, method='CensusACS1yrSurvey', import_name=import_name, facet_id='facet2')

        self.flush_to_spanner()

        calculations = [
            {
                "name": "County to State Place Rollup Multi-Facet",
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
            # A. Verify Facet 1 (5yr) was created
            query_ts1 = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' 
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'
            """
            res_ts1 = list(snapshot.execute_sql(query_ts1))
            self.assertEqual(len(res_ts1), 1)
            facet_id_agg1 = res_ts1[0][0]

            query_obs1 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg1}'
            """
            res_obs1 = list(snapshot.execute_sql(query_obs1))
            self.assertEqual(len(res_obs1), 1)
            self.assertAlmostEqual(float(res_obs1[0][0]), 2400000.0)

            # B. Verify Facet 2 (1yr) was created
            query_ts2 = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' 
                  AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS1yrSurvey'
            """
            res_ts2 = list(snapshot.execute_sql(query_ts2))
            self.assertEqual(len(res_ts2), 1)
            facet_id_agg2 = res_ts2[0][0]

            query_obs2 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg2}'
            """
            res_obs2 = list(snapshot.execute_sql(query_obs2))
            self.assertEqual(len(res_obs2), 1)
            self.assertAlmostEqual(float(res_obs2[0][0]), 2700000.0)

    def test_aggregate_places_missing_parent(self):
        """Pattern 4: Entity-in-Area / Robustness to Topology."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06999', 'County') # ORPHAN COUNTY (no parent specified!)
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06999', '2020', 500000.0, method='CensusACS5yrSurvey', import_name=import_name) # Orphan population

        self.flush_to_spanner()

        calculations = [
            {
                "name": "County to State Place Rollup Missing Parent",
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
            # California TimeSeries must exist
            query_ts = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]

            # California population is STILL exactly 2.4M (ignores the 500k from the orphan county)
            query_obs = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg}'
            """
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 2400000.0)

    def test_aggregate_places_allow_multiple_to_places(self):
        """Pattern 5: One-to-Many / Overlapping Grid Rollup (Multi-Parent)."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # SF is contained in BOTH California (geoId/06) and New York (geoId/36) (Multi-parent mock)
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('geoId/36', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Add the second containment edge (SF -> NY) manually
        self.add_containment('geoId/06075', 'geoId/36')

        # Populations: SF = 800k, Alameda = 1.6M
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)

        self.flush_to_spanner()

        # --- TEST 1: allow_multiple_to_places = False (DEFAULT) ---
        # SF (800k) should ONLY roll up to CA (first lexicographically: geoId/06).
        # CA should be 2.4M. NY (geoId/36) should get 0 (no TimeSeries/Observation written for NY).
        calculations1 = [
            {
                "name": "County to State Allow Multiple False",
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
            # California TimeSeries and Observation must exist (2.4M)
            query_ts_ca = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts_ca = list(snapshot.execute_sql(query_ts_ca))
            self.assertEqual(len(res_ts_ca), 1)
            facet_id_agg_ca = res_ts_ca[0][0]

            query_ca = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg_ca}'
            """
            res_ca = list(snapshot.execute_sql(query_ca))
            self.assertEqual(len(res_ca), 1)
            self.assertAlmostEqual(float(res_ca[0][0]), 2400000.0)

            # New York TimeSeries and Observation must NOT exist!
            query_ts_ny = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'
            """
            res_ts_ny = list(snapshot.execute_sql(query_ts_ny))
            self.assertEqual(len(res_ts_ny), 0)

        # Clear tables and re-populate for the second run
        self.clear_tables()
        # Re-add mock data
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

        # --- TEST 2: allow_multiple_to_places = True ---
        # SF (800k) should roll up to BOTH CA and NY.
        # CA should be 2.4M (SF 800k + Alameda 1.6M).
        # NY should be 800k (SF 800k).
        calculations2 = [
            {
                "name": "County to State Allow Multiple True",
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
            # California (should be 2.4M)
            query_ts_ca2 = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            facet_id_agg_ca2 = list(snapshot.execute_sql(query_ts_ca2))[0][0]
            query_ca2 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg_ca2}'
            """
            res_ca2 = list(snapshot.execute_sql(query_ca2))
            self.assertAlmostEqual(float(res_ca2[0][0]), 2400000.0)

            # New York (should now be 800k because SF rolled up to it too!)
            query_ts_ny2 = """
                SELECT facet_id 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'
            """
            res_ts_ny2 = list(snapshot.execute_sql(query_ts_ny2))
            self.assertEqual(len(res_ts_ny2), 1)
            facet_id_agg_ny2 = res_ts_ny2[0][0]
            query_ny2 = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/36' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg_ny2}'
            """
            res_ny2 = list(snapshot.execute_sql(query_ny2))
            self.assertEqual(len(res_ny2), 1)
            self.assertAlmostEqual(float(res_ny2[0][0]), 800000.0)

    def test_aggregate_places_chained_rollup(self):
        """Pattern 2: Deep Nested Administrative Hierarchy (Multi-Level Chaining)."""
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Define the 3-level hierarchy (County -> State -> Country)
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State', parent_id='country/USA')
        self.add_place('country/USA', 'Country')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')
        self.add_place('Country', 'Class')

        # 2. Add observations ONLY for the lowest level (Counties)
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)
        
        self.flush_to_spanner()

        calculations = [
            {
                "name": "Round 1: County -> State",
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
                "name": "Round 2: State -> Country",
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
        res = self.run_orchestrator(
            calculations=calculations,
            active_imports=[import_name]
        )
        self.assertTrue(res.success)

        # Verify Round 1 output exists (California should now have a TimeSeries and a 2.4M Observation)
        with self.database.snapshot(multi_use=True) as snapshot:
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

            query_ca = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg_ca}'
            """
            res_ca = list(snapshot.execute_sql(query_ca))
            self.assertEqual(len(res_ca), 1)
            self.assertAlmostEqual(float(res_ca[0][0]), 2400000.0)

        # --- ROUND 2 verification (both rounds were executed sequentially by orchestrator above) ---

        # Verify Round 2 output exists (USA should now have a TimeSeries and a 2.4M Observation)
        with self.database.snapshot(multi_use=True) as snapshot:
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
            self.assertEqual(facet_json_usa['measurementMethod'], 'dcAggregate/dcAggregate/CensusACS5yrSurvey') # Double aggregated!
            
            expected_prov_usa = f"dc/base/{import_name}_AggState_AggCountry" if self.is_base_dc else f"{import_name}_AggState_AggCountry"
            self.assertEqual(facet_json_usa['provenance'], expected_prov_usa)

            query_usa = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'country/USA' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg_usa}'
            """
            res_usa = list(snapshot.execute_sql(query_usa))
            self.assertEqual(len(res_usa), 1)
            self.assertAlmostEqual(float(res_usa[0][0]), 2400000.0)

    def test_aggregate_places_single_parent_multi_type_containment(self):
        """Pattern: allow_multiple_to_places = False when child is contained in multiple parent types.

        A County is contained in both a Country ('country/USA') and a State ('geoId/06').
        Alphabetically: 'country/USA' < 'geoId/06'.
        When running County -> State place aggregation with allow_multiple_to_places = False,
        the aggregator must filter candidate parents to type State FIRST before selecting
        the minimum parent, rather than computing MIN() across all containedInPlace edges.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'

        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('country/USA', 'Country')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')
        self.add_place('Country', 'Class')

        # Also add containment to country/USA which sorts lexicographically BEFORE geoId/06
        self.add_containment('geoId/06075', 'country/USA')
        self.add_containment('geoId/06001', 'country/USA')

        # Add observations for Counties
        self.add_observation('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', import_name=import_name)
        self.add_observation('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', import_name=import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "County to State Multi-Type Containment Test",
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
            # Verify State (geoId/06) TimeSeries and Observation exist and have 2.4M
            query_ts = """
                SELECT facet_id, facet 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]

            query_obs = f"""
                SELECT value 
                FROM Observation 
                WHERE variable_measured = 'Count_Person' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg}'
            """
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 2400000.0)

    def test_aggregate_places_multi_entity_extra_entities_id(self):
        """Pattern: Place aggregation preserving multi-entity slicing (extra_entities_id & entities JSON).

        When observations are sliced by an additional dimension (e.g., gender/Male, entity2 = gender/Male,
        extra_entities_id = 'gender/Male'), place aggregation must:
        1. Preserve extra_entities_id in the generated parent TimeSeries and Observation rows.
        2. Update entities JSON via JSON_SET to retain entity2 while updating entity1 to the parent place.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'

        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Add observations with non-empty extra_entities_id and entity2
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
                "name": "County to State Multi-Entity Place Rollup",
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
            # 1. Verify parent TimeSeries row exists with extra_entities_id = 'gender/Male'
            # and entities JSON has entity1 = 'geoId/06' and entity2 = 'gender/Male'
            query_ts = """
                SELECT facet_id, extra_entities_id, entities 
                FROM TimeSeries 
                WHERE variable_measured = 'Count_Person_Male' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]
            extra_id_agg = res_ts[0][1]
            entities_agg = res_ts[0][2]

            self.assertEqual(extra_id_agg, 'gender/Male')
            self.assertEqual(entities_agg['entity1'], 'geoId/06')
            self.assertEqual(entities_agg['entity2'], 'gender/Male')

            # 2. Verify Observation row exists with extra_entities_id = 'gender/Male'
            query_obs = f"""
                SELECT value, extra_entities_id 
                FROM Observation 
                WHERE variable_measured = 'Count_Person_Male' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2020' 
                  AND facet_id = '{facet_id_agg}'
            """
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 1000000.0)
            self.assertEqual(res_obs[0][1], 'gender/Male')


class PlaceAggregationGeneratorCustomDcTest(PlaceAggregationGeneratorIntegrationTest):
    is_base_dc = False


