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
"""Integration tests for generic & dynamic place-based aggregations using BigQuery Federation."""

import os
import unittest
import logging
import json
from google.cloud import spanner
from google.cloud import bigquery

# Import the classes to test
# We need to add the current directory to sys.path to import from sibling modules
import sys
sys.path.append(os.path.dirname(__file__))
from aggregation import BigQueryExecutor, PlaceAggregationGenerator

# Configuration from environment or defaults (pointing to stuniki's test db)
PROJECT_ID = os.environ.get('PROJECT_ID', 'datcom-ci')
SPANNER_INSTANCE_ID = os.environ.get('SPANNER_INSTANCE_ID', 'datcom-spanner-test')
SPANNER_DATABASE_ID = os.environ.get('SPANNER_DATABASE_ID', 'dc_graph_stuniki_test')
# Using the specific connection for stuniki's test db
BQ_CONNECTION_ID = os.environ.get(
    'BQ_CONNECTION_ID',
    'projects/datcom-ci/locations/us-central1/connections/spanner_stuniki_test_conn'
)
BQ_LOCATION = os.environ.get('BQ_LOCATION', 'us-central1')

logging.basicConfig(level=logging.INFO)

class PlaceAggregationIntegrationTest(unittest.TestCase):
    """Integration tests for place-based aggregations using BigQuery Federation.

    All place-based aggregations in Data Commons boil down to 5 distinct structural and mathematical patterns. 
    This test suite explicitly verifies all 5 patterns to ensure 100% production-ready coverage.

    This test suite uses a TDD approach to verify a self-contained database-driven architecture:
    1. The test only writes raw, lowest-level child data (no parent placeholders or metadata).
    2. The aggregation query automatically calculates and writes BOTH the parent TimeSeries (metadata)
       and the Observation (values) using deterministic MD5 hashing in SQL.
    3. The assertions dynamically verify both Spanner tables to ensure correct data lineage (dcAggregate/ prefix).
    """

    @classmethod
    def setUpClass(cls):
        logging.info("Setting up Spanner and BigQuery clients...")
        cls.spanner_client = spanner.Client(project=PROJECT_ID)
        cls.instance = cls.spanner_client.instance(SPANNER_INSTANCE_ID)
        cls.database = cls.instance.database(SPANNER_DATABASE_ID)
        cls.bq_client = bigquery.Client(project=PROJECT_ID)
        logging.info("Clients initialized successfully.")

    def setUp(self):
        logging.info("Clearing tables before test...")
        self.clear_tables()
        # Initialize flat lists to accumulate mock data for the current test
        self.mock_nodes = []
        self.mock_edges = []
        self.mock_timeseries = []
        self.mock_observations = []

    def tearDown(self):
        logging.info("Clearing tables after test...")
        self.clear_tables()

    def clear_tables(self):
        # Delete from child tables first due to constraints
        def _clear(transaction):
            transaction.execute_update("DELETE FROM Observation WHERE TRUE")
            transaction.execute_update("DELETE FROM TimeSeries WHERE TRUE")
            transaction.execute_update("DELETE FROM Edge WHERE TRUE")
            transaction.execute_update("DELETE FROM Node WHERE TRUE")
        try:
            self.database.run_in_transaction(_clear)
            logging.info("Tables cleared successfully.")
        except Exception as e:
            logging.warning(f"Failed to clear tables: {e}")

    # --- Helpers ---

    def get_generator(self) -> PlaceAggregationGenerator:
        """Instantiates and returns the PlaceAggregationGenerator.

        Automatically configures the BigQuery client to run queries in the correct
        location (e.g., 'us-central1') matching the BQ Spanner Connection.
        """
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return PlaceAggregationGenerator(executor, is_base_dc=True)

    def add_place(self, place_id, place_type, parent_id=None, name=None, import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a place node and its basic topology (typeOf and containment) to mock lists.

        Use this to define the geographical hierarchy (e.g., County -> State)
        before running the aggregation.
        """
        provenance = f"dc/base/{import_name}"
        # Node tuple: subject_id, name, value, bytes, types
        self.mock_nodes.append((place_id, name or place_id, None, None, [place_type]))
        # typeOf Edge tuple: subject_id, predicate, object_id, provenance
        self.mock_edges.append((place_id, 'typeOf', place_type, provenance))
        # containedInPlace Edge tuple (if parent is specified)
        if parent_id:
            self.mock_edges.append((place_id, 'containedInPlace', parent_id, provenance))

    def add_containment(self, child_id, parent_id, import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a manual containedInPlace edge between a child and parent.

        Useful for defining custom or multi-parent relationships (e.g., overlapping grids)
        that don't fit the standard parent_id flow in add_place.
        """
        provenance = f"dc/base/{import_name}"
        self.mock_edges.append((child_id, 'containedInPlace', parent_id, provenance))

    def add_timeseries(self, variable, entity_id, method='CensusACS5yrSurvey', facet_id='facet1', import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a TimeSeries metadata row to Spanner mock lists.

        NOTE: Under the new self-contained design, you should NOT call this helper
        manually for the target (parent) places. The SQL script now automatically
        calculates and inserts parent TimeSeries rows. Use this only if you need
        to manually mock specific source TimeSeries metadata.
        """
        provenance = f"dc/base/{import_name}"
        entities_json = json.dumps({'entity1': entity_id})
        
        # Check if this TimeSeries already exists to avoid duplicate Spanner insertions
        exists = False
        for ts in self.mock_timeseries:
            if ts[0] == variable and ts[1] == entities_json and ts[3] == facet_id:
                exists = True
                break
                
        if not exists:
            self.mock_timeseries.append(
                (variable, entities_json, '', facet_id, 
                 json.dumps({'measurementMethod': method, 'provenance': provenance}))
            )

    def add_obs(self, variable, entity_id, date, value, method='CensusACS5yrSurvey', facet_id='facet1', import_name='USFed_ConstantMaturityRates_Test'):
        """Adds a raw Observation value and its parent TimeSeries metadata to mock lists.

        This is the primary helper to populate source (lowest-level) data for tests.
        It automatically ensures the child's TimeSeries row exists to satisfy
        Spanner referential integrity.
        """
        # Ensure the parent TimeSeries exists first
        self.add_timeseries(variable, entity_id, method=method, facet_id=facet_id, import_name=import_name)
        # Observation tuple: variable_measured, entity1, extra_entities_id, facet_id, date, value
        self.mock_observations.append(
            (variable, entity_id, '', facet_id, date, str(value))
        )

    def flush_to_spanner(self):
        """Batches and writes all accumulated mock nodes, edges, timeseries, and observations to Spanner.

        Call this at the end of your test setup, just before executing the generator.
        """
        if self.mock_nodes or self.mock_edges:
            self.write_graph_data(self.mock_nodes, self.mock_edges)
        if self.mock_timeseries or self.mock_observations:
            self.write_test_data(self.mock_timeseries, self.mock_observations)

    def write_test_data(self, timeseries_rows, observation_rows):
        logging.info(f"Writing {len(timeseries_rows)} TimeSeries and {len(observation_rows)} Observation rows to Spanner...")
        with self.database.batch() as batch:
            batch.insert(
                table='TimeSeries',
                columns=['variable_measured', 'entities', 'extra_entities_id', 'facet_id', 'facet'],
                values=timeseries_rows
            )
            if observation_rows:
                batch.insert(
                    table='Observation',
                    columns=['variable_measured', 'entity1', 'extra_entities_id', 'facet_id', 'date', 'value'],
                    values=observation_rows
                )

    def write_graph_data(self, node_rows, edge_rows):
        logging.info(f"Writing {len(node_rows)} Nodes and {len(edge_rows)} Edges to Spanner...")
        with self.database.batch() as batch:
            batch.insert(
                table='Node',
                columns=['subject_id', 'name', 'value', 'bytes', 'types'],
                values=node_rows
            )
            batch.insert(
                table='Edge',
                columns=['subject_id', 'predicate', 'object_id', 'provenance'],
                values=edge_rows
            )

    # --- Test Cases---

    def test_aggregate_places_county_to_state(self):
        """Pattern 1: Standard Administrative Hierarchy (County -> State).

        Verifies that a single call dynamically aggregates MULTIPLE variables and dates
        in parallel, automatically creating the parent TimeSeries and Observation rows.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Define isolated mock data simply (NO State parent metadata written!)
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Variable 1 (Count_Person) - Year 2020 (800k + 1.6M = 2.4M)
        self.add_obs('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', facet_id='facet1')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', facet_id='facet1')

        # Variable 1 (Count_Person) - Year 2021 [NEW DATE] (900k + 1.7M = 2.6M)
        self.add_obs('Count_Person', 'geoId/06075', '2021', 900000.0, method='CensusACS5yrSurvey', facet_id='facet1')
        self.add_obs('Count_Person', 'geoId/06001', '2021', 1700000.0, method='CensusACS5yrSurvey', facet_id='facet1')

        # Variable 2 (Count_Farm) - Year 2020 [NEW VARIABLE] (10 + 20 = 30)
        self.add_obs('Count_Farm', 'geoId/06075', '2020', 10.0, method='CensusOfAgriculture', facet_id='facet_farm')
        self.add_obs('Count_Farm', 'geoId/06001', '2020', 20.0, method='CensusOfAgriculture', facet_id='facet_farm')

        self.flush_to_spanner()

        # 2. Run the dynamic aggregation using the location-aware helper
        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

        # 3. Verify both TimeSeries (Metadata) and Observation (Values) were created dynamically
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
            self.assertEqual(facet_json1['provenance'], 'dc/base/USFed_ConstantMaturityRates_Test_AggState')

            # B. Verify Count_Person 2020 Observation (2.4M)
            query_obs1_2020 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg1}'"
            res_obs1_2020 = list(snapshot.execute_sql(query_obs1_2020))
            self.assertEqual(len(res_obs1_2020), 1)
            self.assertAlmostEqual(float(res_obs1_2020[0][0]), 2400000.0)

            # C. Verify Count_Person 2021 Observation (2.6M)
            query_obs1_2021 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2021' AND facet_id = '{facet_id_agg1}'"
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
            self.assertEqual(facet_json2['provenance'], 'dc/base/USFed_ConstantMaturityRates_Test_AggState')

            # E. Verify Count_Farm 2020 Observation (30)
            query_obs2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Farm' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg2}'"
            res_obs2 = list(snapshot.execute_sql(query_obs2))
            self.assertEqual(len(res_obs2), 1)
            self.assertAlmostEqual(float(res_obs2[0][0]), 30.0)

    def test_aggregate_places_state_to_country(self):
        """Pattern 1: Standard Administrative Hierarchy (State -> Country).

        Verifies direct State -> Country aggregation, generating the Country TimeSeries dynamically.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06', 'State', parent_id='country/USA')
        self.add_place('geoId/36', 'State', parent_id='country/USA')
        self.add_place('country/USA', 'Country')
        self.add_place('State', 'Class')
        self.add_place('Country', 'Class')

        self.add_obs('Count_Person', 'geoId/06', '2020', 2400000.0, method='CensusACS5yrSurvey')
        self.add_obs('Count_Person', 'geoId/36', '2020', 20000000.0, method='CensusACS5yrSurvey')

        self.flush_to_spanner()

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='State', destination_type='Country')
        self.assertIsNotNone(job)

        with self.database.snapshot(multi_use=True) as snapshot:
            # A. Verify Country TimeSeries exists
            query_ts = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'country/USA'"
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]
            facet_json = res_ts[0][1]
            self.assertEqual(facet_json['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            self.assertEqual(facet_json['provenance'], 'dc/base/USFed_ConstantMaturityRates_Test_AggCountry')

            # B. Verify Country Observation (22.4M)
            query_obs = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'country/USA' AND date = '2020' AND facet_id = '{facet_id_agg}'"
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 22400000.0)

    def test_aggregate_places_international(self):
        """Pattern 3: Non-US / Custom Administrative Hierarchy (Internationalization).

        Verifies dynamic aggregation on a French Commune -> Department hierarchy,
        proving true schema and country-agnostic genericity.
        """
        import_name = 'INSEE_Census_Test'
        
        self.add_place('place/FR_75056', 'Commune', parent_id='place/FR_75', import_name=import_name)
        self.add_place('place/FR_92050', 'Commune', parent_id='place/FR_92', import_name=import_name)
        self.add_place('place/FR_75', 'Department', import_name=import_name)
        self.add_place('place/FR_92', 'Department', import_name=import_name)
        self.add_place('Commune', 'Class', import_name=import_name)
        self.add_place('Department', 'Class', import_name=import_name)

        self.add_obs('Count_Person', 'place/FR_75056', '2020', 2100000.0, method='INSEE_Census', import_name=import_name)
        self.add_obs('Count_Person', 'place/FR_92050', '2020', 90000.0, method='INSEE_Census', import_name=import_name)

        self.flush_to_spanner()

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='Commune', destination_type='Department')
        self.assertIsNotNone(job)

        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify Paris (place/FR_75)
            query_ts1 = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'place/FR_75'"
            res_ts1 = list(snapshot.execute_sql(query_ts1))
            self.assertEqual(len(res_ts1), 1)
            facet_id_agg1 = res_ts1[0][0]
            facet_json1 = res_ts1[0][1]
            self.assertEqual(facet_json1['measurementMethod'], 'dcAggregate/INSEE_Census')
            self.assertEqual(facet_json1['provenance'], 'dc/base/INSEE_Census_Test_AggDepartment')

            query_obs1 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'place/FR_75' AND date = '2020' AND facet_id = '{facet_id_agg1}'"
            res_obs1 = list(snapshot.execute_sql(query_obs1))
            self.assertEqual(len(res_obs1), 1)
            self.assertAlmostEqual(float(res_obs1[0][0]), 2100000.0)

    def test_aggregate_places_multi_facet(self):
        """Execution Pattern: Multi-Facet / Multi-Method Isolation.

        Verifies that the query dynamically aggregates multiple facets (methods)
        separately in the same run, creating two distinct parent TimeSeries and Observation facets.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        # Facet 1: 5yr Survey (800k + 1.6M = 2.4M)
        self.add_obs('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey', facet_id='facet1')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey', facet_id='facet1')

        # Facet 2: 1yr Survey (900k + 1.8M = 2.7M)
        self.add_obs('Count_Person', 'geoId/06075', '2020', 900000.0, method='CensusACS1yrSurvey', facet_id='facet2')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1800000.0, method='CensusACS1yrSurvey', facet_id='facet2')

        self.flush_to_spanner()

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

        with self.database.snapshot(multi_use=True) as snapshot:
            # A. Verify Facet 1 (5yr) was created
            query_ts1 = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS5yrSurvey'"
            res_ts1 = list(snapshot.execute_sql(query_ts1))
            self.assertEqual(len(res_ts1), 1)
            facet_id_agg1 = res_ts1[0][0]

            query_obs1 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg1}'"
            res_obs1 = list(snapshot.execute_sql(query_obs1))
            self.assertEqual(len(res_obs1), 1)
            self.assertAlmostEqual(float(res_obs1[0][0]), 2400000.0)

            # B. Verify Facet 2 (1yr) was created
            query_ts2 = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06' AND JSON_VALUE(facet, '$.measurementMethod') = 'dcAggregate/CensusACS1yrSurvey'"
            res_ts2 = list(snapshot.execute_sql(query_ts2))
            self.assertEqual(len(res_ts2), 1)
            facet_id_agg2 = res_ts2[0][0]

            query_obs2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg2}'"
            res_obs2 = list(snapshot.execute_sql(query_obs2))
            self.assertEqual(len(res_obs2), 1)
            self.assertAlmostEqual(float(res_obs2[0][0]), 2700000.0)

    def test_aggregate_places_missing_parent(self):
        """Pattern 4: Entity-in-Area / Robustness to Topology.

        Verifies that the aggregation dynamically ignores source places with missing
        containment edges (orphans) without crashing, while still generating the parent State TimeSeries.
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        self.add_place('geoId/06075', 'County', parent_id='geoId/06')
        self.add_place('geoId/06001', 'County', parent_id='geoId/06')
        self.add_place('geoId/06999', 'County') # ORPHAN COUNTY (no parent specified!)
        self.add_place('geoId/06', 'State')
        self.add_place('State', 'Class')
        self.add_place('County', 'Class')

        self.add_obs('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey')
        self.add_obs('Count_Person', 'geoId/06999', '2020', 500000.0, method='CensusACS5yrSurvey') # Orphan population

        self.flush_to_spanner()

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

        with self.database.snapshot(multi_use=True) as snapshot:
            # California TimeSeries must exist
            query_ts = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            res_ts = list(snapshot.execute_sql(query_ts))
            self.assertEqual(len(res_ts), 1)
            facet_id_agg = res_ts[0][0]

            # California population is STILL exactly 2.4M (ignores the 500k from the orphan county)
            query_obs = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg}'"
            res_obs = list(snapshot.execute_sql(query_obs))
            self.assertEqual(len(res_obs), 1)
            self.assertAlmostEqual(float(res_obs[0][0]), 2400000.0)

    def test_aggregate_places_allow_multiple_to_places(self):
        """Pattern 5: One-to-Many / Overlapping Grid Rollup (Multi-Parent).

        Verifies the allow_multiple_to_places parameter. In False mode (default),
        it prevents double-counting by choosing only one parent. In True mode,
        it rolls up to all parents.
        """
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
        self.add_obs('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey')

        self.flush_to_spanner()

        # --- TEST 1: allow_multiple_to_places = False (DEFAULT) ---
        # SF (800k) should ONLY roll up to CA (first lexicographically: geoId/06).
        # CA should be 2.4M. NY (geoId/36) should get 0 (no TimeSeries/Observation written for NY).
        generator = self.get_generator()
        job1 = generator.aggregate_places(
            import_names=[import_name], 
            source_type='County', 
            destination_type='State',
            allow_multiple_to_places=False
        )
        self.assertIsNotNone(job1)

        with self.database.snapshot(multi_use=True) as snapshot:
            # California TimeSeries and Observation must exist (2.4M)
            query_ts_ca = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            res_ts_ca = list(snapshot.execute_sql(query_ts_ca))
            self.assertEqual(len(res_ts_ca), 1)
            facet_id_agg_ca = res_ts_ca[0][0]

            query_ca = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg_ca}'"
            res_ca = list(snapshot.execute_sql(query_ca))
            self.assertEqual(len(res_ca), 1)
            self.assertAlmostEqual(float(res_ca[0][0]), 2400000.0)

            # New York TimeSeries and Observation must NOT exist!
            query_ts_ny = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'"
            res_ts_ny = list(snapshot.execute_sql(query_ts_ny))
            self.assertEqual(len(res_ts_ny), 0)

        # Clear tables and re-populate for the second run
        self.clear_tables()
        self.flush_to_spanner()

        # --- TEST 2: allow_multiple_to_places = True ---
        # SF (800k) should roll up to BOTH CA and NY.
        # CA should be 2.4M (SF 800k + Alameda 1.6M).
        # NY should be 800k (SF 800k).
        generator = self.get_generator()
        job2 = generator.aggregate_places(
            import_names=[import_name], 
            source_type='County', 
            destination_type='State',
            allow_multiple_to_places=True
        )
        self.assertIsNotNone(job2)

        with self.database.snapshot(multi_use=True) as snapshot:
            # California (should be 2.4M)
            query_ts_ca2 = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            facet_id_agg_ca2 = list(snapshot.execute_sql(query_ts_ca2))[0][0]
            query_ca2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg_ca2}'"
            res_ca2 = list(snapshot.execute_sql(query_ca2))
            self.assertAlmostEqual(float(res_ca2[0][0]), 2400000.0)

            # New York (should now be 800k because SF rolled up to it too!)
            query_ts_ny2 = "SELECT facet_id FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/36'"
            res_ts_ny2 = list(snapshot.execute_sql(query_ts_ny2))
            self.assertEqual(len(res_ts_ny2), 1)
            facet_id_agg_ny2 = res_ts_ny2[0][0]
            query_ny2 = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/36' AND date = '2020' AND facet_id = '{facet_id_agg_ny2}'"
            res_ny2 = list(snapshot.execute_sql(query_ny2))
            self.assertEqual(len(res_ny2), 1)
            self.assertAlmostEqual(float(res_ny2[0][0]), 800000.0)

    def test_aggregate_places_chained_rollup(self):
        """Pattern 2: Deep Nested Administrative Hierarchy (Multi-Level Chaining).

        Verifies a 3-level chained rollup (County -> State -> Country) end-to-end.
        We write observations ONLY at the lowest level (County), and verify that
        Round 1 automatically creates the State TimeSeries/Observation, and Round 2
        successfully reads and rolls them up to Country (creating the Country TimeSeries/Observation).
        """
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
        # Absolutely NO metadata or observations are written for California (State) or USA (Country).
        self.add_obs('Count_Person', 'geoId/06075', '2020', 800000.0, method='CensusACS5yrSurvey')
        self.add_obs('Count_Person', 'geoId/06001', '2020', 1600000.0, method='CensusACS5yrSurvey')
        
        self.flush_to_spanner()

        generator = self.get_generator()
        
        # --- ROUND 1: County -> State ---
        # This will dynamically calculate California's TimeSeries & Observation (2.4M)
        job1 = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job1)

        # Verify Round 1 output exists (California should now have a TimeSeries and a 2.4M Observation)
        with self.database.snapshot(multi_use=True) as snapshot:
            query_ts_ca = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'"
            res_ts_ca = list(snapshot.execute_sql(query_ts_ca))
            self.assertEqual(len(res_ts_ca), 1)
            facet_id_agg_ca = res_ts_ca[0][0]
            facet_json_ca = res_ts_ca[0][1]
            self.assertEqual(facet_json_ca['measurementMethod'], 'dcAggregate/CensusACS5yrSurvey')
            self.assertEqual(facet_json_ca['provenance'], 'dc/base/USFed_ConstantMaturityRates_Test_AggState')

            query_ca = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'geoId/06' AND date = '2020' AND facet_id = '{facet_id_agg_ca}'"
            res_ca = list(snapshot.execute_sql(query_ca))
            self.assertEqual(len(res_ca), 1)
            self.assertAlmostEqual(float(res_ca[0][0]), 2400000.0)

        # --- ROUND 2: State -> Country ---
        # This will read the newly written California row (2.4M) and roll it up to USA (dynamically creating USA's TimeSeries & Observation!)
        # We must pass the output import name of Round 1 as the input to Round 2!
        job2 = generator.aggregate_places(import_names=[f"{import_name}_AggState"], source_type='State', destination_type='Country')
        self.assertIsNotNone(job2)

        # Verify Round 2 output exists (USA should now have a TimeSeries and a 2.4M Observation)
        with self.database.snapshot(multi_use=True) as snapshot:
            query_ts_usa = "SELECT facet_id, facet FROM TimeSeries WHERE variable_measured = 'Count_Person' AND JSON_VALUE(entities, '$.entity1') = 'country/USA'"
            res_ts_usa = list(snapshot.execute_sql(query_ts_usa))
            self.assertEqual(len(res_ts_usa), 1)
            facet_id_agg_usa = res_ts_usa[0][0]
            facet_json_usa = res_ts_usa[0][1]
            self.assertEqual(facet_json_usa['measurementMethod'], 'dcAggregate/dcAggregate/CensusACS5yrSurvey') # Double aggregated!
            self.assertEqual(facet_json_usa['provenance'], 'dc/base/USFed_ConstantMaturityRates_Test_AggState_AggCountry')

            query_usa = f"SELECT value FROM Observation WHERE variable_measured = 'Count_Person' AND entity1 = 'country/USA' AND date = '2020' AND facet_id = '{facet_id_agg_usa}'"
            res_usa = list(snapshot.execute_sql(query_usa))
            self.assertEqual(len(res_usa), 1)
            self.assertAlmostEqual(float(res_usa[0][0]), 2400000.0)

if __name__ == '__main__':
    unittest.main()
