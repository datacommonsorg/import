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
"""Integration E2E tests for Data Commons aggregations.

Covers both LinkedEdgeGenerator and ProvenanceSummaryGenerator.

NOTE: This script is intended for local testing purposes only and is NOT
currently part of the CI pipeline.

WARNING: Running these tests will DELETE all existing data in the target
database tables (Cache, Observation, TimeSeries, Edge, and Node) as part of
the setUp and tearDown phases. Do NOT run this against any database whose
data you do not want to lose (e.g., production, staging, or active development databases)!

Before running this script, you MUST update the configuration variables below
(such as PROJECT_ID, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, and BQ_CONNECTION_ID)
to point to your specific test environment.

How to run:
1. Ensure your local environment is authenticated (gcloud auth application-default login).
2. Set the environment variables or edit the config below.
3. Run the following command from the `import/pipeline/workflow/ingestion-helper` directory:

    GOOGLE_API_USE_CLIENT_CERTIFICATE=false \
    UV_NO_CONFIG=1 \
    UV_INDEX_URL=https://pypi.org/simple \
    uv run pytest aggregation/e2e_tests/aggregation_e2e_test.py -s
"""

import os
import unittest
import logging
import json
from collections.abc import Mapping
from google.cloud import spanner
from google.cloud import bigquery

import sys
# Add ingestion-helper to sys.path (two levels up from this file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from aggregation import BigQueryExecutor, LinkedEdgeGenerator, ProvenanceSummaryGenerator

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'datcom-ci')
SPANNER_INSTANCE_ID = os.environ.get('SPANNER_INSTANCE_ID', 'datcom-spanner-test')
SPANNER_DATABASE_ID = os.environ.get('SPANNER_DATABASE_ID', 'dc_graph_stuniki_test')
BQ_CONNECTION_ID = os.environ.get(
    'BQ_CONNECTION_ID',
    'projects/datcom-ci/locations/us-central1/connections/spanner_stuniki_test_conn'
)
BQ_LOCATION = os.environ.get('BQ_LOCATION', 'us-central1')

logging.basicConfig(level=logging.INFO)


class AggregationIntegrationTestBase(unittest.TestCase):
    """Base class for aggregation integration tests, handling Spanner setup/teardown."""

    is_base_dc = True

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
        self.mock_nodes = []
        self.mock_edges = []
        self.mock_timeseries = []
        self.mock_observations = []

    def tearDown(self):
        logging.info("Clearing tables after test...")
        self.clear_tables()
        self.mock_nodes = []
        self.mock_edges = []
        self.mock_timeseries = []
        self.mock_observations = []

    def clear_tables(self):
        def _clear(transaction):
            # Delete in correct order due to constraints
            transaction.execute_update("DELETE FROM Cache WHERE TRUE")
            transaction.execute_update("DELETE FROM Observation WHERE TRUE")
            transaction.execute_update("DELETE FROM TimeSeries WHERE TRUE")
            transaction.execute_update("DELETE FROM Edge WHERE TRUE")
            transaction.execute_update("DELETE FROM Node WHERE TRUE")
        
        self.database.run_in_transaction(_clear)
        logging.info("Tables cleared successfully.")

    def add_node(self, subject_id, name=None, types=None):
        """Adds a node to mock list."""
        self.mock_nodes.append((subject_id, name or subject_id, None, None, types or []))

    def add_edge(self, subject_id, predicate, object_id, import_name):
        """Adds an edge to mock list."""
        prefix = "dc/base/" if self.is_base_dc else ""
        provenance = f"{prefix}{import_name}"
        self.mock_edges.append((subject_id, predicate, object_id, provenance))

    def add_timeseries(self, variable, entity_id, method, period, unit, scaling, import_name, facet_id='facet1', is_dc_aggregate=False):
        """Adds a TimeSeries metadata row to mock list."""
        prefix = "dc/base/" if self.is_base_dc else ""
        provenance = f"{prefix}{import_name}"
        entities_json = json.dumps({'entity1': entity_id})
        facet_json = json.dumps({
            'measurementMethod': method,
            'observationPeriod': period,
            'unit': unit,
            'scalingFactor': scaling,
            'provenance': provenance,
            'isDCAggregate': is_dc_aggregate
        })
        
        # Avoid duplicates in mock list
        exists = False
        for ts in self.mock_timeseries:
            if ts[0] == variable and ts[1] == entities_json and ts[3] == facet_id:
                exists = True
                break
        if not exists:
            self.mock_timeseries.append((variable, entities_json, '', facet_id, facet_json))

    def add_observation(self, variable, entity_id, date, value, method, period, unit, scaling, import_name, facet_id='facet1', is_dc_aggregate=False):
        """Adds an Observation and ensures its parent TimeSeries exists."""
        self.add_timeseries(variable, entity_id, method, period, unit, scaling, import_name, facet_id, is_dc_aggregate)
        self.mock_observations.append((variable, entity_id, '', facet_id, date, str(value)))

    def flush_to_spanner(self):
        """Writes all accumulated mock data to Spanner and clears the buffers."""
        if self.mock_nodes:
            logging.info(f"Writing {len(self.mock_nodes)} Nodes to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Node',
                    columns=['subject_id', 'name', 'value', 'bytes', 'types'],
                    values=self.mock_nodes
                )
            self.mock_nodes = []
        if self.mock_edges:
            logging.info(f"Writing {len(self.mock_edges)} Edges to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Edge',
                    columns=['subject_id', 'predicate', 'object_id', 'provenance'],
                    values=self.mock_edges
                )
            self.mock_edges = []
        if self.mock_timeseries:
            logging.info(f"Writing {len(self.mock_timeseries)} TimeSeries to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='TimeSeries',
                    columns=['variable_measured', 'entities', 'extra_entities_id', 'facet_id', 'facet'],
                    values=self.mock_timeseries
                )
            self.mock_timeseries = []
        if self.mock_observations:
            logging.info(f"Writing {len(self.mock_observations)} Observations to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Observation',
                    columns=['variable_measured', 'entity1', 'extra_entities_id', 'facet_id', 'date', 'value'],
                    values=self.mock_observations
                )
            self.mock_observations = []


class LinkedEdgeGeneratorIntegrationTestBase(AggregationIntegrationTestBase):
    """Base class for LinkedEdgeGenerator integration tests."""

    def get_generator(self) -> LinkedEdgeGenerator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return LinkedEdgeGenerator(executor, is_base_dc=self.is_base_dc)

    def test_linked_contained_in_place(self):
        """Tests run_linked_contained_in_place.
        
        Hierarchy: geoId/06075 (County) -> geoId/06 (State) -> country/USA (Country)
        """
        import_name = 'USFed_ConstantMaturityRates_Test'
        
        # 1. Setup mock data
        self.add_node('geoId/06075', 'San Francisco County', ['County'])
        self.add_node('geoId/06', 'California', ['State'])
        self.add_node('country/USA', 'United States', ['Country'])
        
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', import_name)
        self.add_edge('geoId/06', 'containedInPlace', 'country/USA', import_name)
        
        self.flush_to_spanner()
        
        # 2. Run generator
        generator = self.get_generator()
        jobs = generator.run_linked_contained_in_place([import_name])
        self.assertIsNotNone(jobs)
        
        # 3. Verify results in Spanner
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
            self.assertEqual(len(results), 3)
            self.assertEqual(tuple(results[0]), ('geoId/06', 'country/USA', expected_provenance))
            self.assertEqual(tuple(results[1]), ('geoId/06075', 'country/USA', expected_provenance))
            self.assertEqual(tuple(results[2]), ('geoId/06075', 'geoId/06', expected_provenance))

    def test_linked_member_of(self):
        """Tests run_linked_member_of.
        
        Hierarchy: Instance_A -> memberOf -> Class_B -> specializationOf -> Class_C
        """
        import_name = 'Schema_Import_Test'
        
        # 1. Setup mock data
        self.add_node('Instance_A', 'Instance A', ['Class_B'])
        self.add_node('Class_B', 'Class B', ['Class'])
        self.add_node('Class_C', 'Class C', ['Class'])
        
        self.add_edge('Instance_A', 'memberOf', 'Class_B', import_name)
        self.add_edge('Class_B', 'specializationOf', 'Class_C', import_name)
        
        self.flush_to_spanner()
        
        # 2. Run generator
        generator = self.get_generator()
        job = generator.run_linked_member_of([import_name])
        self.assertIsNotNone(job)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedMemberOf'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
            self.assertEqual(len(results), 2)
            self.assertEqual(tuple(results[0]), ('Instance_A', 'Class_B', expected_provenance))
            self.assertEqual(tuple(results[1]), ('Instance_A', 'Class_C', expected_provenance))

    def test_linked_member(self):
        """Tests run_linked_member.
        
        Hierarchy: dc/topic/TestTopic -> member -> dc/svpg/TestSvpg -> relevantVariable -> TestVariable
        """
        import_name = 'Topic_Import_Test'
        
        # 1. Setup mock data
        self.add_node('dc/topic/TestTopic', 'Test Topic', ['Topic'])
        self.add_node('dc/svpg/TestSvpg', 'Test SVPG', ['StatVarPeerGroup'])
        self.add_node('TestVariable', 'Test Variable', ['StatisticalVariable'])
        
        self.add_edge('dc/topic/TestTopic', 'member', 'dc/svpg/TestSvpg', import_name)
        self.add_edge('dc/svpg/TestSvpg', 'relevantVariable', 'TestVariable', import_name)
        
        self.flush_to_spanner()
        
        # 2. Run generator
        generator = self.get_generator()
        job = generator.run_linked_member([import_name])
        self.assertIsNotNone(job)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedMember'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
            self.assertEqual(len(results), 1)
            self.assertEqual(tuple(results[0]), ('TestVariable', 'dc/topic/TestTopic', expected_provenance))

    def test_linked_contained_in_place_cycle(self):
        """Tests run_linked_contained_in_place with a cyclic hierarchy.
        
        Cycle: geoId/06 -> containedInPlace -> geoId/36 -> containedInPlace -> geoId/06
        """
        import_name = 'Cyclic_Import_Test'
        
        # 1. Setup mock data (cycle)
        self.add_node('geoId/06', 'California', ['State'])
        self.add_node('geoId/36', 'New York', ['State'])
        
        self.add_edge('geoId/06', 'containedInPlace', 'geoId/36', import_name)
        self.add_edge('geoId/36', 'containedInPlace', 'geoId/06', import_name)
        
        self.flush_to_spanner()
        
        # 2. Run generator
        generator = self.get_generator()
        jobs = generator.run_linked_contained_in_place([import_name])
        self.assertIsNotNone(jobs)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
            # Should resolve to 4 distinct edges: 06->06, 06->36, 36->06, 36->36
            self.assertEqual(len(results), 4)
            self.assertEqual(tuple(results[0]), ('geoId/06', 'geoId/06', expected_provenance))
            self.assertEqual(tuple(results[1]), ('geoId/06', 'geoId/36', expected_provenance))
            self.assertEqual(tuple(results[2]), ('geoId/36', 'geoId/06', expected_provenance))
            self.assertEqual(tuple(results[3]), ('geoId/36', 'geoId/36', expected_provenance))

    def test_linked_contained_in_place_idempotency(self):
        """Tests run_linked_contained_in_place does not write duplicate edges if they already exist."""
        import_name = 'Idempotency_Import_Test'
        
        # 1. Setup mock base data
        self.add_node('geoId/06075', 'San Francisco County', ['County'])
        self.add_node('geoId/06', 'California', ['State'])
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', import_name)
        
        # 2. Pre-insert the expected generated edge (simulating a previous run)
        self.add_edge('geoId/06075', 'linkedContainedInPlace', 'geoId/06', 'GeneratedGraphs')
        
        self.flush_to_spanner()
        
        # 3. Run generator
        generator = self.get_generator()
        jobs = generator.run_linked_contained_in_place([import_name])
        self.assertIsNotNone(jobs)
        
        # 4. Verify no new edges were written (only the 1 pre-inserted edge exists)
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
            self.assertEqual(len(results), 1)
            self.assertEqual(tuple(results[0]), ('geoId/06075', 'geoId/06', expected_provenance))


class LinkedEdgeGeneratorBaseDcTest(LinkedEdgeGeneratorIntegrationTestBase):
    is_base_dc = True


class LinkedEdgeGeneratorCustomDcTest(LinkedEdgeGeneratorIntegrationTestBase):
    is_base_dc = False


class ProvenanceSummaryGeneratorIntegrationTestBase(AggregationIntegrationTestBase):
    """Base class for ProvenanceSummaryGenerator integration E2E tests."""

    def get_generator(self) -> ProvenanceSummaryGenerator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return ProvenanceSummaryGenerator(executor, is_base_dc=self.is_base_dc)

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
        self.add_node('geoId/06', 'California', ['State'])
        self.add_node('geoId/36', 'New York', ['State'])
        self.add_node('State', 'State Class', ['Class'])
        
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_edge('geoId/36', 'typeOf', 'State', import_name)
        
        # Add observations (this also adds TimeSeries)
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/06', '2021', 110.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/36', '2020', 200.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        self.flush_to_spanner()
        
        # 2. Run generator
        generator = self.get_generator()
        jobs = generator.run_all([import_name])
        self.assertEqual(len(jobs), 1)
        
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
        
        self.add_node('geoId/06', 'California', ['State'])
        self.add_node('geoId/36', 'New York', ['State'])
        self.add_node('State', 'State Class', ['Class'])
        
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        self.add_edge('geoId/36', 'typeOf', 'State', import_name)
        
        # Add observations
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        # We use a string for the value to simulate malformed data in Spanner
        self.add_observation('Count_Person', 'geoId/06', '2021', 'bad_value', 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/36', '2020', 200.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        self.flush_to_spanner()
        
        generator = self.get_generator()
        jobs = generator.run_all([import_name])
        self.assertEqual(len(jobs), 1)
        
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
        self.add_node('geoId/06', 'California', ['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)
        
        # 2. Dangling 1: We only add typeOf edge, NO node
        self.add_edge('geoId/99', 'typeOf', 'State', import_name)
        
        # 3. Dangling 2: We only add node, NO typeOf edge
        self.add_node('geoId/88', 'Unknown Place', ['State'])
        
        self.add_node('State', 'State Class', ['Class'])
        
        # Add observations for all 3
        self.add_observation('Count_Person', 'geoId/06', '2020', 100.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/99', '2020', 150.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        self.add_observation('Count_Person', 'geoId/88', '2020', 250.0, 'CensusACS5yrSurvey', 'P1Y', '1', '1', import_name)
        
        self.flush_to_spanner()
        
        generator = self.get_generator()
        jobs = generator.run_all([import_name])
        self.assertEqual(len(jobs), 1)
        
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


class ProvenanceSummaryGeneratorBaseDcTest(ProvenanceSummaryGeneratorIntegrationTestBase):
    is_base_dc = True


class ProvenanceSummaryGeneratorCustomDcTest(ProvenanceSummaryGeneratorIntegrationTestBase):
    is_base_dc = False


if __name__ == '__main__':
    unittest.main()
