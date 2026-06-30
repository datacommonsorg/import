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

Covers:
- LinkedEdgeGenerator (Linked Edges)
- ProvenanceSummaryGenerator (Provenance Summaries)
- StatVarAggregator (Statistical Variable Aggregations)
- StatVarGroupGenerator (Statistical Variable Group / Vertical generation)

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
from aggregation import BigQueryExecutor, LinkedEdgeGenerator, ProvenanceSummaryGenerator, StatVarAggregator, PlaceAggregationGenerator, StatVarGroupGenerator, EntityAggregationGenerator, EntityAggregationConfig

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

    def add_node(self, subject_id, name=None, value=None, types=None):
        """Adds a node to mock list."""
        self.mock_nodes.append((subject_id, name or subject_id, value, None, types or []))

    def add_edge(self, subject_id, predicate, object_id, import_name):
        """Adds an edge to mock list."""
        prefix = "dc/base/" if self.is_base_dc else ""
        provenance = f"{prefix}{import_name}"
        self.mock_edges.append((subject_id, predicate, object_id, provenance))

    def add_timeseries(self, variable, entity_id, method='CensusACS5yrSurvey', period='P1Y', unit='1', scaling='1', import_name='USFed_ConstantMaturityRates_Test', facet_id='facet1', is_dc_aggregate=False, extra_entities_id=''):
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
            'isDcAggregate': is_dc_aggregate
        })
        
        # Avoid duplicates in mock list
        exists = False
        for ts in self.mock_timeseries:
            if ts[0] == variable and ts[1] == entities_json and ts[2] == extra_entities_id and ts[3] == facet_id:
                exists = True
                break
        if not exists:
            self.mock_timeseries.append((variable, entities_json, extra_entities_id, facet_id, facet_json))

    def add_observation(self, variable, entity_id, date, value, method='CensusACS5yrSurvey', period='P1Y', unit='1', scaling='1', import_name='USFed_ConstantMaturityRates_Test', facet_id='facet1', is_dc_aggregate=False, extra_entities_id=''):
        """Adds an Observation and ensures its parent TimeSeries exists."""
        self.add_timeseries(variable, entity_id, method, period, unit, scaling, import_name, facet_id, is_dc_aggregate, extra_entities_id)
        self.mock_observations.append((variable, entity_id, extra_entities_id, facet_id, date, str(value)))

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


class LinkedEdgeGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration tests for LinkedEdgeGenerator."""

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
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('country/USA', 'United States', types=['Country'])
        
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
        self.add_node('Instance_A', 'Instance A', types=['Class_B'])
        self.add_node('Class_B', 'Class B', types=['Class'])
        self.add_node('Class_C', 'Class C', types=['Class'])
        
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
        self.add_node('dc/topic/TestTopic', 'Test Topic', types=['Topic'])
        self.add_node('dc/svpg/TestSvpg', 'Test SVPG', types=['StatVarPeerGroup'])
        self.add_node('TestVariable', 'Test Variable', types=['StatisticalVariable'])
        
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
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('geoId/36', 'New York', types=['State'])
        
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
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_node('geoId/06', 'California', types=['State'])
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


class LinkedEdgeGeneratorCustomDcTest(LinkedEdgeGeneratorIntegrationTest):
    is_base_dc = False


class ProvenanceSummaryGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for ProvenanceSummaryGenerator."""

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


class ProvenanceSummaryGeneratorCustomDcTest(ProvenanceSummaryGeneratorIntegrationTest):
    is_base_dc = False


class PlaceAggregationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration tests for PlaceAggregationGenerator."""

    def get_generator(self) -> PlaceAggregationGenerator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return PlaceAggregationGenerator(executor, is_base_dc=self.is_base_dc)

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

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

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

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='State', destination_type='Country')
        self.assertIsNotNone(job)

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

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='Commune', destination_type='Department')
        self.assertIsNotNone(job)

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

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

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

        generator = self.get_generator()
        job = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job)

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
        job2 = generator.aggregate_places(
            import_names=[import_name], 
            source_type='County', 
            destination_type='State',
            allow_multiple_to_places=True
        )
        self.assertIsNotNone(job2)

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

        generator = self.get_generator()
        
        # --- ROUND 1: County -> State ---
        job1 = generator.aggregate_places(import_names=[import_name], source_type='County', destination_type='State')
        self.assertIsNotNone(job1)

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

        # --- ROUND 2: State -> Country ---
        # We must pass the output import name of Round 1 as the input to Round 2!
        job2 = generator.aggregate_places(import_names=[f"{import_name}_AggState"], source_type='State', destination_type='Country')
        self.assertIsNotNone(job2)

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


class PlaceAggregationGeneratorCustomDcTest(PlaceAggregationGeneratorIntegrationTest):
    is_base_dc = False


class EntityAggregationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for EntityAggregationGenerator."""

    def get_generator(self) -> EntityAggregationGenerator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return EntityAggregationGenerator(executor, is_base_dc=self.is_base_dc)

    def test_aggregate_earthquakes(self):
        """Tests aggregation of EarthquakeEvents with magnitude constraint and multiple date formats."""
        import_name = 'EarthquakeUSGS'
        output_import = 'EarthquakeUSGS_Agg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"

        # 1. Setup mock data
        # Location (California)
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: Mag 7.2 (Included)
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('eq_1', 'magnitude', '7.2', import_name)

        # eq_2: Mag 6.5 (Filtered out)
        self.add_node('eq_2', 'Earthquake 2', types=['EarthquakeEvent'])
        self.add_edge('eq_2', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_2', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_2', 'occurrenceTime', '2023-06-20T12:00:00Z', import_name)
        self.add_edge('eq_2', 'magnitude', '6.5', import_name)

        # eq_3: Mag 7.0 (Included)
        self.add_node('eq_3', 'Earthquake 3', types=['EarthquakeEvent'])
        self.add_edge('eq_3', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_3', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_3', 'occurrenceTime', '2023-09-01T12:00:00Z', import_name)
        self.add_edge('eq_3', 'magnitude', '7.0', import_name)

        self.flush_to_spanner()

        # 2. Run generator
        config = EntityAggregationConfig(
            entity_types=['EarthquakeEvent'],
            location_props=['affectedPlace'],
            date_prop='occurrenceTime',
            agg_date_formats=['YYYY', 'YYYY-MM'],
            constraints=['magnitude: [7 - M]'],
            output_import=output_import,
            input_imports=[import_name]
        )

        generator = self.get_generator()
        jobs = generator.aggregate_entities([config])
        self.assertEqual(len(jobs), 1)
        jobs[0].result()

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # A. Find the generated SV DCID
            sv_query = """
                SELECT subject_id FROM Edge 
                WHERE predicate = 'populationType' AND object_id = 'EarthquakeEvent'
                  AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'magnitude' AND object_id = '[7 - M]'
                  )
            """
            sv_results = list(snapshot.execute_sql(sv_query))
            self.assertEqual(len(sv_results), 1, "Should generate exactly one SV DCID")
            sv_dcid = sv_results[0][0]
            self.assertTrue(sv_dcid.startswith("dc/sv/gp/"), f"SV DCID should start with dc/sv/gp/, got {sv_dcid}")

            # Verify SV Node exists
            node_query = f"SELECT name, types FROM Node WHERE subject_id = '{sv_dcid}'"
            node_results = list(snapshot.execute_sql(node_query))
            self.assertEqual(len(node_results), 1)
            self.assertIn('StatisticalVariable', node_results[0][1])

            # Verify SV Edges
            edge_query = f"SELECT predicate, object_id, provenance FROM Edge WHERE subject_id = '{sv_dcid}'"
            edges = {r[0]: (r[1], r[2]) for r in snapshot.execute_sql(edge_query)}
            self.assertEqual(edges.get('typeOf'), ('StatisticalVariable', expected_provenance))
            self.assertEqual(edges.get('populationType'), ('EarthquakeEvent', expected_provenance))
            self.assertEqual(edges.get('measuredProperty'), ('count', expected_provenance))
            self.assertEqual(edges.get('statType'), ('measuredValue', expected_provenance))
            self.assertEqual(edges.get('magnitude'), ('[7 - M]', expected_provenance))

            # B. Verify TimeSeries (should have 2: one for P1Y, one for P1M)
            ts_query = f"""
                SELECT facet_id, facet, provenance 
                FROM TimeSeries 
                WHERE variable_measured = '{sv_dcid}' 
                  AND JSON_VALUE(entities, '$.entity1') = 'geoId/06'
            """
            ts_results = list(snapshot.execute_sql(ts_query))
            self.assertEqual(len(ts_results), 2, "Should have 2 TimeSeries (yearly and monthly)")
            
            facets = {r[1]['observationPeriod']: (r[0], r[1], r[2]) for r in ts_results}
            self.assertIn('P1Y', facets)
            self.assertIn('P1M', facets)

            # Verify yearly facet
            facet_id_y, facet_y, prov_y = facets['P1Y']
            self.assertEqual(facet_y['measurementMethod'], 'DataCommonsAggregate')
            self.assertEqual(prov_y, expected_provenance)
            self.assertEqual(facet_y['provenance'], expected_provenance)
            self.assertEqual(facet_y['isDcAggregate'], True)

            # Verify monthly facet
            facet_id_m, facet_m, prov_m = facets['P1M']
            self.assertEqual(facet_m['measurementMethod'], 'DataCommonsAggregate')
            self.assertEqual(prov_m, expected_provenance)
            self.assertEqual(facet_m['provenance'], expected_provenance)
            self.assertEqual(facet_m['isDcAggregate'], True)

            # C. Verify Observations
            # Yearly Obs (2023 -> 2)
            obs_y_query = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{sv_dcid}' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2023' 
                  AND facet_id = '{facet_id_y}'
            """
            obs_y_results = list(snapshot.execute_sql(obs_y_query))
            self.assertEqual(len(obs_y_results), 1)
            self.assertAlmostEqual(float(obs_y_results[0][0]), 2.0)

            # Monthly Obs (2023-05 -> 1, 2023-09 -> 1)
            obs_m_query = f"""
                SELECT date, value FROM Observation 
                WHERE variable_measured = '{sv_dcid}' 
                  AND entity1 = 'geoId/06' 
                  AND facet_id = '{facet_id_m}'
                ORDER BY date
            """
            obs_m_results = list(snapshot.execute_sql(obs_m_query))
            self.assertEqual(len(obs_m_results), 2, "Should have exactly 2 monthly observations")
            self.assertEqual(obs_m_results[0][0], '2023-05')
            self.assertAlmostEqual(float(obs_m_results[0][1]), 1.0)
            self.assertEqual(obs_m_results[1][0], '2023-09')
            self.assertAlmostEqual(float(obs_m_results[1][1]), 1.0)

    def test_aggregate_wildcards_and_default_date(self):
        """Tests aggregation with wildcard constraints (generating multiple SVs) and default date handling."""
        import_name = 'EarthquakeUSGS_Wildcard'
        output_import = 'EarthquakeUSGS_Wildcard_Agg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"

        # 1. Setup mock data
        # Location (California)
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: cause = Tsunami, no date (defaults to current date)
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'cause', 'Tsunami', import_name)

        # eq_2: cause = Tectonic, no date
        self.add_node('eq_2', 'Earthquake 2', types=['EarthquakeEvent'])
        self.add_edge('eq_2', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_2', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_2', 'cause', 'Tectonic', import_name)

        # eq_3: cause = Tsunami, no date
        self.add_node('eq_3', 'Earthquake 3', types=['EarthquakeEvent'])
        self.add_edge('eq_3', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_3', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_3', 'cause', 'Tsunami', import_name)

        self.flush_to_spanner()

        # 2. Run generator (date_prop is omitted -> defaults to current date)
        config = EntityAggregationConfig(
            entity_types=['EarthquakeEvent'],
            location_props=['affectedPlace'],
            date_prop='', 
            agg_date_formats=['YYYY', 'YYYY-MM'],
            constraints=['cause: *'],
            output_import=output_import,
            input_imports=[import_name]
        )

        generator = self.get_generator()
        jobs = generator.aggregate_entities([config])
        self.assertEqual(len(jobs), 1)
        jobs[0].result()

        # Get current dates for assertion
        from datetime import datetime
        current_year = datetime.utcnow().strftime('%Y')
        current_month = datetime.utcnow().strftime('%Y-%m')

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # We expect TWO SVs to be generated (one for Tsunami, one for Tectonic)
            sv_query = """
                SELECT subject_id, object_id FROM Edge 
                WHERE predicate = 'cause' AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'populationType' AND object_id = 'EarthquakeEvent'
                )
                ORDER BY object_id
            """
            sv_results = list(snapshot.execute_sql(sv_query))
            self.assertEqual(len(sv_results), 2, "Should generate exactly two SVs")
            
            # Map cause -> SV DCID
            sv_map = {r[1]: r[0] for r in sv_results}
            self.assertIn('Tsunami', sv_map)
            self.assertIn('Tectonic', sv_map)
            
            tsunami_sv = sv_map['Tsunami']
            tectonic_sv = sv_map['Tectonic']

            # Verify Tsunami SV Edges (should have cause=Tsunami)
            tsunami_edges_query = f"SELECT predicate, object_id FROM Edge WHERE subject_id = '{tsunami_sv}'"
            tsunami_edges = {r[0]: r[1] for r in snapshot.execute_sql(tsunami_edges_query)}
            self.assertEqual(tsunami_edges.get('cause'), 'Tsunami')
            self.assertEqual(tsunami_edges.get('populationType'), 'EarthquakeEvent')

            # Verify Tectonic SV Edges (should have cause=Tectonic)
            tectonic_edges_query = f"SELECT predicate, object_id FROM Edge WHERE subject_id = '{tectonic_sv}'"
            tectonic_edges = {r[0]: r[1] for r in snapshot.execute_sql(tectonic_edges_query)}
            self.assertEqual(tectonic_edges.get('cause'), 'Tectonic')
            self.assertEqual(tectonic_edges.get('populationType'), 'EarthquakeEvent')

            # Verify Observations for Tsunami (Count = 2)
            obs_tsunami_query = f"""
                SELECT date, value FROM Observation 
                WHERE variable_measured = '{tsunami_sv}' 
                  AND entity1 = 'geoId/06'
                ORDER BY date
            """
            obs_tsunami = list(snapshot.execute_sql(obs_tsunami_query))
            # Should have 2 observations (one yearly, one monthly)
            self.assertEqual(len(obs_tsunami), 2)
            # Yearly
            self.assertEqual(obs_tsunami[0][0], current_year)
            self.assertAlmostEqual(float(obs_tsunami[0][1]), 2.0)
            # Monthly
            self.assertEqual(obs_tsunami[1][0], current_month)
            self.assertAlmostEqual(float(obs_tsunami[1][1]), 2.0)

            # Verify Observations for Tectonic (Count = 1)
            obs_tectonic_query = f"""
                SELECT date, value FROM Observation 
                WHERE variable_measured = '{tectonic_sv}' 
                  AND entity1 = 'geoId/06'
                ORDER BY date
            """
            obs_tectonic = list(snapshot.execute_sql(obs_tectonic_query))
            self.assertEqual(len(obs_tectonic), 2)
            # Yearly
            self.assertEqual(obs_tectonic[0][0], current_year)
            self.assertAlmostEqual(float(obs_tectonic[0][1]), 1.0)
            # Monthly
            self.assertEqual(obs_tectonic[1][0], current_month)
            self.assertAlmostEqual(float(obs_tectonic[1][1]), 1.0)

    def test_aggregate_multiple_constraints_and_bounds(self):
        """Tests aggregation with multiple constraints (range with bounds and literal value)."""
        import_name = 'EarthquakeUSGS_MultiCons'
        output_import = 'EarthquakeUSGS_MultiCons_Agg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: Mag 4.0, type MagnitudeMl (Included: 3 <= 4.0 <= 5)
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('eq_1', 'magnitude', '4.0', import_name)
        self.add_edge('eq_1', 'magnitudeType', 'MagnitudeMl', import_name)

        # eq_2: Mag 2.5, type MagnitudeMl (Filtered out: too weak)
        self.add_node('eq_2', 'Earthquake 2', types=['EarthquakeEvent'])
        self.add_edge('eq_2', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_2', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_2', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('eq_2', 'magnitude', '2.5', import_name)
        self.add_edge('eq_2', 'magnitudeType', 'MagnitudeMl', import_name)

        # eq_3: Mag 6.0, type MagnitudeMl (Filtered out: too strong)
        self.add_node('eq_3', 'Earthquake 3', types=['EarthquakeEvent'])
        self.add_edge('eq_3', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_3', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_3', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('eq_3', 'magnitude', '6.0', import_name)
        self.add_edge('eq_3', 'magnitudeType', 'MagnitudeMl', import_name)

        # eq_4: Mag 4.0, type MagnitudeMs (Filtered out: wrong type)
        self.add_node('eq_4', 'Earthquake 4', types=['EarthquakeEvent'])
        self.add_edge('eq_4', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_4', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_4', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('eq_4', 'magnitude', '4.0', import_name)
        self.add_edge('eq_4', 'magnitudeType', 'MagnitudeMs', import_name)

        self.flush_to_spanner()

        # 2. Run generator
        config = EntityAggregationConfig(
            entity_types=['EarthquakeEvent'],
            location_props=['affectedPlace'],
            date_prop='occurrenceTime',
            agg_date_formats=['YYYY'],
            constraints=['magnitude: [3 5 M]', 'magnitudeType: MagnitudeMl'],
            output_import=output_import,
            input_imports=[import_name]
        )

        generator = self.get_generator()
        jobs = generator.aggregate_entities([config])
        self.assertEqual(len(jobs), 1)
        jobs[0].result()

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Find SV
            sv_query = """
                SELECT subject_id FROM Edge 
                WHERE predicate = 'populationType' AND object_id = 'EarthquakeEvent'
                  AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'magnitude' AND object_id = '[3 5 M]'
                  )
                  AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'magnitudeType' AND object_id = 'MagnitudeMl'
                  )
            """
            sv_results = list(snapshot.execute_sql(sv_query))
            self.assertEqual(len(sv_results), 1)
            sv_dcid = sv_results[0][0]

            # Verify count is 1 (only eq_1 satisfies all constraints)
            obs_query = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{sv_dcid}' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2023'
            """
            obs_results = list(snapshot.execute_sql(obs_query))
            self.assertEqual(len(obs_results), 1)
            self.assertAlmostEqual(float(obs_results[0][0]), 1.0)


class EntityAggregationGeneratorCustomDcTest(EntityAggregationGeneratorIntegrationTest):
    is_base_dc = False


class StatVarAggregatorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarAggregator."""

    def get_aggregator(self) -> StatVarAggregator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return StatVarAggregator(executor, is_base_dc=self.is_base_dc)

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
        
        # 2. Run aggregator
        aggregator = self.get_aggregator()
        jobs = aggregator.aggregate_stat_vars(
            ancestor_sv='SV_Parent',
            source_svs=['SV_A', 'SV_B'],
            import_names=[import_name],
            output_import_name=output_import_name,
            skip_all_sources_present_check=False
        )
        self.assertEqual(len(jobs), 1)
        
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
        
        # 2. Run aggregator (strict mode)
        aggregator = self.get_aggregator()
        jobs = aggregator.aggregate_stat_vars(
            ancestor_sv='SV_Parent',
            source_svs=['SV_A', 'SV_B'],
            import_names=[import_name],
            output_import_name=output_import_name,
            skip_all_sources_present_check=False
        )
        self.assertEqual(len(jobs), 1)
        
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
        
        # 2. Run aggregator (lenient mode)
        aggregator = self.get_aggregator()
        jobs = aggregator.aggregate_stat_vars(
            ancestor_sv='SV_Parent',
            source_svs=['SV_A', 'SV_B'],
            import_names=[import_name],
            output_import_name=output_import_name,
            skip_all_sources_present_check=True
        )
        self.assertEqual(len(jobs), 1)
        
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
        
        # 2. Run aggregator
        aggregator = self.get_aggregator()
        jobs = aggregator.aggregate_stat_vars(
            ancestor_sv='SV_Parent',
            source_svs=['SV_A', 'SV_B'],
            import_names=[import_name],
            output_import_name=output_import_name,
            skip_all_sources_present_check=False
        )
        self.assertEqual(len(jobs), 1)
        
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
        
        # 2. Run aggregator
        aggregator = self.get_aggregator()
        jobs = aggregator.aggregate_stat_vars(
            ancestor_sv='SV_Parent',
            source_svs=['SV_A', 'SV_B'],
            import_names=[import_name],
            output_import_name=output_import_name,
            skip_all_sources_present_check=False
        )
        self.assertEqual(len(jobs), 1)
        
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


class StatVarGroupGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarGroupGenerator."""

    def get_generator(self) -> StatVarGroupGenerator:
        executor = BigQueryExecutor(
            BQ_CONNECTION_ID,
            PROJECT_ID,
            SPANNER_INSTANCE_ID,
            SPANNER_DATABASE_ID,
            location=BQ_LOCATION,
            run_sequential=True
        )
        return StatVarGroupGenerator(
            executor,
            is_base_dc=self.is_base_dc,
            max_iterations=2
        )

    def test_stat_var_group_generation(self):
        """
        Tests the generation of StatVarGroups and hierarchical edges from SV specs.
        
        Setup:
          - A vertical spec mapping 'Student' population type to a 'TestVertical' SVG.
          - An unconstrained SV 'Count_Student'.
          - A constrained SV 'Count_Student_Female' (gender=Female).
          - A curated SV 'Median_Age_Student'.
          - A basic populationType SV 'Count_Person'.
        """
        generator = self.get_generator()
        ns = generator.namespace
        prov = generator.generated_provenance
        
        # 1. Setup mock Vertical Node and Spec mappings
        self.add_node(f'{ns}g/TestVertical', 'Test Vertical', value=f'{ns}g/TestVertical', types=['StatVarGroup'])
        self.add_node(f'{ns}g/TestCustomVertical', 'Test Custom Vertical', types=['StatVarGroup'])
        self.add_node('Student', 'Student', value='Student', types=['Class'])
        self.add_node('Person', 'Person', value='Person', types=['Class'])
        
        # Spec mappings
        self.add_edge('Spec_Student', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_Student', 'populationType', 'Student', 'TestImport')
        self.add_edge('Spec_Student', 'vertical', f'{ns}g/TestVertical', 'TestImport')
        self.add_edge('Spec_Person', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_Person', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_Person', 'observationProperties', 'measuredProperty=count', 'TestImport')
        self.add_edge('Spec_Person', 'vertical', f'{ns}g/TestVertical', 'TestImport')
        self.add_edge(f'{ns}g/TestVertical', 'specializationOf', f'{ns}g/Root', 'TestImport')
        self.add_edge(f'{ns}g/TestCustomVertical', 'specializationOf', f'{ns}g/Root', 'TestCustomImport')

        # 2. Setup mock SV data
        # Unconstrained Student SV
        self.add_node('Count_Student', 'Count of Students', types=['StatisticalVariable'])
        self.add_edge('Count_Student', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Count_Student', 'populationType', 'Student', 'TestImport')

        # Constrained Student SV (gender = Female)
        self.add_node('Count_Student_Female', 'Female Students', types=['StatisticalVariable'])
        self.add_edge('Count_Student_Female', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Count_Student_Female', 'populationType', 'Student', 'TestImport')
        self.add_edge('Count_Student_Female', 'constraintProperties', 'gender', 'TestImport')
        self.add_edge('Count_Student_Female', 'gender', 'Female', 'TestImport')

        # SV with curated grouping
        self.add_node('Median_Age_Student', 'Median age of students', types=['StatisticalVariable'])
        self.add_edge('Median_Age_Student', 'typeOf', 'StatisticalVariable', 'TestCustomImport')
        self.add_edge('Median_Age_Student', 'populationType', 'Student', 'TestCustomImport')
        self.add_edge('Median_Age_Student', 'memberOf', f'{ns}g/TestCustomVertical', 'TestCustomImport')

        # SV with basic populationType
        self.add_node('Count_Person', 'Population', types=['StatisticalVariable'])
        self.add_edge('Count_Person', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Count_Person', 'populationType', 'Person', 'TestImport')
        self.add_edge('Count_Person', 'measuredProperty', 'count', 'TestImport')
        
        self.flush_to_spanner()

        # 2. Run generator
        jobs = generator.run_all(import_names=['Schema'])
        self.assertIsNotNone(jobs)
        for job in jobs:
            job.result()

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Check newly created SVG nodes (Should generate root 'Student' and constrained 'Student_Gender-Female')
            node_query = """
                SELECT subject_id 
                FROM Node 
                WHERE 'StatVarGroup' IN UNNEST(types) 
                  AND subject_id LIKE '%/g/Student%'
                ORDER BY subject_id
            """
            nodes = [r[0] for r in snapshot.execute_sql(node_query)]
            self.assertIn(f'{ns}g/Student', nodes)
            self.assertIn(f'{ns}g/Student_Gender', nodes)
            self.assertIn(f'{ns}g/Student_Gender-Female', nodes)

            # Check linkedMemberOf/memberOf attachments
            edge_query = """
                SELECT subject_id, predicate, object_id, provenance
                FROM Edge 
                WHERE predicate IN ('memberOf', 'specializationOf', 'linkedMemberOf')
                ORDER BY subject_id, predicate, object_id
            """
            edges = [(r[0], r[1], r[2], r[3]) for r in snapshot.execute_sql(edge_query)]

            # Verify unconstrained SV attached directly to the Student Root SVG
            self.assertIn(('Count_Student', 'memberOf', f'{ns}g/Student', prov), edges)

            # Verify unconstrained SV attached to ancestor SVGs
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/Root', prov), edges)
            
            # Verify constrained SV attached to the constrained SVG
            self.assertIn(('Count_Student_Female', 'memberOf', f'{ns}g/Student_Gender-Female', prov), edges)

            # Verify constrained SV attached to ancestor SVGs
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Student_Gender-Female', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Student_Gender', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Root', prov), edges)

            # Verify basic populationType SV attached to SVG by mprop
            self.assertIn(('Count_Person', 'memberOf', f'{ns}g/TestVertical', prov), edges)

            # Verify basic populationType SV attached to ancestor SVGs
            self.assertIn(('Count_Person', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Count_Person', 'linkedMemberOf', f'{ns}g/Root', prov), edges)
            
            # Verify hierarchical specialization of generated SVGs
            self.assertIn((f'{ns}g/Student_Gender-Female', 'specializationOf', f'{ns}g/Student_Gender', prov), edges)
            self.assertIn((f'{ns}g/Student_Gender', 'specializationOf', f'{ns}g/Student', prov), edges)

            # Verify the root SVG attached to the Vertical declared in the Spec
            self.assertIn((f'{ns}g/Student', 'specializationOf', f'{ns}g/TestVertical', prov), edges)

            # Verify curated SVs attached to ancestor SVs based on curated hierarchy
            self.assertNotIn(('Median_Age_Student', 'memberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/TestCustomVertical', prov), edges)
            self.assertIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/Root', prov), edges)


class StatVarGroupGeneratorCustomDcTest(StatVarGroupGeneratorIntegrationTest):
    is_base_dc = False


if __name__ == '__main__':
    unittest.main()