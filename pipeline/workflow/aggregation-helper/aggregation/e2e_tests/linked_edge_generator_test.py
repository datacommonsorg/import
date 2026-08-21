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
"""Integration E2E tests for Data Commons LinkedEdgeGenerator.

Covers:
- LinkedEdgeGenerator (generating linked edges for graph structures such as
  containedInPlace, memberOf, and member relationships, including cycles and idempotency).

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
    uv run pytest aggregation/e2e_tests/linked_edge_generator_test.py -s
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
from aggregation import BigQueryExecutor, LinkedEdgeGenerator
from aggregation.common import TOPIC_LIST_PROVENANCE, get_provenance_name


class LinkedEdgeGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration tests for LinkedEdgeGenerator."""

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
        
        calculations = [
            {
                "name": "Linked Edges ContainedInPlace",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results in Spanner
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
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
        
        calculations = [
            {
                "name": "Linked Edges MemberOf",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedMemberOf'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
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
        
        calculations = [
            {
                "name": "Linked Edges Member",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedMember'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
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
        
        calculations = [
            {
                "name": "Linked Edges Cycle",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 3. Verify results
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
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
        self.add_edge('geoId/06075', 'linkedContainedInPlace', 'geoId/06', f'generated/{import_name}')
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Linked Edges Idempotency",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        # 4. Verify no new edges were written (only the 1 pre-inserted edge exists)
        with self.database.snapshot() as snapshot:
            query = """
                SELECT subject_id, object_id, provenance 
                FROM Edge 
                WHERE predicate = 'linkedContainedInPlace'
                ORDER BY subject_id, object_id
            """
            results = list(snapshot.execute_sql(query))
            
            expected_provenance = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
            self.assertEqual(len(results), 1)
            self.assertEqual(tuple(results[0]), ('geoId/06075', 'geoId/06', expected_provenance))

    def test_linked_edges_multiple_imports(self):
        """Tests that passing multiple imports simultaneously assigns distinct scoped provenances per row."""
        import_a = 'ImportA_MultiTest'
        import_b = 'ImportB_MultiTest'
        self.add_node('geoId/06075', 'San Francisco County', types=['County'])
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_node('country/USA', 'United States', types=['Country'])
        self.add_node('geoId/36061', 'New York County', types=['County'])
        self.add_node('geoId/36', 'New York', types=['State'])
        
        self.add_edge('geoId/06075', 'containedInPlace', 'geoId/06', import_a)
        self.add_edge('geoId/06', 'containedInPlace', 'country/USA', import_a)
        self.add_edge('geoId/36061', 'containedInPlace', 'geoId/36', import_b)
        self.add_edge('geoId/36', 'containedInPlace', 'country/USA', import_b)
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Linked Edges Multiple Imports",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_a, import_b]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_a, import_b])
        self.assertTrue(res.success)
        
        with self.database.snapshot(multi_use=True) as snapshot:
            prov_a = 'dc/base/generated/ImportA_MultiTest' if self.is_base_dc else 'generated/ImportA_MultiTest'
            prov_b = 'dc/base/generated/ImportB_MultiTest' if self.is_base_dc else 'generated/ImportB_MultiTest'
            res_a = list(snapshot.execute_sql(f"SELECT subject_id FROM Edge WHERE provenance = '{prov_a}' AND predicate = 'linkedContainedInPlace'"))
            res_b = list(snapshot.execute_sql(f"SELECT subject_id FROM Edge WHERE provenance = '{prov_b}' AND predicate = 'linkedContainedInPlace'"))
            self.assertEqual(len(res_a), 3, "ImportA should have 3 scoped linked edges.")
            self.assertEqual(len(res_b), 3, "ImportB should have 3 scoped linked edges.")


    def test_topic_and_svpg_list_edges(self):
        """Tests materialization of relevantVariableList and memberList edges and literal nodes."""
        import_name = 'TopicTest_Import'
        
        # 1. Setup mock topics and SVPGs
        self.add_node('dc/topic/Environment', 'Environment', types=['Topic'])
        self.add_node('custom/topic/AirQuality', 'Air Quality')  # Type provided via typeOf edge
        self.add_node('dc/svpg/AgeGroups', 'Age Groups', types=['StatVarPeerGroup'])
        
        self.add_edge('custom/topic/AirQuality', 'typeOf', 'Topic', import_name)
        self.add_edge('dc/svpg/AgeGroups', 'typeOf', 'StatVarPeerGroup', import_name)
        
        # Add relevantVariable and member arcs
        self.add_edge('dc/topic/Environment', 'relevantVariable', 'Count_Person', import_name)
        self.add_edge('dc/topic/Environment', 'relevantVariable', 'dc/topic/Water', import_name)
        
        self.add_edge('custom/topic/AirQuality', 'relevantVariable', 'AirPollution_PM25', import_name)
        self.add_edge('custom/topic/AirQuality', 'relevantVariable', 'AirPollution_O3', import_name)
        
        self.add_edge('dc/svpg/AgeGroups', 'member', 'Count_Person_18To64', import_name)
        self.add_edge('dc/svpg/AgeGroups', 'member', 'Count_Person_0To17', import_name)
        
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Linked Edges With Topic Lists",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name],
                "generate_topic_list_edges": True
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        expected_provenance = get_provenance_name(TOPIC_LIST_PROVENANCE, self.is_base_dc)
        
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify Edge records
            edge_query = """
                SELECT subject_id, predicate, provenance
                FROM Edge
                WHERE predicate IN ('relevantVariableList', 'memberList')
                ORDER BY subject_id
            """
            edges = list(snapshot.execute_sql(edge_query))
            self.assertEqual(len(edges), 3)
            self.assertEqual(tuple(edges[0]), ('custom/topic/AirQuality', 'relevantVariableList', expected_provenance))
            self.assertEqual(tuple(edges[1]), ('dc/svpg/AgeGroups', 'memberList', expected_provenance))
            self.assertEqual(tuple(edges[2]), ('dc/topic/Environment', 'relevantVariableList', expected_provenance))
            
            # Verify Node records contain the aggregated CSV string values
            node_query = """
                SELECT n.value
                FROM Node n
                JOIN Edge e ON n.subject_id = e.object_id
                WHERE e.predicate IN ('relevantVariableList', 'memberList')
                ORDER BY e.subject_id
            """
            nodes = [r[0] for r in snapshot.execute_sql(node_query)]
            self.assertEqual(nodes, [
                'AirPollution_O3,AirPollution_PM25',
                'Count_Person_0To17,Count_Person_18To64',
                'Count_Person,dc/topic/Water'
            ])

    def test_topic_list_edges_disabled(self):
        """Tests that disabling generate_topic_list_edges skips topic/SVPG list edge materialization."""
        import_name = 'TopicDisabledTest_Import'
        
        self.add_node('dc/topic/Environment', 'Environment', types=['Topic'])
        self.add_edge('dc/topic/Environment', 'relevantVariable', 'Count_Person', import_name)
        self.flush_to_spanner()
        
        calculations = [
            {
                "name": "Linked Edges Without Topic Lists",
                "type": "LINKED_EDGES",
                "stage": 1,
                "input_imports": [import_name],
                "generate_topic_list_edges": False
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)
        
        with self.database.snapshot() as snapshot:
            query = "SELECT count(*) FROM Edge WHERE predicate = 'relevantVariableList'"
            count = list(snapshot.execute_sql(query))[0][0]
            self.assertEqual(count, 0)


class LinkedEdgeGeneratorCustomDcTest(LinkedEdgeGeneratorIntegrationTest):
    is_base_dc = False
