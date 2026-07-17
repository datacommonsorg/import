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
"""Integration E2E tests for Data Commons StatVarGroupGenerator.

Covers:
- StatVarGroupGenerator (generating StatVarGroups and hierarchical edges from SV
  specifications, vertical specialization hierarchy, linking unconstrained and
  constrained variables, curated groups, and uncategorized variables).

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
    uv run pytest aggregation/e2e_tests/stat_var_group_generator_test.py -s
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
from aggregation import BigQueryExecutor, StatVarGroupGenerator


class StatVarGroupGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for StatVarGroupGenerator."""

    def _setup_mock_data(self, ns):
        # 1. Setup mock Vertical Node and Spec mappings
        self.add_node(f'{ns}g/TestVertical', 'Test Vertical', value=f'{ns}g/TestVertical', types=['StatVarGroup'])
        self.add_node(f'{ns}g/TestCustomVertical', 'Test Custom Vertical', types=['StatVarGroup'])
        self.add_node('Student', 'Student', value='Student', types=['Class'])
        self.add_node('Person', 'Person', value='Person', types=['Class'])
        self.add_node('Thing', 'Thing', value='Thing', types=['Class'])
        
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
        
        # Uncategorized SV with basic populationType
        self.add_node('Count_Thing', 'Population', types=['StatisticalVariable'])
        self.add_edge('Count_Thing', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Count_Thing', 'populationType', 'Thing', 'TestImport')
        self.add_edge('Count_Thing', 'measuredProperty', 'count', 'TestImport')

        self.flush_to_spanner()

    def test_stat_var_group_generation(self):
        """
        Tests the generation of StatVarGroups and hierarchical edges from SV specs.
        
        Setup:
          - A vertical spec mapping 'Student' population type to a 'TestVertical' SVG.
          - An unconstrained SV 'Count_Student'.
          - A constrained SV 'Count_Student_Female' (gender=Female).
          - A curated SV 'Median_Age_Student'.
          - A basic populationType SV 'Count_Person'.
          - An uncategorized basic SV 'Count_Thing'.
        """
        ns = 'dc/' if self.is_base_dc else 'c/'
        prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
        
        self._setup_mock_data(ns)

        calculations = [
            {
                "name": "StatVar Groups Generation",
                "type": "STAT_VAR_GROUPS",
                "stage": 1,
                "input_imports": ["TestImport"]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=["TestImport"])
        self.assertTrue(res.success)

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

            # Verify uncategorized basic populationType SV attached to Uncategorized_Variables SVG
            self.assertIn(('Count_Thing', 'memberOf', f'{ns}g/Uncategorized_Variables', prov), edges)

            # Verify uncategorized basic populationType SV attached to ancestor SVGs
            self.assertIn(('Count_Thing', 'linkedMemberOf', f'{ns}g/Uncategorized_Variables', prov), edges)
            self.assertIn(('Count_Thing', 'linkedMemberOf', f'{ns}g/Uncategorized', prov), edges)
            self.assertIn(('Count_Thing', 'linkedMemberOf', f'{ns}g/Root', prov), edges)
            
            # Verify hierarchical specialization of generated SVGs
            self.assertIn((f'{ns}g/Student_Gender-Female', 'specializationOf', f'{ns}g/Student_Gender', prov), edges)
            self.assertIn((f'{ns}g/Student_Gender', 'specializationOf', f'{ns}g/Student', prov), edges)

            # Verify the root SVG attached to the Vertical declared in the Spec
            self.assertIn((f'{ns}g/Student', 'specializationOf', f'{ns}g/TestVertical', prov), edges)

            # Verify curated SVs from OTHER imports are NOT processed
            self.assertNotIn(('Median_Age_Student', 'memberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/TestCustomVertical', prov), edges)
            self.assertNotIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/Root', prov), edges)


    def test_stat_var_group_generation_only_custom_import(self):
        """
        Tests that running for TestCustomImport only processes its data.
        """
        ns = 'dc/' if self.is_base_dc else 'c/'
        prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'
        
        self._setup_mock_data(ns)

        calculations = [
            {
                "name": "StatVar Groups Generation",
                "type": "STAT_VAR_GROUPS",
                "stage": 1,
                "input_imports": ["TestCustomImport"]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=["TestCustomImport"])
        self.assertTrue(res.success)

        # Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Check newly created SVG nodes (Should NOT generate 'Student' groups as they are in TestImport)
            node_query = """
                SELECT subject_id 
                FROM Node 
                WHERE 'StatVarGroup' IN UNNEST(types) 
                  AND subject_id LIKE '%/g/Student%'
                ORDER BY subject_id
            """
            nodes = [r[0] for r in snapshot.execute_sql(node_query)]
            self.assertEqual(len(nodes), 0)

            # Check linkedMemberOf/memberOf attachments
            edge_query = """
                SELECT subject_id, predicate, object_id, provenance
                FROM Edge 
                WHERE predicate IN ('memberOf', 'specializationOf', 'linkedMemberOf')
                ORDER BY subject_id, predicate, object_id
            """
            edges = [(r[0], r[1], r[2], r[3]) for r in snapshot.execute_sql(edge_query)]

            # Verify curated SVs from TestCustomImport ARE processed
            self.assertIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/TestCustomVertical', prov), edges)
            self.assertIn(('Median_Age_Student', 'linkedMemberOf', f'{ns}g/Root', prov), edges)

            # Verify TestImport SVs are NOT processed (no generated edges for them)
            self.assertNotIn(('Count_Student', 'memberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Count_Student', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertNotIn(('Count_Student_Female', 'memberOf', f'{ns}g/Student_Gender-Female', prov), edges)
            self.assertNotIn(('Count_Person', 'memberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertNotIn(('Count_Thing', 'memberOf', f'{ns}g/Uncategorized_Variables', prov), edges)


class StatVarGroupGeneratorCustomDcTest(StatVarGroupGeneratorIntegrationTest):
    is_base_dc = False
