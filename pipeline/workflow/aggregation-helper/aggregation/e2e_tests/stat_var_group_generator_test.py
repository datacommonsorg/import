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
- DependentPropertyValue (DPV) matching and stripping.
- Single-child SVG pruning (simple chain and DAG fan-out cases).

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


    def test_pruning_single_child_svgs(self):
        """
        Tests pruning of single-child SVGs in a simple chain (non-DAG) hierarchy.

        Without pruning, Count_Student_Female produces a chain:
          TestVertical → Student → Student_Gender → Student_Gender-Female → Count_Student_Female

        With pruning enabled:
          - Student (2 children: Student_Gender + Count_Student) → NOT pruned
          - Student_Gender (1 child: Student_Gender-Female) → pruned
          - Student_Gender-Female (1 child: Count_Student_Female) → pruned
          - Count_Student_Female rolls up to Student (nearest non-pruned ancestor)

        Result: Student_Gender and Student_Gender-Female nodes/edges removed.
        Count_Student_Female gets memberOf Student (redirected).
        """
        ns = 'dc/' if self.is_base_dc else 'c/'
        prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        self._setup_mock_data(ns)

        calculations = [
            {
                "name": "StatVar Groups Generation with Pruning",
                "type": "STAT_VAR_GROUPS",
                "stage": 1,
                "input_imports": ["TestImport"],
                "should_prune_single_child_svgs": True
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=["TestImport"])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify pruned SVG nodes are removed, non-pruned ones remain
            node_query = """
                SELECT subject_id 
                FROM Node 
                WHERE 'StatVarGroup' IN UNNEST(types) 
                  AND subject_id LIKE '%/g/Student%'
                ORDER BY subject_id
            """
            nodes = [r[0] for r in snapshot.execute_sql(node_query)]

            # Student has 2 children initially → NOT pruned
            self.assertIn(f'{ns}g/Student', nodes)
            # Student_Gender had 1 child → pruned
            self.assertNotIn(f'{ns}g/Student_Gender', nodes)
            # Student_Gender-Female had 1 child → pruned
            self.assertNotIn(f'{ns}g/Student_Gender-Female', nodes)

            edge_query = """
                SELECT subject_id, predicate, object_id, provenance
                FROM Edge 
                WHERE predicate IN ('memberOf', 'specializationOf', 'linkedMemberOf')
                ORDER BY subject_id, predicate, object_id
            """
            edges = [(r[0], r[1], r[2], r[3]) for r in snapshot.execute_sql(edge_query)]

            # --- Constrained SV: Count_Student_Female ---
            # Redirected: memberOf should point to Student (nearest non-pruned ancestor)
            self.assertIn(('Count_Student_Female', 'memberOf', f'{ns}g/Student', prov), edges)

            # linkedMemberOf to non-pruned ancestors should be retained
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Count_Student_Female', 'linkedMemberOf', f'{ns}g/Root', prov), edges)

            # Edges involving pruned SVGs should be completely removed
            self.assertNotIn(('Count_Student_Female', 'memberOf',
                f'{ns}g/Student_Gender-Female', prov), edges)
            self.assertNotIn(('Count_Student_Female', 'linkedMemberOf',
                f'{ns}g/Student_Gender-Female', prov), edges)
            self.assertNotIn(('Count_Student_Female', 'linkedMemberOf',
                f'{ns}g/Student_Gender', prov), edges)
            self.assertNotIn((f'{ns}g/Student_Gender', 'specializationOf',
                f'{ns}g/Student', prov), edges)
            self.assertNotIn((f'{ns}g/Student_Gender-Female', 'specializationOf',
                f'{ns}g/Student_Gender', prov), edges)

            # --- Unconstrained SV: Count_Student ---
            # Unaffected by pruning (Student not pruned, has 2 children)
            self.assertIn(('Count_Student', 'memberOf', f'{ns}g/Student', prov), edges)
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/Student', prov), edges)
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/TestVertical', prov), edges)
            self.assertIn(('Count_Student', 'linkedMemberOf', f'{ns}g/Root', prov), edges)

            # --- Other SVs unaffected ---
            # Count_Person (basic popType, attached to TestVertical)
            self.assertIn(('Count_Person', 'memberOf', f'{ns}g/TestVertical', prov), edges)
            # Count_Thing (uncategorized)
            self.assertIn(('Count_Thing', 'memberOf', f'{ns}g/Uncategorized_Variables', prov), edges)

    def test_pruning_dag_fanout(self):
        """
        Tests pruning when multi-cprop SVGs create a diamond DAG structure.

        Count_Military_Person has 2 cprops (armedForcesStatus, veteranStatus).
        The iterative loop generates a DAG by removing one cprop at a time:

          MilitaryService (vertical, NOT pruned)
            ├── Person_ArmedForcesStatus (1 child → pruned)
            └── Person_VeteranStatus (1 child → pruned)
                  └── Person_ArmedForcesStatus_VeteranStatus (1 child → pruned)
                        └── Count_Military_Person (SV)

        The 2-cprop SVG has TWO parents via specializationOf (diamond DAG).
        When pruned, the walk-up follows both paths, both converging at
        MilitaryService. The recursive CTE must:
          1. Explore both paths through the DAG
          2. Pick the nearest non-pruned ancestor (MilitaryService)
          3. Produce exactly ONE redirected memberOf edge (no duplicates)
          4. Remove all pruned SVG nodes and their edges
        """
        ns = 'dc/' if self.is_base_dc else 'c/'
        prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        self._setup_dpv_mock_data(ns)

        calculations = [
            {
                "name": "StatVar Groups Generation with DAG Pruning",
                "type": "STAT_VAR_GROUPS",
                "stage": 1,
                "input_imports": ["TestImport"],
                "should_prune_single_child_svgs": True
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=["TestImport"])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify all military SVGs are pruned (each had exactly 1 child)
            node_query = """
                SELECT subject_id
                FROM Node
                WHERE 'StatVarGroup' IN UNNEST(types)
                  AND subject_id LIKE '%/g/Person_%'
                ORDER BY subject_id
            """
            nodes = [r[0] for r in snapshot.execute_sql(node_query)]
            self.assertNotIn(f'{ns}g/Person_ArmedForcesStatus', nodes)
            self.assertNotIn(f'{ns}g/Person_VeteranStatus', nodes)
            self.assertNotIn(f'{ns}g/Person_ArmedForcesStatus_VeteranStatus', nodes)

            edge_query = """
                SELECT subject_id, predicate, object_id, provenance
                FROM Edge
                WHERE predicate IN ('memberOf', 'specializationOf', 'linkedMemberOf')
                ORDER BY subject_id, predicate, object_id
            """
            edges = [(r[0], r[1], r[2], r[3]) for r in snapshot.execute_sql(edge_query)]

            # --- Count_Military_Person: redirected to MilitaryService ---
            # All 3 SVGs pruned; both DAG paths converge at MilitaryService (vertical).
            # Exactly ONE redirected memberOf edge (no duplicates from DAG fan-out).
            military_member_edges = [
                e for e in edges
                if e[0] == 'Count_Military_Person' and e[1] == 'memberOf'
            ]
            self.assertEqual(len(military_member_edges), 1,
                f'Expected exactly 1 memberOf edge for Count_Military_Person, '
                f'got {len(military_member_edges)}: {military_member_edges}')
            self.assertIn(('Count_Military_Person', 'memberOf', f'{ns}g/MilitaryService', prov), edges)

            # linkedMemberOf to non-pruned ancestors retained
            self.assertIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/MilitaryService', prov), edges)
            self.assertIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Root', prov), edges)

            # No edges involving pruned SVGs
            self.assertNotIn(('Count_Military_Person', 'memberOf',
                f'{ns}g/Person_ArmedForcesStatus_VeteranStatus', prov), edges)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Person_ArmedForcesStatus_VeteranStatus', prov), edges)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Person_ArmedForcesStatus', prov), edges)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Person_VeteranStatus', prov), edges)
            self.assertNotIn((f'{ns}g/Person_ArmedForcesStatus_VeteranStatus',
                'specializationOf', f'{ns}g/Person_ArmedForcesStatus', prov), edges)
            self.assertNotIn((f'{ns}g/Person_ArmedForcesStatus_VeteranStatus',
                'specializationOf', f'{ns}g/Person_VeteranStatus', prov), edges)
            self.assertNotIn((f'{ns}g/Person_ArmedForcesStatus',
                'specializationOf', f'{ns}g/MilitaryService', prov), edges)
            self.assertNotIn((f'{ns}g/Person_VeteranStatus',
                'specializationOf', f'{ns}g/MilitaryService', prov), edges)

            # --- Other SVs unaffected by pruning ---
            # Median_Income_Person: 0 cprops after DPV stripping → attached to Demographics
            # No SVGs generated for it, so pruning has no effect.
            self.assertIn(('Median_Income_Person', 'memberOf',
                f'{ns}g/Demographics', prov), edges)
            self.assertIn(('Median_Income_Person', 'linkedMemberOf',
                f'{ns}g/Demographics', prov), edges)
            self.assertIn(('Median_Income_Person', 'linkedMemberOf',
                f'{ns}g/Root', prov), edges)

            # Median_Income_Person_Over20: 1 cprop (age) → Person_Age-Years20Onwards generated
            # Person_Age has 1 child (Person_Age-Years20Onwards) → pruned
            # Person_Age-Years20Onwards has 1 child (the SV) → pruned
            # SV redirected to nearest non-pruned ancestor (Uncategorized, since no vertical)
            over20_member_edges = [
                e for e in edges
                if e[0] == 'Median_Income_Person_Over20' and e[1] == 'memberOf'
            ]
            self.assertEqual(len(over20_member_edges), 1,
                f'Expected exactly 1 memberOf edge for Median_Income_Person_Over20, '
                f'got {len(over20_member_edges)}: {over20_member_edges}')

    def _setup_dpv_mock_data(self, ns):
        """Setup mock data for DPV (dependentPropertyValue) matching tests."""
        # Verticals
        self.add_node(f'{ns}g/Demographics', 'Demographics', value=f'{ns}g/Demographics', types=['StatVarGroup'])
        self.add_node(f'{ns}g/MilitaryService', 'Military Service', value=f'{ns}g/MilitaryService', types=['StatVarGroup'])
        self.add_node('Person', 'Person', value='Person', types=['Class'])
        self.add_edge(f'{ns}g/Demographics', 'specializationOf', f'{ns}g/Root', 'TestImport')
        self.add_edge(f'{ns}g/MilitaryService', 'specializationOf', f'{ns}g/Root', 'TestImport')

        # Create Node entries for statVarProperties and dependentPropertyValue values.
        # The SpecValues query resolves these object_ids to Node.value via a JOIN.
        # These Nodes must have no types (types IS NULL OR ARRAY_LENGTH(types) = 0).
        self.add_node('svProp_measuredProperty_income', value='measuredProperty=income')
        self.add_node('dpv_age_Years15Onwards', value='age=Years15Onwards')
        self.add_node('dpv_age_Years20Onwards', value='age=Years20Onwards')
        self.add_node('dpv_incomeStatus_WithIncome', value='incomeStatus=WithIncome')

        # Spec 1: DPV spec with 2 DPVs, 0 cprops, vertical=Demographics
        # SVs with cprops exactly {age, incomeStatus} and matching pv values will match this spec.
        # Both pvs get stripped → 0 remaining cprops → SV attached to Demographics.
        self.add_edge('Spec_DPV_Full', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_DPV_Full', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_DPV_Full', 'statVarProperties', 'svProp_measuredProperty_income', 'TestImport')
        self.add_edge('Spec_DPV_Full', 'dependentPropertyValue', 'dpv_age_Years15Onwards', 'TestImport')
        self.add_edge('Spec_DPV_Full', 'dependentPropertyValue', 'dpv_incomeStatus_WithIncome', 'TestImport')
        self.add_edge('Spec_DPV_Full', 'vertical', f'{ns}g/Demographics', 'TestImport')

        # Spec 2: DPV spec with 1 DPV, 1 cprop (age), no vertical
        # SVs with cprops exactly {age, incomeStatus} where incomeStatus=WithIncome will match.
        # incomeStatus gets stripped, age remains → hierarchy uses age only.
        self.add_edge('Spec_DPV_Partial', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_DPV_Partial', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_DPV_Partial', 'statVarProperties', 'svProp_measuredProperty_income', 'TestImport')
        self.add_edge('Spec_DPV_Partial', 'constraintProperties', 'age', 'TestImport')
        self.add_edge('Spec_DPV_Partial', 'dependentPropertyValue', 'dpv_incomeStatus_WithIncome', 'TestImport')

        # Spec 3: DPV spec with 2 DPVs, 2 cprops (armedForcesStatus, veteranStatus), vertical=MilitaryService
        # SVs with cprops exactly {armedForcesStatus, veteranStatus, age, incomeStatus} and matching
        # DPV values will match. DPVs stripped → [armedForcesStatus, veteranStatus] remain.
        # NOTE: This spec has 2 cprops, so it's EXCLUDED from VerticalSpec (only 0-1 cprop specs
        # participate in vertical matching). It's only used for DPV stripping.
        # The 1-cprop specs below handle vertical attachment for the individual cprops.
        self.add_edge('Spec_DPV_Military', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'statVarProperties', 'svProp_measuredProperty_income', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'constraintProperties', 'armedForcesStatus', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'constraintProperties', 'veteranStatus', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'dependentPropertyValue', 'dpv_age_Years15Onwards', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'dependentPropertyValue', 'dpv_incomeStatus_WithIncome', 'TestImport')
        self.add_edge('Spec_DPV_Military', 'vertical', f'{ns}g/MilitaryService', 'TestImport')

        # Spec 4: 1-cprop vertical spec for armedForcesStatus → MilitaryService
        # (Not a DPV spec — used for vertical matching of Person_ArmedForcesStatus SVG)
        self.add_edge('Spec_ArmedForces', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_ArmedForces', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_ArmedForces', 'statVarProperties', 'svProp_measuredProperty_income', 'TestImport')
        self.add_edge('Spec_ArmedForces', 'constraintProperties', 'armedForcesStatus', 'TestImport')
        self.add_edge('Spec_ArmedForces', 'vertical', f'{ns}g/MilitaryService', 'TestImport')

        # Spec 5: 1-cprop vertical spec for veteranStatus → MilitaryService
        # (Not a DPV spec — used for vertical matching of Person_VeteranStatus SVG)
        self.add_edge('Spec_Veteran', 'typeOf', 'StatVarGroupSpec', 'TestImport')
        self.add_edge('Spec_Veteran', 'populationType', 'Person', 'TestImport')
        self.add_edge('Spec_Veteran', 'statVarProperties', 'svProp_measuredProperty_income', 'TestImport')
        self.add_edge('Spec_Veteran', 'constraintProperties', 'veteranStatus', 'TestImport')
        self.add_edge('Spec_Veteran', 'vertical', f'{ns}g/MilitaryService', 'TestImport')

        # SV 1: Exact DPV match with Spec_DPV_Full (2 DPVs, 0 cprops)
        # cprops = {age, incomeStatus}, age=Years15Onwards, incomeStatus=WithIncome
        # → Both specs match, but Spec_DPV_Full wins (2 DPVs > 1 DPV)
        # → Both pvs stripped → 0 cprops → attached to Demographics
        self.add_node('Median_Income_Person', 'Median Income Person', types=['StatisticalVariable'])
        self.add_edge('Median_Income_Person', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Median_Income_Person', 'populationType', 'Person', 'TestImport')
        self.add_edge('Median_Income_Person', 'measuredProperty', 'income', 'TestImport')
        self.add_edge('Median_Income_Person', 'constraintProperties', 'age', 'TestImport')
        self.add_edge('Median_Income_Person', 'constraintProperties', 'incomeStatus', 'TestImport')
        self.add_edge('Median_Income_Person', 'age', 'Years15Onwards', 'TestImport')
        self.add_edge('Median_Income_Person', 'incomeStatus', 'WithIncome', 'TestImport')

        # SV 2: DPV value mismatch → Spec_DPV_Full doesn't match (age=Years20Onwards)
        # → Spec_DPV_Partial matches → incomeStatus stripped, age remains
        # → 1 cprop (age) → hierarchy generates Person_Age SVG (uncategorized, no vertical)
        self.add_node('Median_Income_Person_Over20', 'Median Income Person Over 20', types=['StatisticalVariable'])
        self.add_edge('Median_Income_Person_Over20', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'populationType', 'Person', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'measuredProperty', 'income', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'constraintProperties', 'age', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'constraintProperties', 'incomeStatus', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'age', 'Years20Onwards', 'TestImport')
        self.add_edge('Median_Income_Person_Over20', 'incomeStatus', 'WithIncome', 'TestImport')

        # SV 3: Exact cprops match with Spec_DPV_Military only
        # cprops = {armedForcesStatus, veteranStatus, age, incomeStatus}
        # → Spec_DPV_Full doesn't match (cprops {age, incomeStatus} != {armedForcesStatus, ...})
        # → Spec_DPV_Military matches → DPVs stripped → [armedForcesStatus, veteranStatus] remain
        # → hierarchy generates SVGs under MilitaryService
        self.add_node('Count_Military_Person', 'Count Military Person', types=['StatisticalVariable'])
        self.add_edge('Count_Military_Person', 'typeOf', 'StatisticalVariable', 'TestImport')
        self.add_edge('Count_Military_Person', 'populationType', 'Person', 'TestImport')
        self.add_edge('Count_Military_Person', 'measuredProperty', 'income', 'TestImport')
        self.add_edge('Count_Military_Person', 'constraintProperties', 'armedForcesStatus', 'TestImport')
        self.add_edge('Count_Military_Person', 'constraintProperties', 'veteranStatus', 'TestImport')
        self.add_edge('Count_Military_Person', 'constraintProperties', 'age', 'TestImport')
        self.add_edge('Count_Military_Person', 'constraintProperties', 'incomeStatus', 'TestImport')
        self.add_edge('Count_Military_Person', 'armedForcesStatus', 'Active', 'TestImport')
        self.add_edge('Count_Military_Person', 'veteranStatus', 'Veteran', 'TestImport')
        self.add_edge('Count_Military_Person', 'age', 'Years15Onwards', 'TestImport')
        self.add_edge('Count_Military_Person', 'incomeStatus', 'WithIncome', 'TestImport')

        self.flush_to_spanner()

    def test_dpv_matching(self):
        """
        Tests dependentPropertyValue (DPV) matching and stripping in SVG generation.

        Setup:
          - Spec_DPV_Full: 2 DPVs (age=Years15Onwards, incomeStatus=WithIncome),
            0 cprops, vertical=Demographics, statVarProperties=measuredProperty=income
          - Spec_DPV_Partial: 1 DPV (incomeStatus=WithIncome), 1 cprop (age),
            no vertical, statVarProperties=measuredProperty=income
          - Spec_DPV_Military: 2 DPVs, 2 cprops (armedForcesStatus, veteranStatus),
            vertical=MilitaryService, statVarProperties=measuredProperty=income.
            NOTE: Excluded from VerticalSpec (2 cprops); only used for DPV stripping.
          - Spec_ArmedForces: 1 cprop (armedForcesStatus), vertical=MilitaryService.
            Handles vertical matching for Person_ArmedForcesStatus SVG.
          - Spec_Veteran: 1 cprop (veteranStatus), vertical=MilitaryService.
            Handles vertical matching for Person_VeteranStatus SVG.

        SVs:
          - Median_Income_Person: cprops={age, incomeStatus}, age=Years15Onwards,
            incomeStatus=WithIncome → matches Spec_DPV_Full (most specific: 2 DPVs)
            → both pvs stripped → 0 cprops → attached to Demographics
          - Median_Income_Person_Over20: same but age=Years20Onwards → Spec_DPV_Full
            doesn't match (exact value mismatch) → Spec_DPV_Partial matches →
            incomeStatus stripped, age remains → NOT attached to Demographics
          - Count_Military_Person: cprops={armedForcesStatus, veteranStatus, age,
            incomeStatus} → only Spec_DPV_Military matches (exact cprops) →
            DPVs stripped → [armedForcesStatus, veteranStatus] remain →
            SVGs generated under MilitaryService
        """
        ns = 'dc/' if self.is_base_dc else 'c/'
        prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        self._setup_dpv_mock_data(ns)

        calculations = [
            {
                "name": "StatVar Groups Generation with DPV",
                "type": "STAT_VAR_GROUPS",
                "stage": 1,
                "input_imports": ["TestImport"]
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=["TestImport"])
        self.assertTrue(res.success)

        with self.database.snapshot(multi_use=True) as snapshot:
            edge_query = """
                SELECT subject_id, predicate, object_id, provenance
                FROM Edge 
                WHERE predicate IN ('memberOf', 'specializationOf', 'linkedMemberOf')
                ORDER BY subject_id, predicate, object_id
            """
            edges = [(r[0], r[1], r[2], r[3]) for r in snapshot.execute_sql(edge_query)]

            # --- SV 1: Median_Income_Person ---
            # DPVs fully stripped → 0 cprops → attached to Demographics via SVVerticalEdges
            self.assertIn(('Median_Income_Person', 'memberOf', f'{ns}g/Demographics', prov), edges)
            self.assertIn(('Median_Income_Person', 'linkedMemberOf', f'{ns}g/Demographics', prov), edges)
            self.assertIn(('Median_Income_Person', 'linkedMemberOf', f'{ns}g/Root', prov), edges)

            # Should NOT have any generated SVG hierarchy nodes (0 attrs = no iteration)
            self.assertNotIn(('Median_Income_Person', 'linkedMemberOf',
                f'{ns}g/Person_Age-Years15Onwards', prov), edges)
            self.assertNotIn(('Median_Income_Person', 'linkedMemberOf',
                f'{ns}g/Person_IncomeStatus-WithIncome', prov), edges)

            # --- SV 2: Median_Income_Person_Over20 ---
            # Spec_DPV_Full doesn't match (age=Years20Onwards != Years15Onwards)
            # Spec_DPV_Partial matches → incomeStatus stripped, age remains
            # → NOT attached to Demographics
            self.assertNotIn(('Median_Income_Person_Over20', 'memberOf',
                f'{ns}g/Demographics', prov), edges)
            self.assertNotIn(('Median_Income_Person_Over20', 'linkedMemberOf',
                f'{ns}g/Demographics', prov), edges)

            # Should have age-based SVG hierarchy (incomeStatus stripped, age remains)
            # Person is basic → 1-cprop group (Person_Age) generated in iteration
            self.assertIn(('Median_Income_Person_Over20', 'linkedMemberOf',
                f'{ns}g/Person_Age-Years20Onwards', prov), edges)

            # --- SV 3: Count_Military_Person ---
            # Only Spec_DPV_Military matches (exact cprops match)
            # DPVs stripped → [armedForcesStatus, veteranStatus] remain
            # → 1-cprop SVGs generated under MilitaryService
            self.assertIn((f'{ns}g/Person_ArmedForcesStatus', 'specializationOf',
                f'{ns}g/MilitaryService', prov), edges)
            self.assertIn((f'{ns}g/Person_VeteranStatus', 'specializationOf',
                f'{ns}g/MilitaryService', prov), edges)

            # SV should be linked to MilitaryService vertical
            self.assertIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/MilitaryService', prov), edges)
            self.assertIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Root', prov), edges)

            # SV should NOT be attached to Demographics (different spec)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Demographics', prov), edges)

            # DPV pvs should NOT appear in hierarchy (age and incomeStatus stripped)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Person_Age-Years15Onwards', prov), edges)
            self.assertNotIn(('Count_Military_Person', 'linkedMemberOf',
                f'{ns}g/Person_IncomeStatus-WithIncome', prov), edges)


class StatVarGroupGeneratorCustomDcTest(StatVarGroupGeneratorIntegrationTest):
    is_base_dc = False
