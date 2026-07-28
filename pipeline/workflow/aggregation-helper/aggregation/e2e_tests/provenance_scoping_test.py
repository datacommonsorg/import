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
"""Integration E2E test suite for SQL provenance scoping expressions in live Spanner SQL."""

import unittest
from aggregation.e2e_tests.base import AggregationIntegrationTestBase
from aggregation.common import get_sql_generated_provenance_expr


class ProvenanceScopingSqlIntegrationTest(AggregationIntegrationTestBase):
    """Verifies that SQL generated provenance expressions execute idempotently in live Spanner SQL."""

    is_base_dc = True

    def test_sql_expr_idempotency_live_spanner(self):
        """Tests get_sql_generated_provenance_expr against live Spanner SQL with normal and pre-prefixed data."""
        import_name = 'LiveSql_Test_Import'
        normal_prov = f'dc/base/{import_name}' if self.is_base_dc else import_name
        prefixed_prov = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'

        # Insert edges with both normal and already-generated provenances
        self.add_node('geoId/01', 'Alabama', types=['State'])
        self.add_node('geoId/02', 'Alaska', types=['State'])
        self.add_edge('geoId/01', 'testPred', 'geoId/02', import_name)
        # Manually append already-prefixed edge to mock_edges (bypassing add_edge's auto-prefixing)
        self.mock_edges.append(('geoId/02', 'testPred', 'geoId/01', prefixed_prov))
        self.flush_to_spanner()

        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")
        query = f"""
            SELECT subject_id, provenance, {prov_expr} AS transformed_prov
            FROM Edge
            WHERE predicate = 'testPred'
            ORDER BY subject_id
        """

        with self.database.snapshot() as snapshot:
            results = list(snapshot.execute_sql(query))
            self.assertEqual(len(results), 2)

            # Both the normal provenance and the already-generated provenance must transform identically
            expected_transformed = f'dc/base/generated/{import_name}' if self.is_base_dc else f'generated/{import_name}'
            self.assertEqual(results[0][0], 'geoId/01')
            self.assertEqual(results[0][1], normal_prov)
            self.assertEqual(results[0][2], expected_transformed)

            self.assertEqual(results[1][0], 'geoId/02')
            self.assertEqual(results[1][1], prefixed_prov)
            self.assertEqual(results[1][2], expected_transformed)


class ProvenanceScopingSqlCustomDcTest(ProvenanceScopingSqlIntegrationTest):
    """Runs the live Spanner SQL idempotency tests in Custom DC (non-base DC) mode."""
    is_base_dc = False


if __name__ == '__main__':
    unittest.main()
