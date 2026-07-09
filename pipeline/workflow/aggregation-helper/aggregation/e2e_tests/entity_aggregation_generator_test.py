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
"""Integration E2E tests for Data Commons EntityAggregationGenerator.

Covers:
- EntityAggregationGenerator (aggregating entity nodes into StatisticalVariable count
  observations based on property constraints such as magnitude ranges, wildcards,
  multi-slice brackets on the same property, multiple location properties, latLong
  filtering, missing date safeguards, duplicate edge resilience, multiple entity
  types, and negative/upper/unitless bounds).

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
    uv run pytest aggregation/e2e_tests/entity_aggregation_generator_test.py -s
"""

import unittest
from datetime import datetime
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
    BQ_CONNECTION_ID,
    BQ_LOCATION,
)
from aggregation import BigQueryExecutor, EntityAggregationGenerator, EntityAggregationConfig


class EntityAggregationGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for EntityAggregationGenerator."""

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

        calculations = [
            {
                "name": "Earthquakes Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY", "YYYY-MM"],
                    "constraints": [{"property": "magnitude", "min": 7, "unit": "M"}],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

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

        calculations = [
            {
                "name": "Wildcards and Default Date Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "",
                    "agg_date_formats": ["YYYY", "YYYY-MM"],
                    "constraints": [{"property": "cause", "wildcard": True}],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # Get current dates for assertion
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

        calculations = [
            {
                "name": "Multiple Constraints and Bounds Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [
                        {"property": "magnitude", "min": 3, "max": 5, "unit": "M"},
                        {"property": "magnitudeType", "value": "MagnitudeMl"}
                    ],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

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

    def test_aggregate_multiple_slices_same_property(self):
        """Tests aggregation when config has multiple alternative brackets on the same property (Slice 0 + Slice 1)."""
        import_name = 'EarthquakeUSGS_MultiSlice'
        output_import = 'EarthquakeUSGS_MultiSlice_Agg'
        prefix = "dc/base/" if self.is_base_dc else ""
        expected_provenance = f"{prefix}{output_import}"

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: Mag 3.5 (Included in Slice 0: [3 4 M])
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-08-10T12:00:00Z', import_name)
        self.add_edge('eq_1', 'magnitude', '3.5', import_name)

        # eq_2: Mag 4.5 (Included in Slice 1: [4 5 M])
        self.add_node('eq_2', 'Earthquake 2', types=['EarthquakeEvent'])
        self.add_edge('eq_2', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_2', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_2', 'occurrenceTime', '2023-08-15T12:00:00Z', import_name)
        self.add_edge('eq_2', 'magnitude', '4.5', import_name)

        # eq_3: Mag 3.8 (Included in Slice 0: [3 4 M])
        self.add_node('eq_3', 'Earthquake 3', types=['EarthquakeEvent'])
        self.add_edge('eq_3', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_3', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_3', 'occurrenceTime', '2023-08-20T12:00:00Z', import_name)
        self.add_edge('eq_3', 'magnitude', '3.8', import_name)

        # eq_4: Mag 2.0 (Filtered out by both slices)
        self.add_node('eq_4', 'Earthquake 4', types=['EarthquakeEvent'])
        self.add_edge('eq_4', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_4', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_4', 'occurrenceTime', '2023-08-25T12:00:00Z', import_name)
        self.add_edge('eq_4', 'magnitude', '2.0', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Multiple Slices Same Property Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [
                        {"property": "magnitude", "min": 3, "max": 4, "unit": "M"},
                        {"property": "magnitude", "min": 4, "max": 5, "unit": "M"}
                    ],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify both slices in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Find SV for Slice 0: [3 4 M]
            sv_0_query = """
                SELECT subject_id FROM Edge 
                WHERE predicate = 'populationType' AND object_id = 'EarthquakeEvent'
                  AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'magnitude' AND object_id = '[3 4 M]'
                  )
            """
            sv_0_results = list(snapshot.execute_sql(sv_0_query))
            self.assertEqual(len(sv_0_results), 1)
            sv_0_dcid = sv_0_results[0][0]

            # Verify count for Slice 0 is 2 (eq_1 + eq_3)
            obs_0_query = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{sv_0_dcid}' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2023'
            """
            obs_0_results = list(snapshot.execute_sql(obs_0_query))
            self.assertEqual(len(obs_0_results), 1)
            self.assertAlmostEqual(float(obs_0_results[0][0]), 2.0)

            # Find SV for Slice 1: [4 5 M]
            sv_1_query = """
                SELECT subject_id FROM Edge 
                WHERE predicate = 'populationType' AND object_id = 'EarthquakeEvent'
                  AND subject_id IN (
                    SELECT subject_id FROM Edge 
                    WHERE predicate = 'magnitude' AND object_id = '[4 5 M]'
                  )
            """
            sv_1_results = list(snapshot.execute_sql(sv_1_query))
            self.assertEqual(len(sv_1_results), 1)
            sv_1_dcid = sv_1_results[0][0]

            # Verify count for Slice 1 is 1 (eq_2)
            obs_1_query = f"""
                SELECT value FROM Observation 
                WHERE variable_measured = '{sv_1_dcid}' 
                  AND entity1 = 'geoId/06' 
                  AND date = '2023'
            """
            obs_1_results = list(snapshot.execute_sql(obs_1_query))
            self.assertEqual(len(obs_1_results), 1)
            self.assertAlmostEqual(float(obs_1_results[0][0]), 1.0)

    def test_aggregate_filters_latlong(self):
        """Tests that entities with latLong/ location objects are filtered out."""
        import_name = 'EarthquakeUSGS_LatLong'
        output_import = 'EarthquakeUSGS_LatLong_Agg'

        # 1. Setup mock data
        # Valid place location (California)
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_valid: affectedPlace is geoId/06 (Included)
        self.add_node('eq_valid', 'Valid Earthquake', types=['EarthquakeEvent'])
        self.add_edge('eq_valid', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_valid', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_valid', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        # eq_latlong: affectedPlace is latLong/37.77_-122.41 (Should be filtered out)
        self.add_node('eq_latlong', 'LatLong Earthquake', types=['EarthquakeEvent'])
        self.add_edge('eq_latlong', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_latlong', 'affectedPlace', 'latLong/37.77_-122.41', import_name)
        self.add_edge('eq_latlong', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Filters LatLong Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify NO observations exist for latLong/37.77_-122.41
            latlong_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'latLong/37.77_-122.41'"
            ))
            self.assertEqual(len(latlong_obs), 0, "latLong/ locations should be filtered out")

            # Verify geoId/06 observation exists with count 1
            valid_obs = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND date = '2023'"
            ))
            self.assertEqual(len(valid_obs), 1)
            self.assertAlmostEqual(float(valid_obs[0][0]), 1.0)

    def test_aggregate_missing_date_safeguard(self):
        """Tests that entities missing date_prop when date_prop is set are cleanly skipped."""
        import_name = 'EarthquakeUSGS_NoDate'
        output_import = 'EarthquakeUSGS_NoDate_Agg'

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_valid: has occurrenceTime (Included)
        self.add_node('eq_valid', 'Valid Earthquake', types=['EarthquakeEvent'])
        self.add_edge('eq_valid', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_valid', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_valid', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        # eq_nodate: missing occurrenceTime (Should be skipped when date_prop is set)
        self.add_node('eq_nodate', 'No Date Earthquake', types=['EarthquakeEvent'])
        self.add_edge('eq_nodate', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_nodate', 'affectedPlace', 'geoId/06', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Missing Date Safeguard Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify results in Spanner
        with self.database.snapshot(multi_use=True) as snapshot:
            # Verify exactly 1 observation exists for 2023 with count 1
            obs_results = list(snapshot.execute_sql(
                "SELECT date, value FROM Observation WHERE entity1 = 'geoId/06'"
            ))
            self.assertEqual(len(obs_results), 1)
            self.assertEqual(obs_results[0][0], '2023')
            self.assertAlmostEqual(float(obs_results[0][1]), 1.0)

    def test_aggregate_duplicate_edge_resilience(self):
        """Tests that duplicate edges in Spanner do not cause event double counting."""
        import_name = 'EarthquakeUSGS_Dup'
        output_import = 'EarthquakeUSGS_Dup_Agg'

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: single earthquake node
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)
        
        # Add DUPLICATE location edges for eq_1
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', 'EarthquakeUSGS_OtherProvenance')

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Duplicate Edge Resilience Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify count is 1.0 (NOT 2.0)
        with self.database.snapshot(multi_use=True) as snapshot:
            obs_results = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND date = '2023'"
            ))
            self.assertEqual(len(obs_results), 1)
            self.assertAlmostEqual(float(obs_results[0][0]), 1.0, msg="Duplicate edges should not cause double counting")

    def test_aggregate_multiple_location_props(self):
        """Tests aggregation when config specifies multiple location properties (affectedPlace and location)."""
        import_name = 'EarthquakeUSGS_MultiLoc'
        output_import = 'EarthquakeUSGS_MultiLoc_Agg'

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: uses affectedPlace
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        # eq_2: uses location
        self.add_node('eq_2', 'Earthquake 2', types=['EarthquakeEvent'])
        self.add_edge('eq_2', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_2', 'location', 'geoId/06', import_name)
        self.add_edge('eq_2', 'occurrenceTime', '2023-06-20T12:00:00Z', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Multiple Location Props Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent"],
                    "location_props": ["affectedPlace", "location"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify Observation count is 2.0
        with self.database.snapshot(multi_use=True) as snapshot:
            obs_results = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND date = '2023'"
            ))
            self.assertEqual(len(obs_results), 1)
            self.assertAlmostEqual(float(obs_results[0][0]), 2.0, msg="Both affectedPlace and location events should be aggregated")

    def test_aggregate_multiple_entity_types(self):
        """Tests aggregation when config specifies multiple entity_types (EarthquakeEvent and SeismicEvent)."""
        import_name = 'EarthquakeUSGS_MultiType'
        output_import = 'EarthquakeUSGS_MultiType_Agg'

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # eq_1: typeOf = EarthquakeEvent
        self.add_node('eq_1', 'Earthquake 1', types=['EarthquakeEvent'])
        self.add_edge('eq_1', 'typeOf', 'EarthquakeEvent', import_name)
        self.add_edge('eq_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('eq_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        # seismic_1: typeOf = SeismicEvent
        self.add_node('seismic_1', 'Seismic Event 1', types=['SeismicEvent'])
        self.add_edge('seismic_1', 'typeOf', 'SeismicEvent', import_name)
        self.add_edge('seismic_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('seismic_1', 'occurrenceTime', '2023-05-14T12:00:00Z', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Multiple Entity Types Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["EarthquakeEvent", "SeismicEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "occurrenceTime",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify two distinct SVs were created with respective populationTypes
        with self.database.snapshot(multi_use=True) as snapshot:
            sv_pop_types = list(snapshot.execute_sql(
                "SELECT subject_id, object_id FROM Edge WHERE predicate = 'populationType' AND provenance = '" + 
                ("dc/base/" if self.is_base_dc else "") + output_import + "' ORDER BY object_id"
            ))
            self.assertEqual(len(sv_pop_types), 2, "Should generate 2 SVs with distinct populationTypes")
            self.assertEqual(sv_pop_types[0][1], 'EarthquakeEvent')
            self.assertEqual(sv_pop_types[1][1], 'SeismicEvent')

            # Verify Observations exist for both SVs
            for sv_dcid, pop_type in sv_pop_types:
                obs = list(snapshot.execute_sql(
                    f"SELECT value FROM Observation WHERE variable_measured = '{sv_dcid}' AND entity1 = 'geoId/06' AND date = '2023'"
                ))
                self.assertEqual(len(obs), 1, f"Should have observation for SV with populationType {pop_type}")
                self.assertAlmostEqual(float(obs[0][0]), 1.0)

    def test_aggregate_negative_upper_and_unitless_bounds(self):
        """Tests negative bounds, upper bounds (max only), and unitless range constraints."""
        import_name = 'TempDepth_Test'
        output_import = 'TempDepth_Test_Agg'

        # 1. Setup mock data
        self.add_node('geoId/06', 'California', types=['State'])
        self.add_edge('geoId/06', 'typeOf', 'State', import_name)

        # event_1: temp = -5.0, depth = 30.0 (Matches both)
        self.add_node('event_1', 'Event 1', types=['ColdTemperatureEvent'])
        self.add_edge('event_1', 'typeOf', 'ColdTemperatureEvent', import_name)
        self.add_edge('event_1', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('event_1', 'startDate', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('event_1', 'temperature', '-5.0', import_name)
        self.add_edge('event_1', 'depth', '30.0', import_name)

        # event_2: temp = -15.0, depth = 80.0 (Filtered out by both)
        self.add_node('event_2', 'Event 2', types=['ColdTemperatureEvent'])
        self.add_edge('event_2', 'typeOf', 'ColdTemperatureEvent', import_name)
        self.add_edge('event_2', 'affectedPlace', 'geoId/06', import_name)
        self.add_edge('event_2', 'startDate', '2023-05-14T12:00:00Z', import_name)
        self.add_edge('event_2', 'temperature', '-15.0', import_name)
        self.add_edge('event_2', 'depth', '80.0', import_name)

        self.flush_to_spanner()

        calculations = [
            {
                "name": "Negative Upper and Unitless Bounds Aggregation",
                "type": "ENTITY_AGGREGATION",
                "stage": 1,
                "input_imports": [import_name],
                "output_import": output_import,
                "entity_aggregation": {
                    "entity_types": ["ColdTemperatureEvent"],
                    "location_props": ["affectedPlace"],
                    "date_prop": "startDate",
                    "agg_date_formats": ["YYYY"],
                    "constraints": [
                        {"property": "temperature", "min": -10, "max": 40, "unit": "Celsius"},
                        {"property": "depth", "max": 50, "unit": "Kilometer"}
                    ],
                }
            }
        ]
        res = self.run_orchestrator(calculations=calculations, active_imports=[import_name])
        self.assertTrue(res.success)

        # 3. Verify count is 1.0 (event_1 matched, event_2 excluded)
        with self.database.snapshot(multi_use=True) as snapshot:
            obs_results = list(snapshot.execute_sql(
                "SELECT value FROM Observation WHERE entity1 = 'geoId/06' AND date = '2023'"
            ))
            self.assertEqual(len(obs_results), 1)
            self.assertAlmostEqual(float(obs_results[0][0]), 1.0, msg="Only event_1 should satisfy negative and upper bound constraints")


class EntityAggregationGeneratorCustomDcTest(EntityAggregationGeneratorIntegrationTest):
    is_base_dc = False
