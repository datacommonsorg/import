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
"""Statistical Variable Calculation Generator using BQ Federation."""

import logging
from typing import List, Dict, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class StatVarCalculationGenerator:
    """Generates and runs statistical variable calculations using BigQuery Federation.

    This class constructs multi-statement SQL scripts to:
    1. Query input observations from Spanner.
    2. Perform mathematical operations (DIVIDE, MULTIPLY, ADD, SUBTRACT).
    3. Construct output TimeSeries and Observation rows with correct facets and facet_ids.
    4. Write the results back to Spanner.
    """

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the StatVarCalculationGenerator.

        Args:
            executor: The BigQueryExecutor to use.
            is_base_dc: Whether this is running in the base Data Commons environment,
                which determines if "dc/base/" prefix should be added to provenances.
        """
        self.executor = executor
        self.is_base_dc = is_base_dc

    def calculate_stat_vars(
        self,
        calculations: List[Dict],
        import_names: List[str],
        output_import_name: str
    ) -> List[bigquery.job.QueryJob]:
        """Runs the statistical variable calculations.

        Args:
            calculations: List of calculation specs (parsed from manifest).
            import_names: List of input import names (provenances) to filter by.
            output_import_name: Name of the output import (provenance) to write.

        Returns:
            A list containing the BigQuery QueryJob representing the async execution.
        """
        if not calculations or not import_names:
            logging.info("Empty calculations or import names. Skipping.")
            return []

        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()

        # Format input provenances
        prefix = "dc/base/" if self.is_base_dc else ""
        safe_imports = [_escape_sql_literal(name) for name in import_names]
        provenance_names = [f"{prefix}{name}" for name in safe_imports]
        provenance_str = ", ".join([f"'{name}'" for name in provenance_names])

        output_provenance = f"{prefix}{output_import_name}"
        safe_output_provenance = _escape_sql_literal(output_provenance)

        sql_statements = []

        for idx, calc in enumerate(calculations):
            operation = calc.get('operation')
            multiplier = calc.get('multiplier', 1.0)
            input1 = calc.get('input1', {})
            input2 = calc.get('input2', {})
            output = calc.get('output', {})

            # Compile input filter clauses
            filter1 = self._get_input_filter_sql(input1)
            filter2 = self._get_input_filter_sql(input2)

            # Determine mathematical expression
            if operation == 'DIVIDE':
                val_expr = f"SAFE_DIVIDE(I1.val, I2.val) * {multiplier}"
            elif operation == 'MULTIPLY':
                val_expr = f"(I1.val * I2.val) * {multiplier}"
            elif operation == 'ADD':
                val_expr = "I1.val + I2.val"
            elif operation == 'SUBTRACT':
                val_expr = "I1.val - I2.val"
            else:
                raise ValueError(f"Unsupported operation: {operation}")

            # Determine output SV name expression
            if 'sv' in output:
                out_sv_expr = f"'{_escape_sql_literal(output['sv'])}'"
            elif 'sv_prefix' in output:
                prefix_str = _escape_sql_literal(output['sv_prefix'])
                out_sv_expr = f"""
                    CONCAT(
                        '{prefix_str}',
                        IF(STARTS_WITH(I1.sv, 'Temperature'), 'Mean_', ''),
                        I1.sv,
                        '_',
                        IF(
                            STARTS_WITH(I1.mm, 'dcAggregate/NASA_Mean_CMIP6_'),
                            SUBSTR(I1.mm, LENGTH('dcAggregate/NASA_Mean_CMIP6_') + 1),
                            I1.mm
                        )
                    )
                """
            else:
                raise ValueError("Calculation output must specify either 'sv' or 'sv_prefix'.")

            # Determine output Measurement Method expression
            if 'measurement_method' in output:
                out_mm_expr = f"'{_escape_sql_literal(output['measurement_method'])}'"
            elif 'measurement_method_prefix' in output:
                prefix_str = _escape_sql_literal(output['measurement_method_prefix'])
                out_mm_expr = f"""
                    CONCAT(
                        '{prefix_str}',
                        IF(
                            STARTS_WITH(I2.mm, 'dcAggregate/'),
                            SUBSTR(I2.mm, LENGTH('dcAggregate/') + 1),
                            I2.mm
                        )
                    )
                """
            else:
                # If neither is specified, default to NULL
                out_mm_expr = "CAST(NULL AS STRING)"

            # Determine output facet info values
            facet_info = output.get('facet_info', {})
            out_unit_val = f"'{_escape_sql_literal(facet_info['unit'])}'" if 'unit' in facet_info else "CAST(NULL AS STRING)"
            out_period_val = f"'{_escape_sql_literal(facet_info['observation_period'])}'" if 'observation_period' in facet_info else "CAST(NULL AS STRING)"
            out_scaling_val = f"'{_escape_sql_literal(facet_info['scaling_factor'])}'" if 'scaling_factor' in facet_info else "CAST(NULL AS STRING)"

            # Construct the new facet JSON expression
            facet_expr = f"""
                SAFE.PARSE_JSON(TO_JSON_STRING(STRUCT(
                    '{safe_output_provenance}' AS provenance,
                    {out_mm_expr} AS measurementMethod,
                    {out_unit_val} AS unit,
                    {out_period_val} AS observationPeriod,
                    {out_scaling_val} AS scalingFactor,
                    true AS isDcAggregate
                )))
            """

            # Construct the fingerprint (facet_id) expression for Observations
            fingerprint_expr = f"""
                CAST(FARM_FINGERPRINT(CONCAT(
                    '{safe_output_provenance}', '^',
                    COALESCE({out_mm_expr}, ''), '^',
                    COALESCE({out_period_val}, ''), '^',
                    COALESCE({out_scaling_val}, ''), '^',
                    COALESCE({out_unit_val}, ''), '^',
                    'true'
                )) AS STRING)
            """

            # Fingerprint calculation for TimeSeries step (working on the parsed facet JSON)
            fingerprint_ts_expr = """
                CAST(FARM_FINGERPRINT(CONCAT(
                  COALESCE(JSON_VALUE(facet, '$.provenance'), ''), '^',
                  COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''), '^',
                  COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
                  COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
                  COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
                  COALESCE(JSON_VALUE(facet, '$.isDcAggregate'), 'true')
                )) AS STRING)
            """

            # Build the query for this specific calculation
            # Note: We stringify the JSON columns ('entities' and 'facet') in SourceTS before running
            # SELECT DISTINCT in UniqueTS, then parse them back to JSON in ParsedTS. This is a workaround
            # for BigQuery's limitation: "Column of type JSON cannot be used in SELECT DISTINCT".
            query = f"""
            -- =========================================================================
            -- CALCULATION {idx}: Export TimeSeries Metadata
            -- =========================================================================
            EXPORT DATA
              OPTIONS( uri="{dest}",
                format='CLOUD_SPANNER',
                spanner_options = '{{"table": "TimeSeries"}}' ) AS
            WITH I1 AS (
              SELECT 
                o.entity1,
                o.extra_entities_id,
                JSON_VALUE(ts.facet, '$.measurementMethod') AS mm,
                o.variable_measured AS sv,
                ts.entities
              FROM EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value FROM Observation''') o
              JOIN EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, entities, facet 
                   FROM TimeSeries 
                   WHERE provenance IN ({provenance_str})''') ts
              ON o.variable_measured = ts.variable_measured
                AND o.entity1 = ts.entity1
                AND o.extra_entities_id = ts.extra_entities_id
                AND o.facet_id = ts.facet_id
              WHERE {filter1}
            ),
            I2 AS (
              SELECT 
                o.entity1,
                o.extra_entities_id,
                JSON_VALUE(ts.facet, '$.measurementMethod') AS mm,
                o.variable_measured AS sv
              FROM EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value FROM Observation''') o
              JOIN EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, entities, facet 
                   FROM TimeSeries 
                   WHERE provenance IN ({provenance_str})''') ts
              ON o.variable_measured = ts.variable_measured
                AND o.entity1 = ts.entity1
                AND o.extra_entities_id = ts.extra_entities_id
                AND o.facet_id = ts.facet_id
              WHERE {filter2}
            ),
            SourceTS AS (
              SELECT
                {out_sv_expr} AS variable_measured,
                I1.extra_entities_id,
                -- Stringify JSON columns because BigQuery does not support SELECT DISTINCT on JSON
                TO_JSON_STRING(I1.entities) AS entities_str,
                TO_JSON_STRING({facet_expr}) AS facet_str
              FROM I1
              JOIN I2 ON I1.entity1 = I2.entity1
                AND I1.extra_entities_id = I2.extra_entities_id
            ),
            UniqueTS AS (
              SELECT DISTINCT
                variable_measured,
                extra_entities_id,
                entities_str,
                facet_str
              FROM SourceTS
            ),
            ParsedTS AS (
              SELECT
                variable_measured,
                extra_entities_id,
                SAFE.PARSE_JSON(entities_str) AS entities,
                SAFE.PARSE_JSON(facet_str) AS facet
              FROM UniqueTS
            )
            SELECT 
              variable_measured,
              entities,
              extra_entities_id,
              {fingerprint_ts_expr} AS facet_id,
              facet
            FROM ParsedTS;


            -- =========================================================================
            -- CALCULATION {idx}: Export Observations
            -- =========================================================================
            EXPORT DATA
              OPTIONS( uri="{dest}",
                format='CLOUD_SPANNER',
                spanner_options = '{{"table": "Observation"}}' ) AS
            WITH I1 AS (
              SELECT 
                o.entity1,
                o.extra_entities_id,
                o.date,
                SAFE_CAST(o.value AS FLOAT64) AS val,
                o.variable_measured AS sv,
                JSON_VALUE(ts.facet, '$.measurementMethod') AS mm
              FROM EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value FROM Observation''') o
              JOIN EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, entities, facet 
                   FROM TimeSeries 
                   WHERE provenance IN ({provenance_str})''') ts
              ON o.variable_measured = ts.variable_measured
                AND o.entity1 = ts.entity1
                AND o.extra_entities_id = ts.extra_entities_id
                AND o.facet_id = ts.facet_id
              WHERE {filter1}
            ),
            I2 AS (
              SELECT 
                o.entity1,
                o.extra_entities_id,
                o.date,
                SAFE_CAST(o.value AS FLOAT64) AS val,
                o.variable_measured AS sv,
                JSON_VALUE(ts.facet, '$.measurementMethod') AS mm
              FROM EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value FROM Observation''') o
              JOIN EXTERNAL_QUERY("{connection_id}",
                '''SELECT variable_measured, entity1, extra_entities_id, facet_id, entities, facet 
                   FROM TimeSeries 
                   WHERE provenance IN ({provenance_str})''') ts
              ON o.variable_measured = ts.variable_measured
                AND o.entity1 = ts.entity1
                AND o.extra_entities_id = ts.extra_entities_id
                AND o.facet_id = ts.facet_id
              WHERE {filter2}
            ),
            JoinedObs AS (
              SELECT 
                I1.entity1,
                {out_sv_expr} AS variable_measured,
                I1.extra_entities_id,
                {fingerprint_expr} AS calculated_facet_id,
                I1.date,
                {val_expr} AS calculated_val
              FROM I1
              JOIN I2 ON I1.entity1 = I2.entity1
                AND I1.extra_entities_id = I2.extra_entities_id
                AND I1.date = I2.date
            )
            SELECT 
              variable_measured,
              entity1,
              extra_entities_id,
              calculated_facet_id AS facet_id,
              date,
              CAST(calculated_val AS STRING) AS value
            FROM JoinedObs
            WHERE calculated_val IS NOT NULL;
            """
            sql_statements.append(query)

        # Combine all queries into a single multi-statement SQL script
        combined_query = "\n\n".join(sql_statements)
        logging.info(f"Executing combined StatVar Calculation query with {len(calculations)} steps...")
        
        job = self.executor.execute(combined_query)
        return [job]

    def _get_input_filter_sql(self, input_spec: Dict) -> str:
        """Translates input manifest filter rules into SQL WHERE clause constraints."""
        filters = []
        
        # SV name filter (always present, but might be regex)
        sv_regex = input_spec.get('sv_regex', '')
        if sv_regex:
            filters.append(f"REGEXP_CONTAINS(o.variable_measured, r'^{sv_regex}$')")

        # Measurement Method filter (extracted from facet JSON)
        mm_regex = input_spec.get('measurement_method_regex', '')
        if mm_regex:
            filters.append(f"REGEXP_CONTAINS(JSON_VALUE(ts.facet, '$.measurementMethod'), r'^{mm_regex}$')")

        # Facet filters (all extracted from facet JSON to ensure compatibility with older schemas)
        facet_info = input_spec.get('facet_info', {})
        if facet_info:
            if 'unit' in facet_info:
                filters.append(f"JSON_VALUE(ts.facet, '$.unit') = '{_escape_sql_literal(facet_info['unit'])}'")
            if 'observation_period' in facet_info:
                filters.append(f"JSON_VALUE(ts.facet, '$.observationPeriod') = '{_escape_sql_literal(facet_info['observation_period'])}'")
            if 'scaling_factor' in facet_info:
                filters.append(f"JSON_VALUE(ts.facet, '$.scalingFactor') = '{_escape_sql_literal(facet_info['scaling_factor'])}'")

        return " AND ".join(filters) if filters else "TRUE"
