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
"""Orchestrates StatVar Series Aggregations using BigQuery Federation."""

import logging
from typing import Any, Dict, List

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

logging.getLogger().setLevel(logging.INFO)


class StatVarSeriesAggregator:
    """Orchestrates StatVar Series Aggregations.

    This class contains the SQL logic to read source observations from Spanner
    via BigQuery, perform various series aggregations (anomalies, ensembles,
    temporal), and write the results back to Spanner.
    """

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the StatVarSeriesAggregator.

        Args:
            executor: The BigQueryExecutor to use.
            is_base_dc: Whether this is running in the base Data Commons environment.
        """
        self.executor = executor
        self.is_base_dc = is_base_dc

    def aggregate_series(self, calculations: List[Dict[str, Any]]) -> List[str]:
        """Orchestrates multi-round StatVar Series Aggregations.

        Args:
            calculations: A list of dicts representing the calculations to run.

        Returns:
            A list of completed BigQuery job IDs.
        """
        if not calculations:
            logging.info("No calculations specified. Skipping.")
            return []

        # Sort calculations by round to ensure sequential execution
        sorted_calcs = sorted(calculations, key=lambda x: x.get("round", 1))
        job_ids = []

        for calc in sorted_calcs:
            round_num = calc.get("round", 1)
            input_imports = calc.get("input_imports", [])
            output_import = calc.get("output_import")
            aggr_funcs = calc.get("aggr_funcs", [])

            if not input_imports or not output_import or not aggr_funcs:
                logging.warning(f"Invalid calculation config in round {round_num}. Skipping.")
                continue

            logging.info(f"Running Round {round_num}: {input_imports} -> {output_import}")
            job = self._run_round(input_imports, output_import, aggr_funcs)
            if job:
                job_ids.append(job.job_id)

        return job_ids

    def _run_round(self, input_imports: List[str], output_import: str, aggr_funcs: List[Dict[str, Any]]) -> bigquery.job.QueryJob:
        """Generates and executes the SQL for a single round of aggregations."""
        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()

        prefix = "dc/base/" if self.is_base_dc else ""
        safe_inputs = [_escape_sql_literal(name) for name in input_imports]
        input_provenance_str = ", ".join([f"'{prefix}{name}'" for name in safe_inputs])
        output_provenance = f"{prefix}{output_import}"
        safe_output_provenance = _escape_sql_literal(output_provenance)

        # Build SQL fragments for each active function
        ts_ctes = []
        obs_ctes = []
        
        has_max_diff = False
        has_diff_rel = False
        has_stats_models = False

        for func in aggr_funcs:
            func_type = func.get("type")
            if func_type == "max_diff_across_measurement_methods":
                has_max_diff = True
                self._add_max_diff_fragments(ts_ctes, obs_ctes, safe_output_provenance)
            elif func_type == "diff_relative_to_base_date":
                has_diff_rel = True
                self._add_diff_relative_fragments(ts_ctes, obs_ctes, func, safe_output_provenance)
            elif func_type == "stats_across_models":
                has_stats_models = True
                self._add_stats_across_models_fragments(ts_ctes, obs_ctes, safe_output_provenance)
            else:
                logging.warning(f"Unsupported aggregation function type: {func_type}")

        if not ts_ctes or not obs_ctes:
            logging.warning("No valid aggregation functions to run in this round.")
            return None

        # Construct the final multi-statement query
        query = self._build_combined_query(
            connection_id, dest, input_provenance_str, safe_output_provenance,
            ts_ctes, obs_ctes, has_max_diff, has_diff_rel, has_stats_models
        )

        logging.info(f"Executing round aggregation query:\n{query}")
        return self.executor.execute(query)

    def _add_max_diff_fragments(self, ts_ctes: List[str], obs_ctes: List[str], output_provenance: str):
        """Adds SQL fragments for max_diff_across_measurement_methods."""
        ts_cte = f"""
        DiffAcrossModelsTS AS (
          SELECT DISTINCT
            CONCAT('DifferenceAcrossModels_', variable_measured) AS variable_measured,
            entity1,
            extra_entities_id,
            TO_JSON_STRING(JSON_SET(
              JSON_SET(
                JSON_SET(facet, '$.provenance', '{output_provenance}'),
                '$.measurementMethod', 'dcAggregate/DifferenceAcrossModels'
              ),
              '$.isDcAggregate', true
            )) AS facet_str
          FROM SourceTS
        )
        """
        ts_ctes.append(ts_cte)

        obs_cte = f"""
        MaxDiffObs AS (
          SELECT
            CONCAT('DifferenceAcrossModels_', variable_measured) AS variable_measured,
            entity1,
            extra_entities_id,
            date,
            MAX(val_num) - MIN(val_num) AS val,
            CAST(FARM_FINGERPRINT(CONCAT(
              '{output_provenance}', '^',
              'dcAggregate/DifferenceAcrossModels', '^',
              COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.observationPeriod'), ''), '^',
              COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.scalingFactor'), ''), '^',
              COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.unit'), ''), '^',
              'true'
            )) AS STRING) AS facet_id
          FROM RawObs
          GROUP BY entity1, extra_entities_id, variable_measured, date
          HAVING COUNT(DISTINCT model) >= 2
        )
        """
        obs_ctes.append(obs_cte)

    def _add_diff_relative_fragments(self, ts_ctes: List[str], obs_ctes: List[str], func_config: Dict[str, Any], output_provenance: str):
        """Adds SQL fragments for diff_relative_to_base_date."""
        dates = func_config.get("dates", [])
        start_date = func_config.get("start_date")
        end_date = func_config.get("end_date")

        if not dates and (not start_date or not end_date):
            logging.warning("diff_relative_to_base_date missing dates or start_date/end_date. Skipping.")
            return

        # Case 1: Specific base dates
        for idx, base_date in enumerate(dates):
            safe_base_date = _escape_sql_literal(base_date)
            clean_date_suffix = safe_base_date.replace("-", "")
            
            ts_cte = f"""
            DiffRelTS_{idx} AS (
              SELECT DISTINCT
                CONCAT('DifferenceRelativeToBaseDate', '{clean_date_suffix}', '_', variable_measured) AS variable_measured,
                entity1,
                extra_entities_id,
                TO_JSON_STRING(JSON_SET(
                  JSON_SET(
                    JSON_SET(facet, '$.provenance', '{output_provenance}'),
                    '$.measurementMethod', CONCAT('dcAggregate/', COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''))
                  ),
                  '$.isDcAggregate', true
                )) AS facet_str
              FROM SourceTS
            )
            """
            ts_ctes.append(ts_cte)

            obs_cte = f"""
            Baseline_{idx} AS (
              SELECT
                variable_measured,
                entity1,
                extra_entities_id,
                model,
                val_num AS base_val,
                SUBSTR(date, 6, 2) AS base_month
              FROM RawObs
              WHERE date = '{safe_base_date}'
            ),
            DiffRelObs_{idx} AS (
              SELECT
                CONCAT('DifferenceRelativeToBaseDate', '{clean_date_suffix}', '_', r.variable_measured) AS variable_measured,
                r.entity1,
                r.extra_entities_id,
                r.date,
                r.val_num - b.base_val AS val,
                CAST(FARM_FINGERPRINT(CONCAT(
                  '{output_provenance}', '^',
                  CONCAT('dcAggregate/', r.model), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.observationPeriod'), ''), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.scalingFactor'), ''), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.unit'), ''), '^',
                  'true'
                )) AS STRING) AS facet_id
              FROM RawObs r
              JOIN Baseline_{idx} b ON r.entity1 = b.entity1 
                AND r.extra_entities_id = b.extra_entities_id 
                AND r.variable_measured = b.variable_measured 
                AND r.model = b.model
                AND SUBSTR(r.date, 6, 2) = b.base_month
            )
            """
            obs_ctes.append(obs_cte)

        # Case 2: Base date range (average over range)
        if start_date and end_date:
            safe_start = _escape_sql_literal(start_date)
            safe_end = _escape_sql_literal(end_date)
            range_suffix = f"{safe_start.replace('-', '')}to{safe_end.replace('-', '')}"

            ts_cte = f"""
            DiffRelRangeTS AS (
              SELECT DISTINCT
                CONCAT('DifferenceRelativeToBaseDate', '{range_suffix}', '_', variable_measured) AS variable_measured,
                entity1,
                extra_entities_id,
                TO_JSON_STRING(JSON_SET(
                  JSON_SET(
                    JSON_SET(facet, '$.provenance', '{output_provenance}'),
                    '$.measurementMethod', CONCAT('dcAggregate/', COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''))
                  ),
                  '$.isDcAggregate', true
                )) AS facet_str
              FROM SourceTS
            )
            """
            ts_ctes.append(ts_cte)

            obs_cte = f"""
            BaselineRange AS (
              SELECT
                variable_measured,
                entity1,
                extra_entities_id,
                model,
                AVG(val_num) AS base_val,
                SUBSTR(date, 6, 2) AS base_month
              FROM RawObs
              WHERE date BETWEEN '{safe_start}' AND '{safe_end}'
              GROUP BY variable_measured, entity1, extra_entities_id, model, base_month
            ),
            DiffRelRangeObs AS (
              SELECT
                CONCAT('DifferenceRelativeToBaseDate', '{range_suffix}', '_', r.variable_measured) AS variable_measured,
                r.entity1,
                r.extra_entities_id,
                r.date,
                r.val_num - b.base_val AS val,
                CAST(FARM_FINGERPRINT(CONCAT(
                  '{output_provenance}', '^',
                  CONCAT('dcAggregate/', r.model), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.observationPeriod'), ''), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.scalingFactor'), ''), '^',
                  COALESCE(JSON_VALUE(r.facet, '$.unit'), ''), '^',
                  'true'
                )) AS STRING) AS facet_id
              FROM RawObs r
              JOIN BaselineRange b ON r.entity1 = b.entity1 
                AND r.extra_entities_id = b.extra_entities_id 
                AND r.variable_measured = b.variable_measured 
                AND r.model = b.model
                AND SUBSTR(r.date, 6, 2) = b.base_month
            )
            """
            obs_ctes.append(obs_cte)

    def _add_stats_across_models_fragments(self, ts_ctes: List[str], obs_ctes: List[str], output_provenance: str):
        """Adds SQL fragments for stats_across_models (Ensembles)."""
        ts_cte = f"""
        EnsembleTS AS (
          SELECT DISTINCT
            variable_measured,
            entity1,
            extra_entities_id,
            TO_JSON_STRING(facet) AS facet_str,
            stat_type
          FROM SourceTS
          CROSS JOIN UNNEST(['MedianAcrossModels', 'Percentile10AcrossModels', 'Percentile90AcrossModels']) AS stat_type
        ),
        EnsembleUnrolledTS AS (
          SELECT
            CONCAT(stat_type, '_', variable_measured) AS variable_measured,
            entity1,
            extra_entities_id,
            TO_JSON_STRING(JSON_SET(
              JSON_SET(
                SAFE.PARSE_JSON(facet_str),
                '$.provenance', '{output_provenance}'
              ),
              '$.measurementMethod', CONCAT('dcAggregate/', stat_type)
            )) AS facet_str
          FROM EnsembleTS
        )
        """
        ts_ctes.append(ts_cte)

        obs_cte = """
        EnsembleQuantiles AS (
          SELECT 
            entity1,
            extra_entities_id,
            variable_measured,
            date,
            APPROX_QUANTILES(val_num, 100) AS quantiles,
            ANY_VALUE(facet) AS base_facet
          FROM RawObs
          GROUP BY entity1, extra_entities_id, variable_measured, date
        ),
        UnrolledStats AS (
          SELECT entity1, extra_entities_id, variable_measured, date, 'MedianAcrossModels' AS stat_type, quantiles[OFFSET(50)] AS val, base_facet FROM EnsembleQuantiles
          UNION ALL
          SELECT entity1, extra_entities_id, variable_measured, date, 'Percentile10AcrossModels' AS stat_type, quantiles[OFFSET(10)] AS val, base_facet FROM EnsembleQuantiles
          UNION ALL
          SELECT entity1, extra_entities_id, variable_measured, date, 'Percentile90AcrossModels' AS stat_type, quantiles[OFFSET(90)] AS val, base_facet FROM EnsembleQuantiles
        )
        """
        obs_ctes.append(obs_cte)

    def _build_combined_query(
        self, connection_id: str, dest: str, input_provenance_str: str, output_provenance: str,
        ts_ctes: List[str], obs_ctes: List[str],
        has_max_diff: bool, has_diff_rel: bool, has_stats_models: bool
    ) -> str:
        """Combines all CTEs and builds the final EXPORT DATA statements."""
        
        ts_ctes_combined = ",\n".join(ts_ctes)
        
        # 1. Build Union Branches for TimeSeries
        ts_union_targets = []
        if has_max_diff:
            ts_union_targets.append("SELECT variable_measured, entity1, extra_entities_id, facet_str FROM DiffAcrossModelsTS")
        
        # Add specific date anomaly TS branches
        for idx in range(len(ts_ctes)):
            if f"DiffRelTS_{idx}" in ts_ctes_combined:
                ts_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_str FROM DiffRelTS_{idx}")
        if "DiffRelRangeTS" in ts_ctes_combined:
            ts_union_targets.append("SELECT variable_measured, entity1, extra_entities_id, facet_str FROM DiffRelRangeTS")
            
        if has_stats_models:
            ts_union_targets.append("SELECT variable_measured, entity1, extra_entities_id, facet_str FROM EnsembleUnrolledTS")

        # Combine all branches with UNION ALL
        ts_union_select = "\nUNION ALL\n".join(ts_union_targets)

        # Final TimeSeries SELECT: parses facet_str back to JSON and calculates fingerprint
        ts_final_select = f"""
        SELECT
          variable_measured,
          JSON_OBJECT('entity1', entity1) AS entities,
          extra_entities_id,
          CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(JSON_VALUE(facet, '$.provenance'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
            'true'
          )) AS STRING) AS facet_id,
          facet
        FROM (
          SELECT
            variable_measured,
            entity1,
            extra_entities_id,
            SAFE.PARSE_JSON(facet_str) AS facet
          FROM (
            {ts_union_select}
          )
        )
        """

        # Source filtering for TimeSeries (Step 1)
        # We filter by provenance INSIDE the Spanner query for efficiency
        ts_exclude_filter = ""
        if has_stats_models:
            # Stats across models consumes the output of Round 1, but we must ignore
            # DifferenceAcrossModels which doesn't have multiple models to aggregate
            ts_exclude_filter = "AND NOT variable_measured LIKE 'DifferenceAcrossModels_%'"

        ts_query = f"""
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "TimeSeries"}}' ) AS
        WITH SourceTS AS (
          SELECT
            variable_measured,
            entity1,
            extra_entities_id,
            facet
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet 
               FROM TimeSeries 
               WHERE provenance IN ({input_provenance_str}) {ts_exclude_filter} ''')
        ),
        {ts_ctes_combined}
        {ts_final_select};
        """

        # 2. Build Observation Export Statement
        obs_ctes_combined = ",\n".join(obs_ctes)
        
        obs_union_targets = []
        if has_max_diff:
            obs_union_targets.append("SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM MaxDiffObs")
            
        for idx in range(len(obs_ctes)):
            if f"DiffRelObs_{idx}" in obs_ctes_combined:
                obs_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM DiffRelObs_{idx}")
        if "DiffRelRangeObs" in obs_ctes_combined:
            obs_union_targets.append("SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM DiffRelRangeObs")

        if has_stats_models:
            obs_ensemble_select = f"""
            SELECT
              CONCAT(stat_type, '_', variable_measured) AS variable_measured,
              entity1,
              extra_entities_id,
              CAST(FARM_FINGERPRINT(CONCAT(
                '{output_provenance}', '^',
                CONCAT('dcAggregate/', stat_type), '^',
                COALESCE(JSON_VALUE(base_facet, '$.observationPeriod'), ''), '^',
                COALESCE(JSON_VALUE(base_facet, '$.scalingFactor'), ''), '^',
                COALESCE(JSON_VALUE(base_facet, '$.unit'), ''), '^',
                'true'
              )) AS STRING) AS facet_id,
              date,
              CAST(val AS STRING) AS value
            FROM UnrolledStats
            WHERE val IS NOT NULL
            """
            obs_union_targets.append(obs_ensemble_select)

        obs_final_select = "\nUNION ALL\n".join(obs_union_targets)

        # Source filtering for Observations (Step 2)
        # We filter TimeSeries by provenance inside Spanner, and join on it, which naturally filters Observations.
        obs_exclude_filter = ""
        if has_stats_models:
            obs_exclude_filter = "WHERE NOT variable_measured LIKE 'DifferenceAcrossModels_%'"

        obs_query = f"""
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        WITH RawObs AS (
          SELECT 
            o.variable_measured, 
            o.entity1, 
            o.extra_entities_id, 
            o.date, 
            SAFE_CAST(o.value AS FLOAT64) AS val_num,
            ts.facet,
            COALESCE(JSON_VALUE(ts.facet, '$.measurementMethod'), '') AS model
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value 
               FROM Observation {obs_exclude_filter} ''') o
          JOIN EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, facet 
               FROM TimeSeries 
               WHERE provenance IN ({input_provenance_str})''') ts
            ON o.variable_measured = ts.variable_measured AND o.entity1 = ts.entity1 AND o.facet_id = ts.facet_id
        ),
        {obs_ctes_combined}
        {obs_final_select};
        """

        return f"{ts_query}\n\n{obs_query}"
