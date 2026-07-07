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
    temporal, thresholds), and write the results back to Spanner.
    """

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the StatVarSeriesAggregator.

        Args:
            executor: The BigQueryExecutor to use.
            is_base_dc: Whether this is running in the base Data Commons environment.
        """
        self.executor = executor
        self.is_base_dc = is_base_dc

    def aggregate_series(self, calculations: List[Dict[str, Any]]) -> List[Any]:
        """Orchestrates multi-round StatVar Series Aggregations.

        Args:
            calculations: A list of dicts representing the calculations to run.

        Returns:
            A list of completed BigQuery job objects.
        """
        if not calculations:
            logging.info("No calculations specified. Skipping.")
            return []

        # Sort calculations by round to ensure sequential execution
        sorted_calcs = sorted(calculations, key=lambda x: x.get("round", 1))
        jobs = []

        for calc in sorted_calcs:
            round_num = calc.get("round", 1)
            input_imports = calc.get("input_imports", [])
            output_import = calc.get("output_import")
            aggr_funcs = calc.get("aggr_funcs") or calc.get("stat_var_series_aggregation", {}).get("aggr_funcs", [])

            if not input_imports or not output_import or not aggr_funcs:
                logging.warning(f"Invalid calculation config in round {round_num}. Skipping.")
                continue

            logging.info(f"Running Round {round_num}: {input_imports} -> {output_import}")
            job = self._run_round(input_imports, output_import, aggr_funcs)
            if job:
                jobs.append(job)

        return jobs

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
            if not isinstance(func, dict):
                logging.warning(f"Invalid aggregation function config format: {func}")
                continue

            func_type = func.get("type")
            func_config = func

            # Unwrap single-key wrapper dict from YAML/proto (e.g. {'max_diff_across_...': {...}})
            if not func_type and len(func) == 1:
                func_type, inner_config = next(iter(func.items()))
                if isinstance(inner_config, dict):
                    func_config = inner_config
                else:
                    func_config = {}

            if func_type == "max_diff_across_measurement_methods":
                has_max_diff = True
                self._add_max_diff_fragments(ts_ctes, obs_ctes, safe_output_provenance)
            elif func_type == "diff_relative_to_base_date":
                has_diff_rel = True
                self._add_diff_relative_fragments(ts_ctes, obs_ctes, func_config, safe_output_provenance)
            elif func_type == "stats_across_models":
                has_stats_models = True
                self._add_stats_across_models_fragments(ts_ctes, obs_ctes, func_config, safe_output_provenance)
            elif func_type == "aggr_over_time":
                self._add_aggr_over_time_fragments(ts_ctes, obs_ctes, func_config, safe_output_provenance)
            elif func_type == "count_threshold_exception_over_time" or func_type == "count_threshold":
                self._add_count_threshold_fragments(ts_ctes, obs_ctes, func_config, safe_output_provenance)
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
        date_specs = func_config.get("date_specs", [func_config])
        
        for spec_idx, spec in enumerate(date_specs):
            dates = spec.get("dates", [])
            start_date = spec.get("start_date")
            end_date = spec.get("end_date")

            if not dates and (not start_date or not end_date):
                logging.warning("diff_relative_to_base_date missing dates or start_date/end_date. Skipping spec.")
                continue

            # Case 1: Specific base dates
            for base_date in dates:
                cte_idx = len(ts_ctes)
                safe_base_date = _escape_sql_literal(base_date)
                clean_date_suffix = safe_base_date.replace("-", "")
                
                ts_cte = f"""
                DiffRelTS_{cte_idx} AS (
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
                Baseline_{cte_idx} AS (
                  SELECT
                    facet_id,
                    variable_measured,
                    entity1,
                    extra_entities_id,
                    model,
                    val_num AS base_val,
                    date AS base_date,
                    SUBSTR(date, 6, 2) AS base_month
                  FROM RawObs
                  WHERE date = '{safe_base_date}'
                ),
                DiffRelObs_{cte_idx} AS (
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
                  JOIN Baseline_{cte_idx} b ON r.entity1 = b.entity1 
                    AND r.extra_entities_id = b.extra_entities_id 
                    AND r.variable_measured = b.variable_measured 
                    AND r.model = b.model
                    AND r.facet_id = b.facet_id
                    AND SUBSTR(r.date, 6, 2) = b.base_month
                    AND SUBSTR(r.date, 1, 4) > SUBSTR(b.base_date, 1, 4)
                )
                """
                obs_ctes.append(obs_cte)

            # Case 2: Base date range (average over range)
            if start_date and end_date:
                cte_idx = len(ts_ctes)
                safe_start = _escape_sql_literal(start_date)
                safe_end = _escape_sql_literal(end_date)
                range_suffix = f"{safe_start.replace('-', '')}to{safe_end.replace('-', '')}"

                ts_cte = f"""
                DiffRelRangeTS_{cte_idx} AS (
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
                BaselineRange_{cte_idx} AS (
                  SELECT
                    facet_id,
                    variable_measured,
                    entity1,
                    extra_entities_id,
                    model,
                    AVG(val_num) AS base_val,
                    SUBSTR(date, 6, 2) AS base_month
                  FROM RawObs
                  WHERE date BETWEEN '{safe_start}' AND '{safe_end}'
                  GROUP BY facet_id, variable_measured, entity1, extra_entities_id, model, base_month
                ),
                DiffRelRangeObs_{cte_idx} AS (
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
                  JOIN BaselineRange_{cte_idx} b ON r.entity1 = b.entity1 
                    AND r.extra_entities_id = b.extra_entities_id 
                    AND r.variable_measured = b.variable_measured 
                    AND r.model = b.model
                    AND r.facet_id = b.facet_id
                    AND SUBSTR(r.date, 6, 2) = b.base_month
                    AND SUBSTR(r.date, 1, 4) > SUBSTR('{safe_end}', 1, 4)
                )
                """
                obs_ctes.append(obs_cte)

    def _add_stats_across_models_fragments(self, ts_ctes: List[str], obs_ctes: List[str], func_config: Dict[str, Any], output_provenance: str):
        """Adds SQL fragments for stats_across_models (Ensembles)."""
        sv_regex = func_config.get("sv_regex")
        if sv_regex:
            sv_regex = sv_regex.lstrip('^').rstrip('$')
        aggregation_ops = func_config.get("aggregation_ops", ["OPERATOR_MEDIAN", "OPERATOR_PERCENTILE10", "OPERATOR_PERCENTILE90"])

        # Map aggregation ops to stat names and quantile offsets
        stat_specs = []
        for op in aggregation_ops:
            if op in ("OPERATOR_MEDIAN", "MEDIAN", "MedianAcrossModels"):
                stat_specs.append(("MedianAcrossModels", 50))
            elif op in ("OPERATOR_PERCENTILE10", "PERCENTILE10", "Percentile10AcrossModels"):
                stat_specs.append(("Percentile10AcrossModels", 10))
            elif op in ("OPERATOR_PERCENTILE90", "PERCENTILE90", "Percentile90AcrossModels"):
                stat_specs.append(("Percentile90AcrossModels", 90))

        if not stat_specs:
            logging.warning(f"stats_across_models found no valid aggregation_ops in {aggregation_ops}. Defaulting to MedianAcrossModels.")
            stat_specs.append(("MedianAcrossModels", 50))

        stat_names_sql = ", ".join([f"'{s[0]}'" for s in stat_specs])
        regex_filter_ts = f"WHERE REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""
        regex_filter_obs = f"WHERE REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""

        unrolled_union = "\n          UNION ALL\n          ".join([
            f"SELECT entity1, extra_entities_id, variable_measured, date, '{s[0]}' AS stat_type, quantiles[SAFE_OFFSET({s[1]})] AS val, base_facet FROM EnsembleQuantiles"
            for s in stat_specs
        ])

        ts_cte = f"""
        EnsembleTS AS (
          SELECT DISTINCT
            variable_measured,
            entity1,
            extra_entities_id,
            TO_JSON_STRING(facet) AS facet_str,
            stat_type
          FROM SourceTS
          CROSS JOIN UNNEST([{stat_names_sql}]) AS stat_type
          {regex_filter_ts}
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

        obs_cte = f"""
        EnsembleQuantiles AS (
          SELECT 
            entity1,
            extra_entities_id,
            variable_measured,
            date,
            APPROX_QUANTILES(val_num, 100) AS quantiles,
            ANY_VALUE(facet) AS base_facet
          FROM RawObs
          {regex_filter_obs}
          GROUP BY entity1, extra_entities_id, variable_measured, date, COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), COALESCE(JSON_VALUE(facet, '$.unit'), '')
        ),
        UnrolledStats AS (
          {unrolled_union}
        )
        """
        obs_ctes.append(obs_cte)

    def _add_aggr_over_time_fragments(self, ts_ctes: List[str], obs_ctes: List[str], func_config: Dict[str, Any], output_provenance: str):
        """Adds SQL fragments for aggr_over_time (Temporal Aggregation)."""
        time_range = func_config.get("time_range", func_config)
        input_period = time_range.get("input_obs_period", time_range.get("input_period"))
        output_period = time_range.get("output_obs_period", time_range.get("output_period", "P1Y"))
        output_obs_date = time_range.get("output_obs_date", time_range.get("output_date"))

        sv_configs = func_config.get("sv_configs", [func_config])

        for svc in sv_configs:
            operator = svc.get("aggregation_op", svc.get("operator", "MEAN"))
            use_input_sv = svc.get("use_input_sv_for_output", func_config.get("use_input_sv_for_output", True))
            sv_prefix = svc.get("output_sv_prefix", func_config.get("output_sv_prefix", ""))
            sv_regex = svc.get("sv_regex")
            if sv_regex:
                sv_regex = sv_regex.lstrip('^').rstrip('$')

            # Map operator to SQL aggregate function and prefix
            sql_op = "AVG"
            op_name = "AggregateMean"
            if operator in ("OPERATOR_MAX", "MAX"):
                sql_op = "MAX"
                op_name = "HighestValue"
            elif operator in ("OPERATOR_MIN", "MIN"):
                sql_op = "MIN"
                op_name = "LowestValue"
            elif operator in ("OPERATOR_SUM", "SUM"):
                sql_op = "SUM"
                op_name = "AggregateSum"

            # Determine date binning expression based on target period
            if output_obs_date:
                date_bin_expr = f"'{_escape_sql_literal(output_obs_date)}'"
                group_by_date = ""
            else:
                if output_period == "P1Y":
                    date_bin_expr = "SUBSTR(date, 1, 4)"
                elif output_period == "P1M":
                    date_bin_expr = "SUBSTR(date, 1, 7)"
                else:
                    logging.warning(f"Unsupported output period for aggr_over_time: {output_period}. Defaulting to YYYY.")
                    date_bin_expr = "SUBSTR(date, 1, 4)"
                group_by_date = f", {date_bin_expr}"

            # Determine output SV name
            if use_input_sv:
                sv_expr = f"CONCAT('{sv_prefix}', variable_measured)"
            else:
                sv_expr = f"CONCAT('{op_name}_', variable_measured)"

            cte_idx = len(ts_ctes)
            regex_filter_ts = f"AND REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""
            regex_filter_obs = f"AND REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""
            period_filter = f"COALESCE(JSON_VALUE(facet, '$.observationPeriod'), '') = '{input_period}'" if input_period else "1=1"

            # TimeSeries Metadata CTE (sets new period and provenance)
            ts_cte = f"""
            AggrOverTimeTS_{cte_idx} AS (
              SELECT DISTINCT
                {sv_expr} AS variable_measured,
                entity1,
                extra_entities_id,
                TO_JSON_STRING(JSON_SET(
                  JSON_SET(
                    JSON_SET(facet, '$.provenance', '{output_provenance}'),
                    '$.observationPeriod', '{output_period}'
                  ),
                  '$.isDcAggregate', true
                )) AS facet_str
              FROM SourceTS
              WHERE 1=1 {regex_filter_ts}
            )
            """
            ts_ctes.append(ts_cte)

            # Observation Data CTE
            obs_cte = f"""
            AggrOverTimeObs_{cte_idx} AS (
              SELECT
                {sv_expr} AS variable_measured,
                entity1,
                extra_entities_id,
                {date_bin_expr} AS date,
                {sql_op}(val_num) AS val,
                CAST(FARM_FINGERPRINT(CONCAT(
                  '{output_provenance}', '^',
                  COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.measurementMethod'), ''), '^',
                  '{output_period}', '^',
                  COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.scalingFactor'), ''), '^',
                  COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.unit'), ''), '^',
                  'true'
                )) AS STRING) AS facet_id
              FROM RawObs
              WHERE {period_filter} {regex_filter_obs}
              GROUP BY entity1, extra_entities_id, variable_measured, model {group_by_date}
            )
            """
            obs_ctes.append(obs_cte)

    def _add_count_threshold_fragments(self, ts_ctes: List[str], obs_ctes: List[str], func_config: Dict[str, Any], output_provenance: str):
        """Adds SQL fragments for count_threshold (Threshold Exception counting)."""
        time_range = func_config.get("time_range", func_config)
        input_period = time_range.get("input_obs_period", time_range.get("input_period", "P1D"))
        output_period = time_range.get("output_obs_period", time_range.get("output_period", "P1Y"))
        output_obs_date = time_range.get("output_obs_date", time_range.get("output_date"))

        thresholds = func_config.get("thresholds", [func_config])

        for thres in thresholds:
            threshold = thres.get("threshold_value")
            comparison = thres.get("comparison", "GE")
            unit = thres.get("unit", "")
            sv_regex = thres.get("sv_regex")
            if sv_regex:
                sv_regex = sv_regex.lstrip('^').rstrip('$')

            if threshold is None:
                logging.warning("count_threshold missing threshold_value in threshold block. Skipping.")
                continue

            # Determine SV prefix based on input period (replicating C++ logic)
            if input_period == "P1D":
                sv_prefix = "NumberOfDays_"
            elif input_period == "P1M":
                sv_prefix = "NumberOfMonths_"
            else:
                logging.warning(f"Unsupported input period for count_threshold: {input_period}. Defaulting to NumberOfDays_")
                sv_prefix = "NumberOfDays_"

            # Format threshold value (remove trailing .0 if integer to match DCID standard)
            thres_str = str(threshold)
            if thres_str.endswith(".0"):
                thres_str = thres_str[:-2]

            unit_str = unit if unit else "Units"
            is_ge = comparison in ("GE", "OPERATOR_GE")
            suffix = "OrMore_" if is_ge else "OrLess_"
            
            # New SV ID: e.g. NumberOfDays_35KelvinOrMore_Max_Temperature
            new_sv_prefix = f"{sv_prefix}{thres_str}{unit_str}{suffix}"
            sv_expr = f"CONCAT('{new_sv_prefix}', variable_measured)"

            sql_comp = ">=" if is_ge else "<="

            # Determine date binning and output date
            if output_obs_date:
                date_expr = f"'{_escape_sql_literal(output_obs_date)}'"
                group_by_date = ""
            else:
                if output_period == "P1Y":
                    date_expr = "SUBSTR(date, 1, 4)"
                elif output_period == "P1M":
                    date_expr = "SUBSTR(date, 1, 7)"
                else:
                    date_expr = "SUBSTR(date, 1, 4)"
                group_by_date = f", {date_expr}"

            cte_idx = len(ts_ctes)
            regex_filter_ts = f"AND REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""
            regex_filter_obs = f"AND REGEXP_CONTAINS(variable_measured, r'^{sv_regex}$')" if sv_regex else ""

            # TimeSeries Metadata CTE (removes unit, updates period and provenance)
            ts_cte = f"""
            CountThresholdTS_{cte_idx} AS (
              SELECT DISTINCT
                {sv_expr} AS variable_measured,
                entity1,
                extra_entities_id,
                TO_JSON_STRING(JSON_SET(
                  JSON_SET(
                    JSON_REMOVE(facet, '$.unit'),
                    '$.provenance', '{output_provenance}'
                  ),
                  '$.observationPeriod', '{output_period}'
                )) AS facet_str
              FROM SourceTS
              WHERE 1=1 {regex_filter_ts}
            )
            """
            ts_ctes.append(ts_cte)

            # Observation Data CTE
            obs_cte = f"""
            CountThresholdObs_{cte_idx} AS (
              SELECT
                {sv_expr} AS variable_measured,
                entity1,
                extra_entities_id,
                {date_expr} AS date,
                SUM(CASE WHEN val_num {sql_comp} {threshold} THEN 1 ELSE 0 END) AS val,
                CAST(FARM_FINGERPRINT(CONCAT(
                  '{output_provenance}', '^',
                  COALESCE(model, ''), '^',
                  '{output_period}', '^',
                  COALESCE(JSON_VALUE(ANY_VALUE(facet), '$.scalingFactor'), ''), '^',
                  '', '^',
                  'true'
                )) AS STRING) AS facet_id
              FROM RawObs
              WHERE COALESCE(JSON_VALUE(facet, '$.observationPeriod'), '') = '{input_period}'
                {regex_filter_obs}
              GROUP BY entity1, extra_entities_id, variable_measured, model {group_by_date}
              HAVING SUM(CASE WHEN val_num {sql_comp} {threshold} THEN 1 ELSE 0 END) > 0
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
        
        # Dynamically add branches for indexed CTEs by checking exact table definition strings
        for idx in range(len(ts_ctes)):
            if f"DiffRelTS_{idx} AS (" in ts_ctes_combined:
                ts_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_str FROM DiffRelTS_{idx}")
            if f"DiffRelRangeTS_{idx} AS (" in ts_ctes_combined:
                ts_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_str FROM DiffRelRangeTS_{idx}")
            if f"AggrOverTimeTS_{idx} AS (" in ts_ctes_combined:
                ts_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_str FROM AggrOverTimeTS_{idx}")
            if f"CountThresholdTS_{idx} AS (" in ts_ctes_combined:
                ts_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_str FROM CountThresholdTS_{idx}")

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
            if f"DiffRelObs_{idx} AS (" in obs_ctes_combined:
                obs_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM DiffRelObs_{idx}")
            if f"DiffRelRangeObs_{idx} AS (" in obs_ctes_combined:
                obs_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM DiffRelRangeObs_{idx}")
            if f"AggrOverTimeObs_{idx} AS (" in obs_ctes_combined:
                obs_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM AggrOverTimeObs_{idx}")
            if f"CountThresholdObs_{idx} AS (" in obs_ctes_combined:
                obs_union_targets.append(f"SELECT variable_measured, entity1, extra_entities_id, facet_id, date, CAST(val AS STRING) AS value FROM CountThresholdObs_{idx}")

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
            ts.facet_id,
            COALESCE(JSON_VALUE(ts.facet, '$.measurementMethod'), '') AS model
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value 
               FROM Observation {obs_exclude_filter} ''') o
          JOIN EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, facet 
               FROM TimeSeries 
               WHERE provenance IN ({input_provenance_str})''') ts
            ON o.variable_measured = ts.variable_measured AND o.entity1 = ts.entity1 AND o.facet_id = ts.facet_id AND o.extra_entities_id = ts.extra_entities_id
        ),
        {obs_ctes_combined}
        {obs_final_select};
        """

        return f"{ts_query}\n\n{obs_query}"
