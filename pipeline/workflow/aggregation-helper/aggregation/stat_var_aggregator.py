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
"""Generates aggregated Observations and TimeSeries directly for Statvars."""

import logging
from dataclasses import dataclass
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .common import _escape_sql_literal, get_provenance_name

logging.getLogger().setLevel(logging.INFO)


@dataclass
class StatVarAggregationConfig:
    """Configuration for statistical variable aggregation."""
    ancestor_sv: str
    source_svs: List[str]
    import_names: List[str]
    output_import_name: Optional[str] = None
    skip_all_sources_present_check: bool = False


class StatVarAggregator:
    """Orchestrates StatVar aggregations.

    This class contains the SQL logic to read source observations from Spanner
    via BigQuery, perform a SUM aggregation, and write the new aggregated
    TimeSeries and Observation rows back to Spanner.
    """

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the StatVarAggregator.

        Args:
            executor: The BigQueryExecutor to use.
            is_base_dc: Whether this is running in the base Data Commons environment,
                which determines if "dc/base/" prefix should be added to provenances.
        """
        self.executor = executor
        self.is_base_dc = is_base_dc

    def aggregate_stat_vars(
        self,
        config: StatVarAggregationConfig
    ) -> List[bigquery.job.QueryJob]:
        """Aggregates multiple source StatVars into an ancestor StatVar.

        This performs a SUM aggregation over the sources, creating new
        TimeSeries and Observation rows in Spanner.

        Args:
            config: Structured StatVarAggregationConfig dataclass instance.

        Returns:
            A list of BigQuery QueryJob objects representing the async execution.
        """
        ancestor_sv = config.ancestor_sv
        source_svs = config.source_svs
        import_names = config.import_names
        output_import_name = config.output_import_name
        skip_all_sources_present_check = config.skip_all_sources_present_check
        if not import_names or not source_svs:
            logging.info("Empty imports or sources. Skipping aggregation.")
            return []

        if not output_import_name:
            output_import_name = f"{import_names[0]}_StatVarAgg"

        logging.info(
            f"Aggregating {source_svs} into {ancestor_sv} for imports {import_names} "
            f"-> output import: {output_import_name} (skip_check={skip_all_sources_present_check})"
        )

        dest_uri = _escape_sql_literal(self.executor.get_spanner_destination_uri())
        conn_id = _escape_sql_literal(self.executor.connection_id)

        safe_sources = [_escape_sql_literal(sv) for sv in source_svs]
        safe_imports = [_escape_sql_literal(name) for name in import_names]

        sources_str = ", ".join([f"'{sv}'" for sv in safe_sources])
        imports_str = ", ".join([f"'{get_provenance_name(name, self.is_base_dc)}'" for name in safe_imports])

        output_provenance = get_provenance_name(output_import_name, self.is_base_dc)

        query = rf"""  # nosec
        DECLARE ancestor_sv STRING DEFAULT @ancestor_sv;
        DECLARE output_provenance STRING DEFAULT @output_provenance;
        DECLARE skip_check BOOL DEFAULT @skip_check;
        DECLARE source_count INT64 DEFAULT @source_count;

        -- ============================================================================
        -- UDFs
        -- ============================================================================
        CREATE TEMP FUNCTION GetNewMeasurementMethod(method STRING) AS (
          IF(
            method IS NULL OR method = '' OR method = 'DataCommonsAggregate',
            'DataCommonsAggregate',
            IF(
              STARTS_WITH(method, 'dcAggregate/'),
              method,
              CONCAT('dcAggregate/', method)
            )
          )
        );

        CREATE TEMP FUNCTION CalculateFacetId(
          provenance STRING,
          method STRING,
          period STRING,
          scaling_factor STRING,
          unit STRING,
          is_dc_aggregate STRING
        ) AS (
          CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(provenance, ''), '^',
            COALESCE(method, ''), '^',
            COALESCE(period, ''), '^',
            COALESCE(scaling_factor, ''), '^',
            COALESCE(unit, ''), '^',
            COALESCE(is_dc_aggregate, 'true')
          )) AS STRING)
        );

        CREATE TEMP FUNCTION UpdateFacet(
          facet JSON,
          new_method STRING,
          new_provenance STRING
        ) AS (
          JSON_SET(
            facet,
            '$.measurementMethod', new_method,
            '$.provenance', new_provenance,
            '$.isDcAggregate', true
          )
        );

        -- ============================================================================
        -- Step 1: Fetch Source Observations and TimeSeries from Spanner
        -- ============================================================================
        CREATE OR REPLACE TEMP TABLE SourceObservations AS (
          SELECT
            o.variable_measured,
            o.entity1,
            o.extra_entities_id,
            o.facet_id,
            o.date,
            SAFE_CAST(o.value AS FLOAT64) AS val_num,
            o.entities,
            o.facet,
            CalculateFacetId(
              output_provenance,
              GetNewMeasurementMethod(JSON_VALUE(o.facet, '$.measurementMethod')),
              JSON_VALUE(o.facet, '$.observationPeriod'),
              JSON_VALUE(o.facet, '$.scalingFactor'),
              JSON_VALUE(o.facet, '$.unit'),
              'true'
            ) AS new_facet_id,
            UpdateFacet(
              o.facet,
              GetNewMeasurementMethod(JSON_VALUE(o.facet, '$.measurementMethod')),
              output_provenance
            ) AS new_facet
          FROM EXTERNAL_QUERY("{conn_id}",
            '''SELECT o.variable_measured, o.entity1, o.extra_entities_id, o.facet_id, o.date, o.value, ts.entities AS entities, ts.facet AS facet
               FROM Observation o
               JOIN TimeSeries ts ON o.variable_measured = ts.variable_measured
                 AND o.entity1 = ts.entity1
                 AND o.extra_entities_id = ts.extra_entities_id
                 AND o.facet_id = ts.facet_id
               WHERE o.variable_measured IN ({sources_str})
                 AND ts.provenance IN ({imports_str})''') AS o
        );

        -- ============================================================================
        -- Step 2: Aggregate Observations per Entity, Date, and Facet
        -- ============================================================================
        CREATE OR REPLACE TEMP TABLE AggregatedObs AS (
          SELECT
            entity1,
            extra_entities_id,
            new_facet_id AS facet_id,
            date,
            SUM(val_num) AS total_val,
            COUNT(DISTINCT variable_measured) AS contribution_count,
            ANY_VALUE(entities) AS entities,
            ANY_VALUE(new_facet) AS facet
          FROM SourceObservations
          GROUP BY entity1, extra_entities_id, new_facet_id, date
        );

        -- ============================================================================
        -- Step 3: Filter Valid Aggregations based on Strict/Lenient Mode
        -- ============================================================================
        CREATE OR REPLACE TEMP TABLE ValidObs AS (
          SELECT
            entity1,
            extra_entities_id,
            facet_id,
            date,
            total_val,
            entities,
            facet
          FROM AggregatedObs
          WHERE (skip_check OR contribution_count = source_count)
            AND total_val IS NOT NULL
        );

        -- ============================================================================
        -- Step 4: Export Aggregated TimeSeries and Observations to Spanner
        -- ============================================================================
        IF (SELECT COUNT(*) FROM ValidObs) > 0 THEN
          -- ============================================================================
          -- Step 4a: Export Aggregated TimeSeries Headers
          -- ============================================================================
          CREATE OR REPLACE TEMP TABLE ValidTimeSeries AS (
            SELECT
              ancestor_sv AS variable_measured,
              extra_entities_id,
              facet_id,
              ANY_VALUE(entities) AS entities,
              ANY_VALUE(facet) AS facet
            FROM ValidObs
            GROUP BY extra_entities_id, facet_id
          );

          EXPORT DATA
            OPTIONS(
              uri="{dest_uri}",
              format='CLOUD_SPANNER',
              spanner_options = '{{"table": "TimeSeries"}}'
            ) AS (SELECT * FROM ValidTimeSeries);

          -- ============================================================================
          -- Step 4b: Export Aggregated Observations
          -- ============================================================================
          CREATE OR REPLACE TEMP TABLE ValidObservations AS (
            SELECT
              ancestor_sv AS variable_measured,
              entity1,
              extra_entities_id,
              facet_id,
              date,
              CAST(total_val AS STRING) AS value
            FROM ValidObs
          );

          EXPORT DATA
            OPTIONS(
              uri="{dest_uri}",
              format='CLOUD_SPANNER',
              spanner_options = '{{"table": "Observation"}}'
            ) AS (SELECT * FROM ValidObservations);

          -- ============================================================================
          -- Step 4c: Emit Success Diagnostic Log Message
          -- ============================================================================
          SELECT CONCAT(
            'Successfully aggregated and exported observations for ancestor StatVar: ',
            ancestor_sv
          ) AS log_message;
        ELSE
          -- ============================================================================
          -- Step 4d: Emit Skipped Diagnostic Log Message
          -- ============================================================================
          SELECT CONCAT(
            'No valid observations found for ancestor StatVar: ',
            ancestor_sv,
            ' (skip_check=',
            CAST(skip_check AS STRING),
            ', required_source_count=',
            CAST(source_count AS STRING),
            '). Skipped Spanner EXPORT DATA.'
          ) AS log_message;
        END IF;
        """
        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("ancestor_sv", "STRING", ancestor_sv),
                bigquery.ScalarQueryParameter("output_provenance", "STRING", output_provenance),
                bigquery.ScalarQueryParameter("skip_check", "BOOL", skip_all_sources_present_check),
                bigquery.ScalarQueryParameter("source_count", "INT64", len(source_svs)),
            ]
        )
        job = self.executor.execute(query, job_config=job_config)

        return [job]
