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
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

logging.getLogger().setLevel(logging.INFO)


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
        ancestor_sv: str,
        source_svs: List[str],
        import_names: List[str],
        output_import_name: Optional[str] = None,
        skip_all_sources_present_check: bool = False
    ) -> List[bigquery.job.QueryJob]:
        """Aggregates multiple source StatVars into an ancestor StatVar.

        This performs a SUM aggregation over the sources, creating new
        TimeSeries and Observation rows in Spanner.

        Args:
            ancestor_sv: The target parent StatVar ID (e.g., 'Count_Person').
            source_svs: List of source StatVar IDs to aggregate.
            import_names: List of input import names (provenances) to process.
            output_import_name: Optional name for the output import (provenance).
                If not specified, defaults to '{import_names[0]}_StatVarAgg'.
            skip_all_sources_present_check: If True, aggregates even if some source
                variables are missing. If False, only aggregates if all sources are present.

        Returns:
            A list of BigQuery QueryJob objects representing the async execution.
        """
        if not import_names or not source_svs:
            logging.info("Empty imports or sources. Skipping aggregation.")
            return []

        if not output_import_name:
            output_import_name = f"{import_names[0]}_StatVarAgg"

        logging.info(
            f"Aggregating {source_svs} into {ancestor_sv} for imports {import_names} "
            f"-> output import: {output_import_name} (skip_check={skip_all_sources_present_check})"
        )

        # 1. Generate TimeSeries parent query
        ts_query = self._get_timeseries_query(
            ancestor_sv, source_svs, import_names, output_import_name
        )
        
        # 2. Generate Observation child query
        obs_query = self._get_observations_query(
            ancestor_sv, source_svs, import_names, output_import_name, skip_all_sources_present_check
        )

        # 3. Combine into a single multi-statement SQL script (sequentially executed in a single job)
        combined_query = f"{ts_query}\n\n{obs_query}"
        job = self.executor.execute(combined_query)

        return [job]

    def _get_timeseries_query(
        self,
        ancestor_sv: str,
        source_svs: List[str],
        import_names: List[str],
        output_import_name: str
    ) -> str:
        """Creates TimeSeries entries for the ancestor StatVar."""
        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id

        safe_ancestor_sv = _escape_sql_literal(ancestor_sv)
        safe_sources = [_escape_sql_literal(sv) for sv in source_svs]
        safe_imports = [_escape_sql_literal(name) for name in import_names]

        prefix = "dc/base/" if self.is_base_dc else ""
        sources_str = ", ".join([f"'{sv}'" for sv in safe_sources])
        imports_str = ", ".join([f"'{prefix}{name}'" for name in safe_imports])
        
        output_provenance = f"{prefix}{output_import_name}"
        safe_output_provenance = _escape_sql_literal(output_provenance)

        new_method_sql = """
            IF(
              JSON_VALUE(facet, '$.measurementMethod') IS NULL OR JSON_VALUE(facet, '$.measurementMethod') = '' OR JSON_VALUE(facet, '$.measurementMethod') = 'DataCommonsAggregate',
              'DataCommonsAggregate',
              CONCAT('dcAggregate/', JSON_VALUE(facet, '$.measurementMethod'))
            )
        """
        facet_expr = f"""
          JSON_SET(
            JSON_SET(
              JSON_SET(facet, '$.measurementMethod', {new_method_sql}),
              '$.provenance', '{safe_output_provenance}'
            ),
            '$.isDcAggregate', true
          )
        """

        # SQL to insert new TimeSeries rows.
        query = f"""  # nosec
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "TimeSeries"}}' ) AS
        WITH SourceTS AS (
          SELECT
            extra_entities_id,
            -- Stringify JSON columns because BigQuery does not support SELECT DISTINCT on JSON
            TO_JSON_STRING(entities) as entities_str,
            TO_JSON_STRING({facet_expr}) as facet_str
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT extra_entities_id, entities, facet
               FROM TimeSeries
               WHERE variable_measured IN ({sources_str})
                 AND provenance IN ({imports_str})''')
        ),
        UniqueTS AS (
          SELECT DISTINCT
            extra_entities_id,
            entities_str,
            facet_str
          FROM SourceTS
        ),
        ParsedTS AS (
          SELECT
            extra_entities_id,
            SAFE.PARSE_JSON(entities_str) AS entities,
            SAFE.PARSE_JSON(facet_str) AS facet
          FROM UniqueTS
        )
        SELECT
          '{safe_ancestor_sv}' AS variable_measured,
          extra_entities_id,
          -- Replicate Java TimeSeries.calculateFacetId logic using Farm Fingerprint
          CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(JSON_VALUE(facet, '$.provenance'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.isDcAggregate'), 'true')
          )) AS STRING) AS facet_id,
          entities,
          facet
        FROM ParsedTS;
        """
        return query

    def _get_observations_query(
        self,
        ancestor_sv: str,
        source_svs: List[str],
        import_names: List[str],
        output_import_name: str,
        skip_all_sources_present_check: bool
    ) -> str:
        """Aggregates child Observations and writes them to Spanner."""
        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id

        safe_ancestor_sv = _escape_sql_literal(ancestor_sv)
        safe_sources = [_escape_sql_literal(sv) for sv in source_svs]
        safe_imports = [_escape_sql_literal(name) for name in import_names]

        prefix = "dc/base/" if self.is_base_dc else ""
        sources_str = ", ".join([f"'{sv}'" for sv in safe_sources])
        imports_str = ", ".join([f"'{prefix}{name}'" for name in safe_imports])

        output_provenance = f"{prefix}{output_import_name}"
        safe_output_provenance = _escape_sql_literal(output_provenance)

        # Filter condition for completeness check
        if skip_all_sources_present_check:
            filter_condition = "TRUE"
        else:
            filter_condition = f"contribution_count = {len(source_svs)}"

        query = f"""  # nosec
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        WITH MappedObservations AS (
          SELECT
            o.variable_measured,
            o.entity1,
            o.extra_entities_id,
            o.date,
            SAFE_CAST(o.value AS FLOAT64) as val_num,
            -- Replicate Java TimeSeries.calculateFacetId logic using Farm Fingerprint
            CAST(FARM_FINGERPRINT(CONCAT(
              -- New provenance
              '{safe_output_provenance}', '^',
              -- New measurementMethod
              IF(
                JSON_VALUE(facet, '$.measurementMethod') IS NULL OR JSON_VALUE(facet, '$.measurementMethod') = '' OR JSON_VALUE(facet, '$.measurementMethod') = 'DataCommonsAggregate',
                'DataCommonsAggregate',
                CONCAT('dcAggregate/', JSON_VALUE(facet, '$.measurementMethod'))
              ), '^',
              -- Preserved fields
              COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
              -- It is a DC aggregate
              'true'
            )) AS STRING) AS new_facet_id
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT o.variable_measured, o.entity1, o.extra_entities_id, o.facet_id, o.date, o.value, ts.facet AS facet
               FROM Observation o
               JOIN TimeSeries ts ON o.variable_measured = ts.variable_measured
                 AND o.entity1 = ts.entity1
                 AND o.extra_entities_id = ts.extra_entities_id
                 AND o.facet_id = ts.facet_id
               WHERE o.variable_measured IN ({sources_str})
                 AND ts.provenance IN ({imports_str})''') AS o
        ),
        AggregatedObs AS (
          SELECT
            entity1,
            extra_entities_id,
            new_facet_id AS facet_id,
            date,
            SUM(val_num) as total_val,
            COUNT(DISTINCT variable_measured) as contribution_count
          FROM MappedObservations
          GROUP BY entity1, extra_entities_id, new_facet_id, date
        )
        SELECT
          '{safe_ancestor_sv}' AS variable_measured,
          entity1,
          extra_entities_id,
          facet_id,
          date,
          -- Cast back to string as Spanner Observation.value is STRING(MAX)
          CAST(total_val AS STRING) AS value
        FROM AggregatedObs
        WHERE {filter_condition};
        """
        return query
