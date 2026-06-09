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
"""Generates aggregated Observations and TimeSeries directly in Spanner."""

import logging
from typing import List

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

logging.getLogger().setLevel(logging.INFO)


class StatVarAggregator:
    """Orchestrates direct database-to-database StatVar aggregations.

    This class contains the SQL logic to read source observations from Spanner
    via BigQuery, perform a SUM aggregation, and write the new aggregated
    TimeSeries and Observation rows back to Spanner.
    """

    def __init__(self, executor: BigQueryExecutor) -> None:
        """Initializes the StatVarAggregator with the executor."""
        self.executor = executor

    def aggregate_stat_vars(
        self, ancestor_sv: str, source_svs: List[str], import_names: List[str]
    ) -> List[bigquery.job.QueryJob]:
        """Aggregates multiple source StatVars into an ancestor StatVar.

        This performs a SUM aggregation over the sources, creating new
        TimeSeries and Observation rows in Spanner.

        Args:
            ancestor_sv: The target parent StatVar ID (e.g., 'Count_Person').
            source_svs: List of source StatVar IDs to aggregate.
            import_names: List of import names (provenances) to process.

        Returns:
            A list of BigQuery QueryJob objects representing the async execution.
        """
        if not import_names or not source_svs:
            logging.info("Empty imports or sources. Skipping aggregation.")
            return []

        logging.info(
            f"Aggregating {source_svs} into {ancestor_sv} for imports {import_names}"
        )

        # 1. Populate TimeSeries parent rows first (required due to interleaving)
        ts_job = self._populate_timeseries(ancestor_sv, source_svs, import_names)
        
        # 2. Aggregate and populate Observation child rows
        obs_job = self._populate_observations(ancestor_sv, source_svs, import_names)

        return [ts_job, obs_job]

    def _populate_timeseries(
        self, ancestor_sv: str, source_svs: List[str], import_names: List[str]
    ) -> bigquery.job.QueryJob:
        """Creates parent TimeSeries entries for the ancestor StatVar."""
        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id

        safe_ancestor_sv = _escape_sql_literal(ancestor_sv)
        safe_sources = [_escape_sql_literal(sv) for sv in source_svs]
        safe_imports = [_escape_sql_literal(name) for name in import_names]

        sources_str = ", ".join([f"'{sv}'" for sv in safe_sources])
        imports_str = ", ".join([f"'{name}'" for name in safe_imports])

        # SQL to insert new TimeSeries rows.
        # We read from Spanner, compute new facet_id and facet, and export back to Spanner.
        # Note: We exclude 'entity1' and other stored generated columns from the SELECT list
        # because Spanner will automatically compute them from 'entities' and 'facet'.
        query = f"""  # nosec
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "TimeSeries"}}' ) AS
        SELECT DISTINCT
          '{safe_ancestor_sv}' AS variable_measured,
          extra_entities_id,
          -- Generate new facet_id by hashing the updated facet JSON
          TO_HEX(SHA256(TO_JSON_STRING(
            JSON_SET(facet, '$.measurementMethod', 
                     CONCAT('dcAggregate/', COALESCE(JSON_VALUE(facet, '$.measurementMethod'), 'DataCommons'))
            )
          ))) AS facet_id,
          entities,
          JSON_SET(facet, '$.measurementMethod', 
                   CONCAT('dcAggregate/', COALESCE(JSON_VALUE(facet, '$.measurementMethod'), 'DataCommons'))
          ) AS facet
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT variable_measured, extra_entities_id, entities, facet
             FROM TimeSeries
             WHERE variable_measured IN ({sources_str})
               AND provenance IN ({imports_str})''');
        """
        return self.executor.execute(query)

    def _populate_observations(
        self, ancestor_sv: str, source_svs: List[str], import_names: List[str]
    ) -> bigquery.job.QueryJob:
        """Aggregates child Observations and writes them to Spanner."""
        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id

        safe_ancestor_sv = _escape_sql_literal(ancestor_sv)
        safe_sources = [_escape_sql_literal(sv) for sv in source_svs]
        safe_imports = [_escape_sql_literal(name) for name in import_names]

        sources_str = ", ".join([f"'{sv}'" for sv in safe_sources])
        imports_str = ", ".join([f"'{name}'" for name in safe_imports])

        # SQL to aggregate Observations.
        # We join Observation with TimeSeries in Spanner to get the facet (to compute the new facet_id),
        # perform the SUM aggregation in BigQuery, and export back to Spanner.
        query = f"""  # nosec
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        WITH MappedObservations AS (
          SELECT
            o.entity1,
            o.extra_entities_id,
            o.date,
            SAFE_CAST(o.value AS FLOAT64) as val_num,
            -- Rebuild the same new_facet_id based on the updated facet
            TO_HEX(SHA256(TO_JSON_STRING(
              JSON_SET(ts.facet, '$.measurementMethod', 
                       CONCAT('dcAggregate/', COALESCE(JSON_VALUE(ts.facet, '$.measurementMethod'), 'DataCommons'))
              )
            ))) AS new_facet_id
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT o.variable_measured, o.entity1, o.extra_entities_id, o.facet_id, o.date, o.value, ts.facet
               FROM Observation o
               JOIN TimeSeries ts ON o.variable_measured = ts.variable_measured
                 AND o.entity1 = ts.entity1
                 AND o.extra_entities_id = ts.extra_entities_id
                 AND o.facet_id = ts.facet_id
               WHERE o.variable_measured IN ({sources_str})
                 AND ts.provenance IN ({imports_str})''') AS o
        )
        SELECT
          '{safe_ancestor_sv}' AS variable_measured,
          entity1,
          extra_entities_id,
          new_facet_id AS facet_id,
          date,
          -- Cast back to string as Spanner Observation.value is STRING(MAX)
          CAST(SUM(val_num) AS STRING) AS value
        FROM MappedObservations
        GROUP BY entity1, extra_entities_id, new_facet_id, date;
        """
        return self.executor.execute(query)
