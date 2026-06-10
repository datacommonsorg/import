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
"""Place-based aggregation generator using BQ Federation (Generic, Dynamic & Feature-rich)."""

import logging
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class PlaceAggregationGenerator:
    """Generates and runs place-based aggregations using BigQuery Federation.

    Resolves place containment and types using the Edge table, and aggregates
    all variables, facets, and dates in the import in a single execution.
    Fully compatible with Spanner Data Boost.
    """

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc

    def aggregate_places(
            self,
            import_names: List[str],
            source_type: str,
            destination_type: str,
            allow_multiple_to_places: bool = False) -> Optional[bigquery.job.QueryJob]:
        """Generic aggregation that rolls up all data from source_type to destination_type.

        Args:
            import_names: List of import names to filter by.
            source_type: The source place type (e.g., 'County').
            destination_type: The destination place type (e.g., 'State').
            allow_multiple_to_places: If False, each child place is aggregated into
              at most one parent place (lexicographically first) to prevent double-counting.
              If True, a child place can roll up to multiple parents (useful for grids/ZIPs).
        """
        if not import_names:
            return None

        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        
        # Filter TimeSeries by provenance (mapping to import names)
        if self.is_base_dc:
            provenance_names = [f"dc/base/{name}" for name in safe_names]
        else:
            provenance_names = safe_names
        provenance_str = ", ".join([f"'{name}'" for name in provenance_names])

        # Escape types just in case
        source_type = _escape_sql_literal(source_type)
        destination_type = _escape_sql_literal(destination_type)

        logging.info(
            f"Running dynamic place aggregation: {source_type} -> {destination_type} (allow_multiple={allow_multiple_to_places}) for imports: {import_names}"
        )

        if allow_multiple_to_places:
            containment_sql = f"""
            SELECT subject_id AS child_id, object_id AS parent_id
            FROM EXTERNAL_QUERY(
              "{connection_id}",
              '''
              SELECT subject_id, object_id
              FROM Edge
              WHERE predicate = "containedInPlace"
              '''
            )
            """
        else:
            containment_sql = f"""
            SELECT child_id, MIN(parent_id) AS parent_id
            FROM (
              SELECT subject_id AS child_id, object_id AS parent_id
              FROM EXTERNAL_QUERY(
                "{connection_id}",
                '''
                SELECT subject_id, object_id
                FROM Edge
                WHERE predicate = "containedInPlace"
                '''
              )
            )
            GROUP BY child_id
            """

        query = f"""
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        SELECT 
            parent.parent_id AS entity1, 
            ts.variable_measured,
            ts.facet_id,
            ts.extra_entities_id,
            obs.date,
            CAST(SUM(SAFE_CAST(obs.value AS FLOAT64)) AS STRING) AS value
        FROM (
          -- 1. Source TimeSeries (filtered by provenance)
          -- Single table, no join (Data Boost compatible)
          SELECT entity1, variable_measured, facet_id, extra_entities_id
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT entity1, variable_measured, facet_id, extra_entities_id
            FROM TimeSeries
            WHERE provenance IN ({provenance_str})
            '''
          )
        ) ts
        JOIN (
          -- 2. Source Type Check (Edge where predicate = 'typeOf' and object_id = source_type)
          -- Single table, no join (Data Boost compatible)
          SELECT subject_id
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT subject_id
            FROM Edge
            WHERE predicate = "typeOf"
              AND object_id = "{source_type}"
            '''
          )
        ) src_type ON ts.entity1 = src_type.subject_id
        JOIN (
          -- 3. Source Observations
          -- Single table, no join (Data Boost compatible)
          SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value
            FROM Observation
            '''
          )
        ) obs ON 
            ts.variable_measured = obs.variable_measured AND 
            ts.entity1 = obs.entity1 AND 
            ts.extra_entities_id = obs.extra_entities_id AND 
            ts.facet_id = obs.facet_id
        JOIN (
          -- 4. Containment (child -> parent)
          -- Single table, no join (Data Boost compatible).
          -- Dynamically handles allow_multiple_to_places.
          {containment_sql}
        ) parent ON ts.entity1 = parent.child_id
        JOIN (
          -- 5. Destination Type Check (Edge where predicate = 'typeOf' and object_id = destination_type)
          -- Single table, no join (Data Boost compatible)
          SELECT subject_id
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT subject_id
            FROM Edge
            WHERE predicate = "typeOf"
              AND object_id = "{destination_type}"
            '''
          )
        ) dest_type ON parent.parent_id = dest_type.subject_id
        GROUP BY 
            parent_id, 
            ts.variable_measured, 
            ts.facet_id, 
            ts.extra_entities_id, 
            obs.date
        """

        logging.info(f"Running dynamic generic place aggregation query ({source_type} -> {destination_type})...")
        return self.executor.execute(query)
