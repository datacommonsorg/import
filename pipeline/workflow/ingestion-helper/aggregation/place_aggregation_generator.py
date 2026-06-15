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
"""Place-based aggregation generator using BQ Federation."""

import logging
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class PlaceAggregationGenerator:
    """Generates and runs place-based aggregations using BigQuery Federation.

    Uses a multi-statement SQL script that:
    1. Calculates the new aggregated TimeSeries metadata (with dcAggregate/
       measurementMethod and _Agg<Type> provenance) and exports it to Spanner first.
    2. Aggregates all variables, facets, and dates in parallel, calculates
       the facet_id via Farm Fingerprint, and exports the Observations.
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
        """Generates and runs place-based aggregations using BigQuery Federation.

        This is a multi-statement SQL script that:
        1. Calculates the new aggregated TimeSeries metadata (with dcAggregate/
           measurementMethod and _Agg<Type> provenance) and exports it to Spanner first.
        2. Aggregates all variables, facets, and dates in parallel, calculates
           the facet_id via Farm Fingerprint, and exports the Observations.

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

        # If allow_multiple_to_places is False, use MIN() to select only one parent per child.
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

        # SQL helper fragments for dynamic lineage and hashing
        new_prov_sql = f"CONCAT(ts.source_provenance, '_Agg', '{destination_type}')"
        
        new_method_sql = f"""
            IF(
              ts.source_method IS NULL OR ts.source_method = '' OR ts.source_method = 'DataCommonsAggregate',
              'DataCommonsAggregate',
              CONCAT('dcAggregate/', ts.source_method)
            )
        """
        
        # 3. Deterministic Facet ID: Farm Fingerprint of (variable + new_method + new_provenance)
        # We cast the resulting INT64 to STRING to match Spanner's schema.
        target_facet_id_sql = f"""
            CAST(FARM_FINGERPRINT(CONCAT(
              ts.variable_measured, '_',
              {new_method_sql}, '_',
              {new_prov_sql}
            )) AS STRING)
        """

        # Multi-Statement SQL Script:
        # 1. Calculates and exports the new TimeSeries parent rows.
        # 2. Calculates and exports the aggregated Observation child rows.
        query = f"""
        -- =========================================================================
        -- STEP 1: Export Parent TimeSeries Metadata (The Parent Rows)
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "TimeSeries"}}' ) AS
        SELECT
            variable_measured,
            JSON_OBJECT('entity1', parent_id) AS entities,
            extra_entities_id,
            facet_id,
            JSON_OBJECT(
              'measurementMethod', new_method,
              'provenance', new_prov
            ) AS facet
        FROM (
          -- Inner query performs the DISTINCT on STRING columns only (Allowed in BQ!)
          SELECT DISTINCT
              ts.variable_measured,
              parent.parent_id,
              '' AS extra_entities_id,
              {target_facet_id_sql} AS facet_id,
              {new_method_sql} AS new_method,
              {new_prov_sql} AS new_prov
          FROM (
            -- Source TimeSeries (filtered by provenance)
            -- Extracts JSON fields as flat STRINGS immediately
            SELECT 
                entity1, 
                variable_measured, 
                facet_id, 
                extra_entities_id,
                JSON_VALUE(facet, '$.provenance') AS source_provenance,
                JSON_VALUE(facet, '$.measurementMethod') AS source_method
            FROM EXTERNAL_QUERY(
              "{connection_id}",
              '''
              SELECT entity1, variable_measured, facet_id, extra_entities_id, facet
              FROM TimeSeries
              WHERE provenance IN ({provenance_str})
              '''
            )
          ) ts
          JOIN (
            -- Source Type Check (Edge where predicate = 'typeOf' and object_id = source_type)
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
            -- Containment (child -> parent)
            {containment_sql}
          ) parent ON ts.entity1 = parent.child_id
          JOIN (
            -- Destination Type Check (Edge where predicate = 'typeOf' and object_id = destination_type)
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
        ) ;


        -- =========================================================================
        -- STEP 2: Export Aggregated Observations (The Child Rows)
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        SELECT 
            parent.parent_id AS entity1, 
            ts.variable_measured,
            {target_facet_id_sql} AS facet_id,
            ts.extra_entities_id,
            obs.date,
            CAST(SUM(SAFE_CAST(obs.value AS FLOAT64)) AS STRING) AS value
        FROM (
          -- Source TimeSeries (filtered by provenance)
          -- Extracts JSON fields as flat STRINGS immediately
          SELECT 
              entity1, 
              variable_measured, 
              facet_id, 
              extra_entities_id,
              JSON_VALUE(facet, '$.provenance') AS source_provenance,
              JSON_VALUE(facet, '$.measurementMethod') AS source_method
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT entity1, variable_measured, facet_id, extra_entities_id, facet
            FROM TimeSeries
            WHERE provenance IN ({provenance_str})
            '''
          )
        ) ts
        JOIN (
          -- Source Type Check
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
          -- Source Observations
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
          -- Containment (child -> parent)
          {containment_sql}
        ) parent ON ts.entity1 = parent.child_id
        JOIN (
          -- Destination Type Check
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
            ts.extra_entities_id, 
            obs.date,
            ts.source_provenance,
            ts.source_method ;
        """

        logging.info(f"Running place aggregation query for {import_names} ({source_type} -> {destination_type})...")
        return self.executor.execute(query)
