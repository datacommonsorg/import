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
from .common import _escape_sql_literal, get_provenance_name


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
        provenance_names = [get_provenance_name(name, self.is_base_dc) for name in safe_names]
        provenance_str = ", ".join([f"'{name}'" for name in provenance_names])

        # Escape types just in case
        source_type = _escape_sql_literal(source_type)
        destination_type = _escape_sql_literal(destination_type)

        logging.info(
            f"Running dynamic place aggregation: {source_type} -> {destination_type} (allow_multiple={allow_multiple_to_places}) for imports: {import_names}"
        )

        # If allow_multiple_to_places is False, filter candidate parents by destination_type in BigQuery first, then use MIN().
        if allow_multiple_to_places:
            containment_sql = f"""
            SELECT c.child_id, c.parent_id
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
            ) c
            JOIN (
              SELECT subject_id AS parent_id
              FROM EXTERNAL_QUERY(
                "{connection_id}",
                '''
                SELECT subject_id
                FROM Edge
                WHERE predicate = "typeOf" AND object_id = "{destination_type}"
                '''
              )
            ) d ON c.parent_id = d.parent_id
            """
        else:
            containment_sql = f"""
            SELECT child_id, MIN(parent_id) AS parent_id
            FROM (
              SELECT c.child_id, c.parent_id
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
              ) c
              JOIN (
                SELECT subject_id AS parent_id
                FROM EXTERNAL_QUERY(
                  "{connection_id}",
                  '''
                  SELECT subject_id
                  FROM Edge
                  WHERE predicate = "typeOf" AND object_id = "{destination_type}"
                  '''
                )
              ) d ON c.parent_id = d.parent_id
            )
            GROUP BY child_id
            """

        # SQL helper fragments for dynamic lineage and hashing
        # Target provenance and method expressions working directly on the JSON 'facet' column
        new_prov_sql = f"CONCAT(JSON_VALUE(facet, '$.provenance'), '_Agg', '{destination_type}')"
        
        new_method_sql = f"""
            IF(
              JSON_VALUE(facet, '$.measurementMethod') IS NULL OR JSON_VALUE(facet, '$.measurementMethod') = '' OR JSON_VALUE(facet, '$.measurementMethod') = 'DataCommonsAggregate',
              'DataCommonsAggregate',
              IF(
                STARTS_WITH(JSON_VALUE(facet, '$.measurementMethod'), 'dcAggregate/'),
                JSON_VALUE(facet, '$.measurementMethod'),
                CONCAT('dcAggregate/', JSON_VALUE(facet, '$.measurementMethod'))
              )
            )
        """
        
        # Construct the facet expression to update measurementMethod, provenance, and isDcAggregate.
        # This preserves all other fields in the source facet.
        # We align with Java: 'isDcAggregate', true
        facet_expr = f"""
          JSON_SET(
            JSON_SET(
              JSON_SET(facet, '$.measurementMethod', {new_method_sql}),
              '$.provenance', {new_prov_sql}
            ),
            '$.isDcAggregate', true
          )
        """

        # Fingerprint calculation for Step 1 (on 'facet' column)
        fingerprint_step1_sql = """
            CAST(FARM_FINGERPRINT(CONCAT(
              COALESCE(JSON_VALUE(facet, '$.provenance'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
              COALESCE(JSON_VALUE(facet, '$.isDcAggregate'), 'true')
            )) AS STRING)
        """

        # Fingerprint calculation for Step 2 (on 'new_facet' column)
        fingerprint_step2_sql = """
            CAST(FARM_FINGERPRINT(CONCAT(
              COALESCE(JSON_VALUE(new_facet, '$.provenance'), ''), '^',
              COALESCE(JSON_VALUE(new_facet, '$.measurementMethod'), ''), '^',
              COALESCE(JSON_VALUE(new_facet, '$.observationPeriod'), ''), '^',
              COALESCE(JSON_VALUE(new_facet, '$.scalingFactor'), ''), '^',
              COALESCE(JSON_VALUE(new_facet, '$.unit'), ''), '^',
              COALESCE(JSON_VALUE(new_facet, '$.isDcAggregate'), 'true')
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
        WITH SourceTS AS (
          SELECT
            ts.variable_measured,
            parent.parent_id,
            ts.extra_entities_id,
            -- Stringify JSON columns because BigQuery does not support SELECT DISTINCT on JSON
            TO_JSON_STRING(JSON_SET(ts.entities, '$.entity1', parent.parent_id)) as entities_str,
            TO_JSON_STRING({facet_expr}) as facet_str
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT entity1, variable_measured, facet_id, extra_entities_id, entities, facet
               FROM TimeSeries
               WHERE provenance IN ({provenance_str})
                 AND COALESCE(JSON_VALUE(facet, '$.measurementMethod'), '') != 'CensusACS5yrSurveySubjectTable' ''') ts
          JOIN (
            -- Source Type Check (Edge where predicate = 'typeOf' and object_id = source_type)
            SELECT subject_id
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id FROM Edge WHERE predicate = "typeOf" AND object_id = "{source_type}"''')
          ) src_type ON ts.entity1 = src_type.subject_id
          JOIN (
            -- Containment (child -> parent)
            {containment_sql}
          ) parent ON ts.entity1 = parent.child_id
          JOIN (
            -- Destination Type Check (Edge where predicate = 'typeOf' and object_id = destination_type)
            SELECT subject_id
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id FROM Edge WHERE predicate = "typeOf" AND object_id = "{destination_type}"''')
          ) dest_type ON parent.parent_id = dest_type.subject_id
        ),
        UniqueTS AS (
          SELECT DISTINCT
            variable_measured,
            parent_id,
            extra_entities_id,
            entities_str,
            facet_str
          FROM SourceTS
        ),
        ParsedTS AS (
          SELECT
            variable_measured,
            parent_id,
            extra_entities_id,
            SAFE.PARSE_JSON(entities_str) AS entities,
            SAFE.PARSE_JSON(facet_str) AS facet
          FROM UniqueTS
        )
        SELECT
          variable_measured,
          entities,
          extra_entities_id,
          {fingerprint_step1_sql} AS facet_id,
          facet
        FROM ParsedTS;


        -- =========================================================================
        -- STEP 2: Export Aggregated Observations (The Child Rows)
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        WITH MappedObservations AS (
          SELECT
            obs.variable_measured,
            parent.parent_id AS entity1,
            obs.extra_entities_id,
            obs.date,
            SAFE_CAST(obs.value AS FLOAT64) as val_num,
            -- Calculate the new facet JSON first to preserve all fields
            {facet_expr} AS new_facet
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT o.variable_measured, o.entity1, o.extra_entities_id, o.facet_id, o.date, o.value, ts.facet
               FROM Observation o
               JOIN TimeSeries ts ON o.variable_measured = ts.variable_measured
                 AND o.entity1 = ts.entity1
                 AND o.extra_entities_id = ts.extra_entities_id
                 AND o.facet_id = ts.facet_id
               WHERE ts.provenance IN ({provenance_str})
                 AND COALESCE(JSON_VALUE(ts.facet, '$.measurementMethod'), '') != 'CensusACS5yrSurveySubjectTable' ''') AS obs
          JOIN (
            -- Source Type Check
            SELECT subject_id
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id FROM Edge WHERE predicate = "typeOf" AND object_id = "{source_type}"''')
          ) src_type ON obs.entity1 = src_type.subject_id
          JOIN (
            -- Containment (child -> parent)
            {containment_sql}
          ) parent ON obs.entity1 = parent.child_id
          JOIN (
            -- Destination Type Check
            SELECT subject_id
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id FROM Edge WHERE predicate = "typeOf" AND object_id = "{destination_type}"''')
          ) dest_type ON parent.parent_id = dest_type.subject_id
        ),
        ObservationsWithFingerprint AS (
          SELECT
            entity1,
            variable_measured,
            extra_entities_id,
            date,
            val_num,
            -- Calculate new_facet_id using Farm Fingerprint on the new_facet
            {fingerprint_step2_sql} AS new_facet_id
          FROM MappedObservations
        ),
        AggregatedObs AS (
          SELECT
            entity1,
            variable_measured,
            extra_entities_id,
            new_facet_id AS facet_id,
            date,
            SUM(val_num) as total_val
          FROM ObservationsWithFingerprint
          GROUP BY entity1, variable_measured, extra_entities_id, new_facet_id, date
        )
        SELECT 
            entity1, 
            variable_measured,
            facet_id,
            extra_entities_id,
            date,
            CAST(total_val AS STRING) AS value
        FROM AggregatedObs;
        """

        logging.info(f"Running place aggregation query for {import_names} ({source_type} -> {destination_type})...")
        return self.executor.execute(query)
