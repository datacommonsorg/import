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


def get_is_measured_prop_aggregatable_sql() -> str:
    """Returns the SQL definition for IS_MEASURED_PROP_AGGREGATABLE."""
    return r'''
CREATE TEMP FUNCTION IS_MEASURED_PROP_AGGREGATABLE(prop STRING) AS (
  prop IN ('amount', 'area', 'coverageArea', 'generation', 'reserves', 'retailDrugDistribution')
  OR ENDS_WITH(prop, 'Count')
  OR prop = 'count'
);
'''


def get_aggr_strategy_sql() -> str:
    """Returns the SQL definition for GET_AGGR_STRATEGY."""
    return r'''
CREATE TEMP FUNCTION GET_AGGR_STRATEGY(stat_type STRING, prop STRING) AS (
  IF(IS_MEASURED_PROP_AGGREGATABLE(prop),
    CASE 
      WHEN stat_type = 'measuredValue' THEN 'SUM'
      WHEN stat_type = 'minValue' THEN 'MIN'
      WHEN stat_type = 'maxValue' THEN 'MAX'
      ELSE 'NONE'
    END,
    IF(
      (stat_type = 'meanValue' AND prop = 'concentration') OR
      (stat_type = 'measuredValue' AND prop IN ('lifetimeContractionProbability', 'heavyPrecipitationIndex', 'consecutiveDryDays')),
      'MEAN',
      'NONE'
    )
  )
);
'''


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
            output_import_name: str,
            allow_multiple_to_places: bool = False) -> Optional[bigquery.job.QueryJob]:
        """Generates and runs place-based aggregations using BigQuery Federation.

        This is a multi-statement SQL script that:
        1. Calculates the new aggregated TimeSeries metadata (with dcAggregate/
           measurementMethod and output_import_name provenance) and exports it to Spanner first.
        2. Aggregates all variables, facets, and dates in parallel, calculates
           the facet_id via Farm Fingerprint, and exports the Observations.

        Args:
            import_names: List of import names to filter by.
            source_type: The source place type (e.g., 'County').
            destination_type: The destination place type (e.g., 'State').
            output_import_name: The mandatory output import name from orchestration config.
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

        # Output provenance is derived directly from mandatory output_import_name
        safe_output_prov = _escape_sql_literal(get_provenance_name(output_import_name, self.is_base_dc))
        new_prov_sql = f"'{safe_output_prov}'"

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
        # Target provenance derived directly from mandatory output_import_name
        safe_output_prov = _escape_sql_literal(get_provenance_name(output_import_name, self.is_base_dc))
        new_prov_sql = f"'{safe_output_prov}'"
        
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
        # 1. Calculates the StatVar eligibility and aggregation strategy.
        # 2. Calculates and exports the new TimeSeries parent rows.
        # 3. Calculates and exports the aggregated Observation child rows.
        query = f"""
        -- =========================================================================
        -- DEFINE HELPERS
        -- =========================================================================
        {get_is_measured_prop_aggregatable_sql()}
        {get_aggr_strategy_sql()}

        -- Identify eligible StatVars from Spanner Edge table or fallback to default
        CREATE OR REPLACE TEMP TABLE StatVarMetadata AS
        SELECT 
          subject_id AS stat_var,
          MAX(IF(predicate = 'measuredProperty', object_id, NULL)) AS measured_property,
          COALESCE(MAX(IF(predicate = 'statType', object_id, NULL)), 'measuredValue') AS stat_type,
          MAX(IF(predicate = 'measurementDenominator', object_id, NULL)) AS denominator
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT subject_id, predicate, object_id FROM Edge 
             WHERE predicate IN ("measuredProperty", "statType", "measurementDenominator")''')
        GROUP BY subject_id;

        CREATE OR REPLACE TEMP TABLE EligibleSVs AS
        SELECT 
          ts_vars.variable_measured AS stat_var,
          IF(
            sv.stat_var IS NOT NULL,
            IF(sv.denominator IS NOT NULL, 'NONE', GET_AGGR_STRATEGY(COALESCE(sv.stat_type, 'measuredValue'), sv.measured_property)),
            'SUM'
          ) AS aggr_strategy
        FROM (
          SELECT DISTINCT variable_measured 
          FROM (
            SELECT variable_measured 
            FROM EXTERNAL_QUERY("{connection_id}", 
              '''SELECT variable_measured FROM TimeSeries WHERE provenance IN ({provenance_str})''')
          )
        ) ts_vars
        LEFT JOIN StatVarMetadata sv ON ts_vars.variable_measured = sv.stat_var
        WHERE IF(
            sv.stat_var IS NOT NULL,
            IF(sv.denominator IS NOT NULL, 'NONE', GET_AGGR_STRATEGY(COALESCE(sv.stat_type, 'measuredValue'), sv.measured_property)),
            'SUM'
          ) != 'NONE';

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
          JOIN EligibleSVs sv ON ts.variable_measured = sv.stat_var
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
            obs.entity1,
            obs.variable_measured,
            obs.extra_entities_id,
            obs.new_facet_id AS facet_id,
            obs.date,
            CASE 
              WHEN ANY_VALUE(sv.aggr_strategy) = 'SUM' THEN SUM(obs.val_num)
              WHEN ANY_VALUE(sv.aggr_strategy) = 'MIN' THEN MIN(obs.val_num)
              WHEN ANY_VALUE(sv.aggr_strategy) = 'MAX' THEN MAX(obs.val_num)
              WHEN ANY_VALUE(sv.aggr_strategy) = 'MEAN' THEN AVG(obs.val_num)
            END as total_val
          FROM ObservationsWithFingerprint obs
          JOIN EligibleSVs sv ON obs.variable_measured = sv.stat_var
          GROUP BY obs.entity1, obs.variable_measured, obs.extra_entities_id, obs.new_facet_id, obs.date
          HAVING total_val IS NOT NULL
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
