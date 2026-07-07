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
"""Super Enum Aggregation Generator."""

import logging
from typing import Any, List, Optional
from google.cloud import spanner
from google.cloud import bigquery
from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

def get_dc_base32_encode_sql() -> str:
    """Returns the SQL definition for the DC_BASE32_ENCODE UDF.

    This UDF replicates Data Commons' custom Base32 encoding (from dcid.cc).
    It has the following characteristics:
    1. Uses a custom alphabet: "0123456789bcdfghjklmnpqrstvwxyze" (no vowels).
    2. Variable-length and no padding (unlike RFC 4648).
    3. Encodes from LSB to MSB, stopping when the remaining value is 0.
    4. Handles signed BigQuery INT64 by casting negative numbers to their
       unsigned 64-bit equivalent (adding 2^64) in JavaScript.
    """
    return r'''
CREATE TEMP FUNCTION DC_BASE32_ENCODE(fingerprint INT64)
RETURNS STRING
LANGUAGE js AS r"""
  const alphabet = "0123456789bcdfghjklmnpqrstvwxyze";
  let id = BigInt(fingerprint);
  if (id < 0n) {
    id = id + (1n << 64n);
  }
  if (id === 0n) {
    return alphabet[0];
  }
  let result = "";
  while (id > 0n) {
    const val = Number(id & 0x1fn);
    result += alphabet[val];
    id = id >> 5n;
  }
  return result;
""";
'''


def get_is_measured_prop_aggregatable_sql() -> str:
    """Returns the SQL definition for IS_MEASURED_PROP_AGGREGATABLE.

    This function determines if a measured property is aggregatable.
    Rules:
    1. Returns TRUE if the property name ends with 'count' (case-insensitive).
    2. Returns TRUE if the property is in the whitelisted set:
       'amount', 'area', 'coverageArea', 'generation', 'reserves', 'retailDrugDistribution'.
    3. Returns FALSE otherwise.
    """
    return r'''
CREATE TEMP FUNCTION IS_MEASURED_PROP_AGGREGATABLE(prop STRING) AS (
  ENDS_WITH(LOWER(prop), 'count') OR
  prop IN ('amount', 'area', 'coverageArea', 'generation', 'reserves', 'retailDrugDistribution')
);
'''


def get_aggr_strategy_sql() -> str:
    """Returns the SQL definition for GET_AGGR_STRATEGY.

    This function determines the aggregation strategy (SUM, MIN, MAX, MEAN, NONE)
    for a StatVar based on its statType and measuredProperty.
    It matches the C++ logic exactly (google3/datacommons/prophet/derived_graph/util.cc).

    Rules:
    1. If the property IS aggregatable (e.g., 'count'):
       - 'measuredValue' -> 'SUM'
       - 'minValue' -> 'MIN'
       - 'maxValue' -> 'MAX'
       - Any other statType -> 'NONE' (means are not summed).
    2. If the property is NOT aggregatable:
       - Returns 'MEAN' only for a specific whitelist of (statType, property) pairs:
         * 'meanValue' + 'concentration'
         * 'measuredValue' + 'lifetimeContractionProbability'
         * 'measuredValue' + 'heavyPrecipitationIndex'
         * 'measuredValue' + 'consecutiveDryDays'
         * 'kurtosis' / 'skewness' / 'stdDeviation' + 'precipitation' / 'maxTemperature' / 'minTemperature'
       - Returns 'NONE' otherwise.
    """
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
      (stat_type = 'measuredValue' AND prop = 'lifetimeContractionProbability') OR
      (stat_type = 'measuredValue' AND prop = 'heavyPrecipitationIndex') OR
      (stat_type = 'measuredValue' AND prop = 'consecutiveDryDays') OR
      (stat_type = 'kurtosis' AND prop IN ('precipitation', 'maxTemperature', 'minTemperature')) OR
      (stat_type = 'skewness' AND prop IN ('precipitation', 'maxTemperature', 'minTemperature')) OR
      (stat_type = 'stdDeviation' AND prop IN ('precipitation', 'maxTemperature', 'minTemperature')),
      'MEAN',
      'NONE'
    )
  )
);
'''


class SuperEnumAggregationGenerator:
    """Generates Super Enum aggregations using Hybrid or Pure BQ approaches."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True,
                 spanner_client: Optional[spanner.Client] = None,
                 spanner_database: Optional[Any] = None) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc
        self.spanner_client = spanner_client
        self.spanner_database = spanner_database

    def run(self, import_names: List[str]) -> List[bigquery.job.QueryJob]:
        """Runs the Super Enum aggregation.

        Args:
            import_names: List of import names to process.
        """
        if not import_names:
            logging.info("Empty import names. Skipping Super Enum aggregation.")
            return []

        return self._run_aggregation(import_names)

    def _run_aggregation(self, import_names: List[str]) -> List[bigquery.job.QueryJob]:
        """Runs the BigQuery aggregation job."""
        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()

        safe_names = [_escape_sql_literal(name) for name in import_names]
        if self.is_base_dc:
            provenance_names = [f"dc/base/{name}" for name in safe_names]
            prefix = "dc/base/"
        else:
            provenance_names = safe_names
            prefix = ""
        provenance_str = ", ".join([f"'{name}'" for name in provenance_names])

        logging.info(f"Running Super Enum aggregation for imports: {import_names}")

        # SQL script containing JS UDF and multi-statement execution
        query = f"""
        -- =========================================================================
        -- DEFINE HELPERS
        -- =========================================================================
        {get_dc_base32_encode_sql()}
        {get_is_measured_prop_aggregatable_sql()}
        {get_aggr_strategy_sql()}

        -- =========================================================================
        -- STEP 1: Get SpecializationOf Relations
        -- =========================================================================
        CREATE OR REPLACE TEMP TABLE SpecializationOfRelations AS
        SELECT subject_id AS child, object_id AS parent
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT subject_id, object_id FROM Edge WHERE predicate = "specializationOf"''');

        -- =========================================================================
        -- STEP 2: Get All Edges for StatVars that have whitelisted properties
        -- =========================================================================
        CREATE OR REPLACE TEMP TABLE TargetStatVarEdges AS
        SELECT subject_id, predicate, object_id
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT subject_id, predicate, object_id FROM Edge
             WHERE subject_id IN (
               SELECT DISTINCT subject_id FROM Edge
               WHERE predicate IN ("age", "detailedLevelOfSchool", "schoolGradeLevel", "educationalAttainment")
             )''');

        -- =========================================================================
        -- STEP 3: Identify Eligible StatVars
        -- =========================================================================
        CREATE OR REPLACE TEMP TABLE EligibleSVs AS
        SELECT 
          subject_id,
          MAX(IF(predicate = 'measuredProperty', object_id, NULL)) AS measured_property,
          MAX(IF(predicate = 'statType', object_id, NULL)) AS stat_type,
          MAX(IF(predicate = 'measurementDenominator', object_id, NULL)) AS denominator
        FROM TargetStatVarEdges
        GROUP BY subject_id
        HAVING 
          measured_property IS NOT NULL 
          AND stat_type IS NOT NULL 
          AND denominator IS NULL 
          AND GET_AGGR_STRATEGY(stat_type, measured_property) != 'NONE';

        -- =========================================================================
        -- STEP 4: Map Source SVs to Target SVs and Generate DCIDs
        -- =========================================================================
        CREATE OR REPLACE TEMP TABLE GeneratedTargetSVs AS
        WITH RawSVProps AS (
          SELECT subject_id, predicate, object_id
          FROM TargetStatVarEdges
          WHERE subject_id IN (SELECT subject_id FROM EligibleSVs)
        ),
        SVWhitelistedProp AS (
          SELECT subject_id, predicate AS whitelisted_pred, object_id AS child_enum
          FROM RawSVProps
          WHERE predicate IN ('age', 'detailedLevelOfSchool', 'schoolGradeLevel', 'educationalAttainment')
        ),
        SVToParent AS (
          SELECT w.subject_id, w.whitelisted_pred, w.child_enum, r.parent AS parent_enum
          FROM SVWhitelistedProp w
          JOIN SpecializationOfRelations r ON w.child_enum = r.child
        ),
        TargetSVProperties AS (
          SELECT 
            p.subject_id AS source_sv,
            m.parent_enum,
            p.predicate,
            IF(p.predicate = m.whitelisted_pred, m.parent_enum, p.object_id) AS object_id
          FROM RawSVProps p
          JOIN SVToParent m ON p.subject_id = m.subject_id
        ),
        FilteredTargetProps AS (
          SELECT source_sv, parent_enum, predicate, object_id
          FROM TargetSVProperties
          WHERE predicate NOT IN (
            'name', 'description', 'provenance', 'isPublic', 'url', 'memberOf', 
            'label', 'alternateName', 'utteranceTemplate', 'dcid', 'keyStr',
            'differenceBaselineResolution', 'scalingFactor', 'unit'
          )
        ),
        TargetSVKeyStr AS (
          SELECT 
            source_sv,
            parent_enum,
            STRING_AGG(CONCAT(predicate, '=', object_id), '' ORDER BY predicate) AS key_str
          FROM FilteredTargetProps
          GROUP BY source_sv, parent_enum
        )
        SELECT 
          source_sv,
          parent_enum,
          key_str,
          CONCAT('dc/', DC_BASE32_ENCODE(FARM_FINGERPRINT(key_str))) AS target_sv
        FROM TargetSVKeyStr;

        -- =========================================================================
        -- STEP 5: Export New StatVar Nodes to Spanner
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}' ) AS
        SELECT DISTINCT
          target_sv AS subject_id,
          CAST(NULL AS STRING) AS value,
          CAST(NULL AS BYTES) AS bytes,
          CAST(NULL AS STRING) AS name,
          ['StatisticalVariable'] AS types
        FROM GeneratedTargetSVs;

        -- =========================================================================
        -- STEP 6: Export New StatVar Edges to Spanner
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH ReconstructedEdges AS (
          SELECT 
            g.target_sv AS subject_id,
            p.predicate,
            IF(p.predicate = m.whitelisted_pred, r.parent, p.object_id) AS object_id
          FROM TargetStatVarEdges p
          JOIN (
            SELECT subject_id, predicate AS whitelisted_pred, object_id AS child_enum
            FROM TargetStatVarEdges
            WHERE predicate IN ('age', 'detailedLevelOfSchool', 'schoolGradeLevel', 'educationalAttainment')
          ) m ON p.subject_id = m.subject_id
          JOIN SpecializationOfRelations r ON m.child_enum = r.child
          JOIN GeneratedTargetSVs g ON p.subject_id = g.source_sv AND r.parent = g.parent_enum
        )
        SELECT DISTINCT
          subject_id,
          predicate,
          object_id,
          CONCAT('{prefix}', '{safe_names[0]}', '_SuperEnum') AS provenance
        FROM ReconstructedEdges;

        -- =========================================================================
        -- STEP 7: Export Aggregated TimeSeries to Spanner
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "TimeSeries"}}' ) AS
        WITH SourceTS AS (
          SELECT
            ts.variable_measured,
            ts.entities,
            ts.entity1,
            ts.extra_entities_id,
            ts.facet,
            g.target_sv
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entities, entity1, extra_entities_id, facet
               FROM TimeSeries
               WHERE provenance IN ({provenance_str})''') ts
          JOIN GeneratedTargetSVs g ON ts.variable_measured = g.source_sv
        ),
        NewTS AS (
          SELECT
            target_sv AS variable_measured,
            entities,
            entity1,
            extra_entities_id,
            JSON_SET(
              JSON_SET(
                JSON_SET(facet, '$.measurementMethod', 
                  IF(
                    JSON_VALUE(facet, '$.measurementMethod') IS NULL OR JSON_VALUE(facet, '$.measurementMethod') = '' OR JSON_VALUE(facet, '$.measurementMethod') = 'DataCommonsAggregate',
                    'DataCommonsAggregate',
                    IF(STARTS_WITH(JSON_VALUE(facet, '$.measurementMethod'), 'dcAggregate/'),
                       JSON_VALUE(facet, '$.measurementMethod'),
                       CONCAT('dcAggregate/', JSON_VALUE(facet, '$.measurementMethod')))
                  )
                ),
                '$.provenance', CONCAT(JSON_VALUE(facet, '$.provenance'), '_SuperEnum')
              ),
              '$.isDcAggregate', true
            ) AS facet
          FROM SourceTS
        ),
        UniqueTS AS (
          SELECT DISTINCT
            variable_measured,
            entity1,
            extra_entities_id,
            TO_JSON_STRING(entities) as entities_str,
            TO_JSON_STRING(facet) as facet_str
          FROM NewTS
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
          CAST(FARM_FINGERPRINT(CONCAT(
            COALESCE(JSON_VALUE(facet, '$.provenance'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.measurementMethod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.observationPeriod'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.scalingFactor'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.unit'), ''), '^',
            COALESCE(JSON_VALUE(facet, '$.isDcAggregate'), 'true')
          )) AS STRING) AS facet_id,
          facet
        FROM ParsedTS;

        -- =========================================================================
        -- STEP 8: Export Aggregated Observations to Spanner
        -- =========================================================================
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation"}}' ) AS
        WITH MappedObservations AS (
          SELECT
            g.target_sv AS variable_measured,
            o.entity1,
            o.extra_entities_id,
            o.date,
            SAFE_CAST(o.value AS FLOAT64) as val_num,
            CAST(FARM_FINGERPRINT(CONCAT(
              CONCAT(JSON_VALUE(ts.facet, '$.provenance'), '_SuperEnum'), '^',
              IF(
                JSON_VALUE(ts.facet, '$.measurementMethod') IS NULL OR JSON_VALUE(ts.facet, '$.measurementMethod') = '' OR JSON_VALUE(ts.facet, '$.measurementMethod') = 'DataCommonsAggregate',
                'DataCommonsAggregate',
                IF(STARTS_WITH(JSON_VALUE(ts.facet, '$.measurementMethod'), 'dcAggregate/'),
                   JSON_VALUE(ts.facet, '$.measurementMethod'),
                   CONCAT('dcAggregate/', JSON_VALUE(ts.facet, '$.measurementMethod')))
              ), '^',
              COALESCE(JSON_VALUE(ts.facet, '$.observationPeriod'), ''), '^',
              COALESCE(JSON_VALUE(ts.facet, '$.scalingFactor'), ''), '^',
              COALESCE(JSON_VALUE(ts.facet, '$.unit'), ''), '^',
              'true'
            )) AS STRING) AS new_facet_id,
            e.measured_property,
            e.stat_type
          FROM EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, date, value FROM Observation''') o
          JOIN EXTERNAL_QUERY("{connection_id}",
            '''SELECT variable_measured, entity1, extra_entities_id, facet_id, facet FROM TimeSeries WHERE provenance IN ({provenance_str})''') ts
            ON o.variable_measured = ts.variable_measured
            AND o.entity1 = ts.entity1
            AND o.extra_entities_id = ts.extra_entities_id
            AND o.facet_id = ts.facet_id
          JOIN GeneratedTargetSVs g ON o.variable_measured = g.source_sv
          JOIN EligibleSVs e ON o.variable_measured = e.subject_id
        ),
        AggregatedObs AS (
          SELECT
            variable_measured,
            entity1,
            extra_entities_id,
            new_facet_id AS facet_id,
            date,
            CASE GET_AGGR_STRATEGY(MAX(stat_type), MAX(measured_property))
              WHEN 'SUM' THEN SUM(val_num)
              WHEN 'MIN' THEN MIN(val_num)
              WHEN 'MAX' THEN MAX(val_num)
              WHEN 'MEAN' THEN AVG(val_num)
              ELSE NULL
            END AS total_val
          FROM MappedObservations
          GROUP BY variable_measured, entity1, extra_entities_id, new_facet_id, date
        )
        SELECT 
          variable_measured,
          entity1,
          extra_entities_id,
          facet_id,
          date,
          CAST(total_val AS STRING) AS value
        FROM AggregatedObs
        WHERE total_val IS NOT NULL;
        """

        logging.info("Submitting Super Enum aggregation BQ job...")
        job = self.executor.execute(query)
        return [job]
