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
"""Generates and runs entity aggregations using BigQuery Federation."""

import logging
import json
import re
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
from google.cloud import bigquery
from .bq_executor import BigQueryExecutor
from .common import _escape_sql_literal, get_provenance_name


@dataclass
class EntityAggregationConfig:
    """Configuration for entity aggregation."""
    entity_types: List[str]
    location_props: List[str]
    date_prop: str
    agg_date_formats: List[str]
    constraints: List[Dict[str, Any]]
    output_import: str
    input_imports: List[str]


class EntityAggregationGenerator:
    """Generates and runs entity aggregations using BigQuery Federation."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc

    def _create_job_config(self, config: EntityAggregationConfig) -> bigquery.QueryJobConfig:
        """Creates the QueryJobConfig with query parameters for variable injection."""
        output_provenance = get_provenance_name(config.output_import, self.is_base_dc)
        return bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("output_provenance", "STRING", output_provenance),
                bigquery.ScalarQueryParameter("date_prop", "STRING", config.date_prop or ""),
                bigquery.ScalarQueryParameter("constraints_json", "STRING", json.dumps(config.constraints)),
                bigquery.ArrayQueryParameter("entity_types", "STRING", config.entity_types),
                bigquery.ArrayQueryParameter("location_props", "STRING", config.location_props),
                bigquery.ArrayQueryParameter("agg_date_formats", "STRING", config.agg_date_formats),
            ]
        )

    def aggregate_entities(
            self,
            configs: List[EntityAggregationConfig]
    ) -> List[bigquery.job.QueryJob]:
        """Runs entity aggregations and returns their BigQuery jobs."""
        jobs = []
        for config in configs:
            logging.info(
                f"Generating entity aggregation for types: {config.entity_types}"
            )
            query = self._generate_sql(config)
            job_config = self._create_job_config(config)
            job = self.executor.execute(query, job_config=job_config)
            if job:
                jobs.append(job)
        return jobs

    def _generate_sql(self, config: EntityAggregationConfig) -> str:
        """Generates a cohesive BigQuery procedural SQL script for entity aggregation."""
        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()

        safe_input_imports = [_escape_sql_literal(name) for name in config.input_imports]
        input_provenances_sql = ", ".join([f"'{get_provenance_name(name, self.is_base_dc)}'" for name in safe_input_imports])
        entity_types_sql = ", ".join([f"'{_escape_sql_literal(t)}'" for t in config.entity_types])

        all_props = set(config.location_props)
        if config.date_prop:
            all_props.add(config.date_prop)
        for c in config.constraints:
            if isinstance(c, dict) and 'property' in c:
                all_props.add(c['property'])
        all_props_sql = ", ".join([f"'{_escape_sql_literal(p)}'" for p in sorted(all_props)])

        return rf"""-- ============================================================================
-- Entity Aggregation Script
-- Entity Types: {config.entity_types}
-- Location Properties: {config.location_props}
-- Date Property: {config.date_prop or 'DEFAULT_CURRENT_DATE'}
-- ============================================================================

DECLARE output_provenance STRING DEFAULT @output_provenance;
DECLARE date_prop STRING DEFAULT @date_prop;
DECLARE constraints_json STRING DEFAULT @constraints_json;
DECLARE entity_types ARRAY<STRING> DEFAULT @entity_types;
DECLARE location_props ARRAY<STRING> DEFAULT @location_props;
DECLARE agg_date_formats ARRAY<STRING> DEFAULT @agg_date_formats;

-- Step 1: Extract raw entity IDs of target types from Spanner
CREATE OR REPLACE TEMPORARY TABLE `temp_entities` AS
SELECT DISTINCT subject_id AS entity_id, object_id AS entity_type
FROM EXTERNAL_QUERY("{connection_id}",
  '''SELECT subject_id, object_id FROM Edge 
     WHERE predicate = "typeOf" 
       AND object_id IN ({entity_types_sql}) 
       AND provenance IN ({input_provenances_sql})''');

-- Step 2: Two-Stage Fetch - pull all relevant property triples for target entities
CREATE OR REPLACE TEMPORARY TABLE `temp_entity_edges` AS
SELECT DISTINCT subject_id AS entity_id, predicate, object_id AS val
FROM EXTERNAL_QUERY("{connection_id}",
  '''SELECT subject_id, predicate, object_id FROM Edge 
     WHERE predicate IN ({all_props_sql})''')
WHERE subject_id IN (SELECT entity_id FROM `temp_entities`);

-- Step 3: Filter locations (excluding latLong/ nodes)
CREATE OR REPLACE TEMPORARY TABLE `temp_locations` AS
SELECT DISTINCT entity_id, val AS location_id
FROM `temp_entity_edges`
WHERE predicate IN UNNEST(location_props)
  AND NOT STARTS_WITH(val, 'latLong/');

-- Step 4: Define date formatting reference buckets
CREATE OR REPLACE TEMPORARY TABLE `temp_date_formats` AS
SELECT * FROM (
  SELECT 'YYYY' AS fmt, 4 AS char_len, 'P1Y' AS obs_period
  UNION ALL
  SELECT 'YYYY-MM', 7, 'P1M'
  UNION ALL
  SELECT 'YYYY-MM-DD', 10, 'P1D'
)
WHERE fmt IN UNNEST(agg_date_formats);

-- Step 5: Parse constraint specifications from JSON parameter
CREATE OR REPLACE TEMPORARY TABLE `temp_constraint_specs` AS
SELECT
  OFFSET AS constraint_idx,
  JSON_VALUE(c, '$.property') AS prop,
  SAFE_CAST(JSON_VALUE(c, '$.min') AS FLOAT64) AS min_val,
  SAFE_CAST(JSON_VALUE(c, '$.max') AS FLOAT64) AS max_val,
  JSON_VALUE(c, '$.unit') AS unit,
  JSON_VALUE(c, '$.value') AS exact_val,
  IFNULL(SAFE_CAST(JSON_VALUE(c, '$.wildcard') AS BOOL), FALSE) AS is_wildcard
FROM UNNEST(JSON_QUERY_ARRAY(constraints_json)) AS c WITH OFFSET;

-- Step 6: Assign slice_id and formatted constraint descriptions
CREATE OR REPLACE TEMPORARY TABLE `temp_slices` AS
SELECT
  *,
  CASE 
    WHEN is_wildcard THEN 0
    ELSE ROW_NUMBER() OVER(PARTITION BY prop, CASE WHEN is_wildcard THEN 1 ELSE 0 END ORDER BY constraint_idx) - 1
  END AS slice_id,
  CASE
    WHEN is_wildcard THEN '*'
    WHEN exact_val IS NOT NULL THEN exact_val
    WHEN min_val IS NOT NULL AND max_val IS NOT NULL THEN CONCAT('[', min_val, ' ', max_val, IF(unit IS NOT NULL, CONCAT(' ', unit), ''), ']')
    WHEN min_val IS NOT NULL THEN CONCAT('[', min_val, ' -', IF(unit IS NOT NULL, CONCAT(' ', unit), ''), ']')
    WHEN max_val IS NOT NULL THEN CONCAT('[- ', max_val, IF(unit IS NOT NULL, CONCAT(' ', unit), ''), ']')
    ELSE ''
  END AS val_str
FROM `temp_constraint_specs`;

-- Step 7: Evaluate constraint matches per entity
CREATE OR REPLACE TEMPORARY TABLE `temp_entity_constraint_matches` AS
SELECT
  e.entity_id,
  s.slice_id,
  s.prop,
  s.is_wildcard,
  s.val_str,
  e.val AS matched_val
FROM `temp_entity_edges` e
JOIN `temp_slices` s ON e.predicate = s.prop
WHERE
  (s.is_wildcard AND e.val IS NOT NULL)
  OR (s.exact_val IS NOT NULL AND e.val = s.exact_val)
  OR (s.min_val IS NOT NULL AND s.max_val IS NOT NULL AND SAFE_CAST(e.val AS FLOAT64) >= s.min_val AND SAFE_CAST(e.val AS FLOAT64) <= s.max_val)
  OR (s.min_val IS NOT NULL AND s.max_val IS NULL AND SAFE_CAST(e.val AS FLOAT64) >= s.min_val)
  OR (s.max_val IS NOT NULL AND s.min_val IS NULL AND SAFE_CAST(e.val AS FLOAT64) <= s.max_val);

-- Step 8: Filter entities that satisfy all required constraints of a slice
CREATE OR REPLACE TEMPORARY TABLE `temp_valid_slice_entities` AS
WITH SliceRequirements AS (
  SELECT slice_id, COUNT(DISTINCT prop) AS req_count
  FROM `temp_slices`
  GROUP BY slice_id
),
EntityMatchCounts AS (
  SELECT entity_id, slice_id, COUNT(DISTINCT prop) AS match_count
  FROM `temp_entity_constraint_matches`
  GROUP BY entity_id, slice_id
)
SELECT emc.entity_id, emc.slice_id
FROM EntityMatchCounts emc
JOIN SliceRequirements sr ON emc.slice_id = sr.slice_id
WHERE emc.match_count = sr.req_count
UNION ALL
SELECT entity_id, 0 AS slice_id
FROM `temp_entities`
WHERE (SELECT COUNT(*) FROM `temp_slices`) = 0;

-- Step 9: Define StatisticalVariables generated across all slices and map entities to SVs
CREATE OR REPLACE TEMPORARY TABLE `temp_slice_entity_props` AS
SELECT DISTINCT
  v.slice_id,
  v.entity_id,
  e.entity_type,
  s.prop,
  s.is_wildcard,
  s.val_str,
  m.matched_val,
  IF(s.is_wildcard, CONCAT(s.prop, '=', m.matched_val), '') AS wildcard_key
FROM `temp_valid_slice_entities` v
JOIN `temp_entities` e ON v.entity_id = e.entity_id
LEFT JOIN `temp_slices` s ON v.slice_id = s.slice_id
LEFT JOIN `temp_entity_constraint_matches` m 
  ON v.slice_id = m.slice_id AND s.prop = m.prop AND v.entity_id = m.entity_id;

CREATE OR REPLACE TEMPORARY TABLE `temp_slice_svs` AS
SELECT
  slice_id,
  entity_type,
  wildcard_key,
  CONCAT('dc/sv/gp/', CAST(FARM_FINGERPRINT(ARRAY_TO_STRING(sv_props_array, ',')) AS STRING)) AS sv_dcid,
  CONCAT(
    'Count of ', entity_type,
    IF(ARRAY_LENGTH(static_name_parts) > 0, CONCAT(' with ', ARRAY_TO_STRING(static_name_parts, ' and ')), ''),
    ARRAY_TO_STRING(wildcard_name_parts, '')
  ) AS sv_name,
  sv_props_array
FROM (
  SELECT
    slice_id,
    entity_type,
    wildcard_key,
    ARRAY_CONCAT(
      [
        CONCAT('populationType=', entity_type),
        'measuredProperty=count',
        'statType=measuredValue'
      ],
      IFNULL(
        ARRAY_AGG(
          IF(prop IS NULL, NULL, IF(is_wildcard, CONCAT(prop, '=', matched_val), CONCAT(prop, '=', val_str)))
          IGNORE NULLS ORDER BY prop
        ),
        []
      )
    ) AS sv_props_array,
    IFNULL(
      ARRAY_AGG(
        IF(prop IS NULL OR NOT is_wildcard, NULL, CONCAT(', ', prop, '=', matched_val))
        IGNORE NULLS ORDER BY prop
      ),
      []
    ) AS wildcard_name_parts,
    IFNULL(
      ARRAY_AGG(
        IF(prop IS NULL OR is_wildcard, NULL, CONCAT(prop, ' ', val_str))
        IGNORE NULLS ORDER BY prop
      ),
      []
    ) AS static_name_parts
  FROM `temp_slice_entity_props`
  GROUP BY slice_id, entity_type, wildcard_key
);

-- Step 10: Extract all constraint edges for generated StatisticalVariables
CREATE OR REPLACE TEMPORARY TABLE `temp_sv_edges` AS
SELECT DISTINCT
  sv_dcid,
  SPLIT(prop_kv, '=')[OFFSET(0)] AS predicate,
  SPLIT(prop_kv, '=')[OFFSET(1)] AS object_id
FROM `temp_slice_svs`, UNNEST(sv_props_array) AS prop_kv
WHERE SPLIT(prop_kv, '=')[OFFSET(0)] NOT IN ('populationType', 'measuredProperty', 'statType');

-- Step 11: Map each valid entity to its exact StatisticalVariable
CREATE OR REPLACE TEMPORARY TABLE `temp_entity_sv_map` AS
SELECT DISTINCT
  ep.entity_id,
  sv.sv_dcid,
  sv.sv_name,
  ep.entity_type
FROM `temp_slice_entity_props` ep
JOIN `temp_slice_svs` sv
  ON ep.slice_id = sv.slice_id
 AND ep.entity_type = sv.entity_type
 AND ep.wildcard_key = sv.wildcard_key;

-- Step 12: Aggregate event counts across locations and date buckets
CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_with_sv` AS
WITH EntityDates AS (
  SELECT DISTINCT e.entity_id, d.val AS raw_date
  FROM `temp_entity_sv_map` e
  JOIN `temp_entity_edges` d ON e.entity_id = d.entity_id AND d.predicate = date_prop
  WHERE date_prop != '' AND d.val IS NOT NULL
  UNION ALL
  SELECT DISTINCT entity_id, FORMAT_DATE('%Y-%m-%d', CURRENT_DATE()) AS raw_date
  FROM `temp_entity_sv_map`
  WHERE date_prop = ''
)
SELECT
  l.location_id,
  m.entity_type,
  LEFT(ed.raw_date, df.char_len) AS obs_date,
  df.obs_period,
  COUNT(DISTINCT m.entity_id) AS event_count,
  m.sv_dcid,
  m.sv_name
FROM `temp_entity_sv_map` m
JOIN `temp_locations` l ON m.entity_id = l.entity_id
JOIN EntityDates ed ON m.entity_id = ed.entity_id
CROSS JOIN `temp_date_formats` df
GROUP BY
  l.location_id,
  m.entity_type,
  obs_date,
  df.obs_period,
  m.sv_dcid,
  m.sv_name;

-- ============================================================================
-- Step 13: Export Generated Data to Cloud Spanner
-- ============================================================================

-- Export SV Nodes
EXPORT DATA
  OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Node"}}' ) AS
SELECT DISTINCT
  sv_dcid AS subject_id,
  sv_name AS name,
  CAST(NULL AS STRING) AS value,
  CAST(NULL AS BYTES) AS bytes,
  ['StatisticalVariable'] AS types
FROM `temp_aggregated_with_sv`;

-- Export SV Edges
EXPORT DATA
  OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Edge"}}' ) AS
SELECT DISTINCT sv_dcid AS subject_id, 'typeOf' AS predicate, 'StatisticalVariable' AS object_id, output_provenance AS provenance FROM `temp_aggregated_with_sv`
UNION ALL
SELECT DISTINCT sv_dcid AS subject_id, 'populationType' AS predicate, entity_type AS object_id, output_provenance AS provenance FROM `temp_aggregated_with_sv`
UNION ALL
SELECT DISTINCT sv_dcid AS subject_id, 'measuredProperty' AS predicate, 'count' AS object_id, output_provenance AS provenance FROM `temp_aggregated_with_sv`
UNION ALL
SELECT DISTINCT sv_dcid AS subject_id, 'statType' AS predicate, 'measuredValue' AS object_id, output_provenance AS provenance FROM `temp_aggregated_with_sv`
UNION ALL
SELECT DISTINCT sv_dcid AS subject_id, predicate, object_id, output_provenance AS provenance FROM `temp_sv_edges`;

-- Export TimeSeries
EXPORT DATA
  OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "TimeSeries"}}' ) AS
WITH UniqueTimeSeries AS (
  SELECT DISTINCT sv_dcid, location_id, obs_period FROM `temp_aggregated_with_sv`
),
PreparedTS AS (
  SELECT
    sv_dcid AS variable_measured,
    location_id AS entity1,
    '' AS extra_entities_id,
    JSON_OBJECT('entity1', location_id) AS entities,
    JSON_OBJECT(
      'measurementMethod', 'DataCommonsAggregate',
      'observationPeriod', obs_period,
      'provenance', output_provenance,
      'isDcAggregate', true
    ) AS facet,
    obs_period
  FROM UniqueTimeSeries
)
SELECT
  variable_measured,
  entity1,
  extra_entities_id,
  CAST(FARM_FINGERPRINT(CONCAT(
    output_provenance, '^',
    'DataCommonsAggregate', '^',
    obs_period, '^',
    '', '^',
    '', '^',
    'true'
  )) AS STRING) AS facet_id,
  entities,
  facet
FROM PreparedTS;

-- Export Observations
EXPORT DATA
  OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Observation"}}' ) AS
SELECT
  sv_dcid AS variable_measured,
  location_id AS entity1,
  '' AS extra_entities_id,
  CAST(FARM_FINGERPRINT(CONCAT(
    output_provenance, '^',
    'DataCommonsAggregate', '^',
    obs_period, '^',
    '', '^',
    '', '^',
    'true'
  )) AS STRING) AS facet_id,
  obs_date AS date,
  CAST(event_count AS STRING) AS value
FROM `temp_aggregated_with_sv`;
"""
