-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- BigQuery UI query for previewing provenance summaries for one import.
--
-- Before running, replace both placeholders below throughout this file:
--   datcom-store.us-central1.rk-prod-spanner
--   dc/base/CensusACS5YearSurvey
--
-- The query returns one row per (variable_measured, provenance), not one row
-- for the entire import. It does not write the results to Spanner.

DECLARE place_dcids_str STRING;
DECLARE place_count INT64;
DECLARE sample_dcids_str STRING;

-- Aggregate in Spanner before transferring data to BigQuery. This changes the
-- federated result from one row per observation to one row per time series.
CREATE OR REPLACE TEMPORARY TABLE `temp_series_summary` AS
SELECT *
FROM EXTERNAL_QUERY(
  "datcom-store.us-central1.rk-prod-spanner",
  '''SELECT
       ts.variable_measured,
       ts.entity1 AS observation_about,
       ts.facet_id,
       ts.provenance,
       ANY_VALUE(ts.observation_period) AS observation_period,
       ANY_VALUE(ts.measurement_method) AS measurement_method,
       ANY_VALUE(ts.unit) AS unit,
       ANY_VALUE(JSON_VALUE(ts.facet, '$.scalingFactor')) AS scaling_factor,
       ANY_VALUE(SAFE_CAST(JSON_VALUE(ts.facet, '$.isDcAggregate') AS BOOL)) AS is_dc_aggregate,
       MIN(obs.date) AS min_date,
       MAX(obs.date) AS max_date,
       MIN(SAFE_CAST(obs.value AS FLOAT64)) AS min_value,
       MAX(SAFE_CAST(obs.value AS FLOAT64)) AS max_value,
       COUNT(*) AS observation_count
     FROM TimeSeries AS ts
     JOIN Observation AS obs
       USING (variable_measured, entity1, extra_entities_id, facet_id)
     WHERE ts.provenance = 'dc/base/CensusACS5YearSurvey'
     GROUP BY
       ts.variable_measured,
       ts.entity1,
       ts.extra_entities_id,
       ts.facet_id,
       ts.provenance'''
);

CREATE OR REPLACE TEMPORARY TABLE `temp_dataset_places` AS
SELECT DISTINCT observation_about
FROM `temp_series_summary`;

SET place_count = (SELECT COUNT(*) FROM `temp_dataset_places`);

-- Spanner limits IN to 10,000 values. For larger datasets, retain the existing
-- fallback that reads all typeOf edges and filters them in BigQuery.
IF place_count <= 10000 THEN
  SET place_dcids_str = (
    SELECT IFNULL(
      STRING_AGG(FORMAT("'%s'", REPLACE(observation_about, "'", "\\'")), ','),
      "''"
    )
    FROM `temp_dataset_places`
  );

  EXECUTE IMMEDIATE FORMAT('''
    CREATE OR REPLACE TEMPORARY TABLE `temp_type_edges_filtered` AS
    SELECT subject_id, object_id AS place_type
    FROM EXTERNAL_QUERY(
      "datcom-store.us-central1.rk-prod-spanner",
      "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf' AND subject_id IN (%s)"
    );
  ''', place_dcids_str);
ELSE
  CREATE OR REPLACE TEMPORARY TABLE `temp_type_edges_filtered` AS
  SELECT subject_id, object_id AS place_type
  FROM EXTERNAL_QUERY(
    "datcom-store.us-central1.rk-prod-spanner",
    "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf'"
  );
END IF;

CREATE OR REPLACE TEMPORARY TABLE `temp_prepared` AS
SELECT
  series.variable_measured,
  series.observation_about,
  series.facet_id,
  series.provenance,
  series.min_value,
  series.max_value,
  edges.place_type
FROM `temp_series_summary` AS series
LEFT JOIN `temp_type_edges_filtered` AS edges
  ON series.observation_about = edges.subject_id;

CREATE OR REPLACE TEMPORARY TABLE `temp_top_place_dcids` AS
WITH distinct_places AS (
  SELECT DISTINCT
    variable_measured,
    provenance,
    facet_id,
    place_type,
    observation_about AS dcid
  FROM `temp_prepared`
  WHERE place_type IS NOT NULL
)
SELECT
  variable_measured,
  provenance,
  facet_id,
  place_type,
  ARRAY_AGG(dcid ORDER BY dcid LIMIT 3) AS top_dcids
FROM distinct_places
GROUP BY variable_measured, provenance, facet_id, place_type;

SET sample_dcids_str = (
  SELECT IFNULL(
    STRING_AGG(DISTINCT FORMAT("'%s'", REPLACE(dcid, "'", "\\'")), ','),
    "''"
  )
  FROM `temp_top_place_dcids`
  CROSS JOIN UNNEST(top_dcids) AS dcid
);

EXECUTE IMMEDIATE FORMAT('''
  CREATE OR REPLACE TEMPORARY TABLE `temp_node_names_filtered` AS
  SELECT subject_id, name
  FROM EXTERNAL_QUERY(
    "datcom-store.us-central1.rk-prod-spanner",
    "SELECT subject_id, name FROM Node WHERE subject_id IN (%s)"
  );
''', sample_dcids_str);

CREATE OR REPLACE TEMPORARY TABLE `temp_place_type_summary` AS
WITH place_stats AS (
  SELECT
    variable_measured,
    provenance,
    facet_id,
    place_type,
    MIN(min_value) AS min_val,
    MAX(max_value) AS max_val,
    COUNT(DISTINCT observation_about) AS place_count
  FROM `temp_prepared`
  WHERE place_type IS NOT NULL
  GROUP BY variable_measured, provenance, facet_id, place_type
),
aggregated_places AS (
  SELECT
    samples.variable_measured,
    samples.provenance,
    samples.facet_id,
    samples.place_type,
    ARRAY_AGG(
      STRUCT(dcid, names.name)
      ORDER BY dcid
    ) AS top_places
  FROM `temp_top_place_dcids` AS samples
  CROSS JOIN UNNEST(samples.top_dcids) AS dcid
  LEFT JOIN `temp_node_names_filtered` AS names
    ON dcid = names.subject_id
  GROUP BY
    samples.variable_measured,
    samples.provenance,
    samples.facet_id,
    samples.place_type
)
SELECT
  stats.variable_measured,
  stats.provenance AS provenance_dcid,
  stats.facet_id,
  stats.place_type,
  stats.place_count,
  stats.min_val,
  stats.max_val,
  places.top_places
FROM place_stats AS stats
JOIN aggregated_places AS places
  USING (variable_measured, provenance, facet_id, place_type);

-- Preview the rows that the production generator exports to KeyValueStore.
WITH facet_base AS (
  SELECT
    variable_measured,
    provenance AS provenance_dcid,
    facet_id,
    ANY_VALUE(
      IF(provenance LIKE 'dc/base/%', SUBSTR(provenance, 9), provenance)
    ) AS import_name,
    ANY_VALUE(measurement_method) AS measurement_method,
    ANY_VALUE(observation_period) AS observation_period,
    ANY_VALUE(unit) AS unit,
    ANY_VALUE(scaling_factor) AS scaling_factor,
    ANY_VALUE(is_dc_aggregate) AS is_dc_aggregate,
    MIN(min_date) AS min_date,
    MAX(max_date) AS max_date,
    MIN(min_value) AS facet_min,
    MAX(max_value) AS facet_max,
    SUM(observation_count) AS facet_obs_count,
    COUNT(*) AS facet_ts_count
  FROM `temp_series_summary`
  GROUP BY variable_measured, provenance, facet_id
),
facet_summaries AS (
  SELECT
    facets.variable_measured,
    facets.provenance_dcid,
    facets.facet_id,
    facets.import_name,
    facets.measurement_method,
    facets.observation_period,
    facets.unit,
    facets.scaling_factor,
    facets.is_dc_aggregate,
    facets.min_date,
    facets.max_date,
    facets.facet_min,
    facets.facet_max,
    facets.facet_obs_count,
    facets.facet_ts_count,
    ARRAY_AGG(
      STRUCT(
        places.place_type,
        places.place_count,
        places.min_val,
        places.max_val,
        places.top_places
      )
    ) AS pt_summaries
  FROM facet_base AS facets
  LEFT JOIN `temp_place_type_summary` AS places
    USING (variable_measured, provenance_dcid, facet_id)
  GROUP BY
    facets.variable_measured,
    facets.provenance_dcid,
    facets.facet_id,
    facets.import_name,
    facets.measurement_method,
    facets.observation_period,
    facets.unit,
    facets.scaling_factor,
    facets.is_dc_aggregate,
    facets.min_date,
    facets.max_date,
    facets.facet_min,
    facets.facet_max,
    facets.facet_obs_count,
    facets.facet_ts_count
)
SELECT
  'ProvenanceSummary' AS type,
  variable_measured AS key,
  provenance_dcid AS provenance,
  JSON_OBJECT(
    'import_name', ANY_VALUE(import_name),
    'observation_count', CAST(SUM(facet_obs_count) AS FLOAT64),
    'time_series_count', CAST(SUM(facet_ts_count) AS FLOAT64),
    'series_summary', ARRAY_AGG(
      JSON_OBJECT(
        'series_key', JSON_OBJECT(
          'measurement_method', measurement_method,
          'observation_period', observation_period,
          'unit', unit,
          'scaling_factor', scaling_factor,
          'is_dc_aggregate', COALESCE(is_dc_aggregate, FALSE)
        ),
        'earliest_date', min_date,
        'latest_date', max_date,
        'min_value', facet_min,
        'max_value', facet_max,
        'observation_count', CAST(facet_obs_count AS FLOAT64),
        'time_series_count', CAST(facet_ts_count AS FLOAT64),
        'place_type_summary', (
          SELECT IF(ARRAY_LENGTH(keys) > 0, JSON_OBJECT(keys, vals), NULL)
          FROM (
            SELECT
              ARRAY_AGG(place_type) AS keys,
              ARRAY_AGG(
                JSON_OBJECT(
                  'place_count', place_count,
                  'min_value', min_val,
                  'max_value', max_val,
                  'top_places', (
                    SELECT ARRAY_AGG(
                      JSON_OBJECT('dcid', top_place.dcid, 'name', top_place.name)
                    )
                    FROM UNNEST(top_places) AS top_place
                  )
                )
              ) AS vals
            FROM UNNEST(pt_summaries)
            WHERE place_type IS NOT NULL
          )
        )
      )
    )
  ) AS value
FROM facet_summaries
GROUP BY variable_measured, provenance_dcid
ORDER BY key;
