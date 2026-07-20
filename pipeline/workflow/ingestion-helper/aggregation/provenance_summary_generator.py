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

import logging
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class ProvenanceSummaryGenerator:
    """Contains the SQL queries to generate ProvenanceSummary in the Cache table."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the ProvenanceSummaryGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self, import_names: List[str]) -> List[bigquery.job.QueryJob]:
        """Runs all provenance summary generation asynchronously and returns their jobs."""
        if not import_names:
            logging.info("No imports specified. Skipping cache aggregations.")
            return []

        logging.info(
            f"Running provenance summary generation for imports: {import_names}"
        )
        job = self.run_provenance_summary_aggregation(import_names)
        return [job] if job else []

    def run_provenance_summary_aggregation(
            self, import_names: List[str]) -> Optional[bigquery.job.QueryJob]:
        """Calculates ProvenanceSummary for all variables and populates the Cache table."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id

        safe_names = [_escape_sql_literal(name) for name in import_names]
        # Format provenances for the SQL IN clause (matching TimeSeries.provenance)
        prefix = "dc/base/" if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenances_str = ", ".join(provenances)

        query = f"""  # nosec
        DECLARE place_dcids_str STRING;
        DECLARE place_count INT64;
        DECLARE sample_dcids_str STRING;
        DECLARE sample_batch_index INT64 DEFAULT 0;
        DECLARE sample_batch_count INT64;
        DECLARE sample_dcid_batch_size INT64 DEFAULT 1000;

        -- Step 1: Aggregate observations per time series in Spanner so that only
        -- one row per series is transferred to BigQuery.
        CREATE OR REPLACE TEMPORARY TABLE `temp_series_summary` AS
        SELECT *
        FROM EXTERNAL_QUERY("{connection_id}",
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
             WHERE ts.provenance IN ({provenances_str})
             GROUP BY
               ts.variable_measured,
               ts.entity1,
               ts.extra_entities_id,
               ts.facet_id,
               ts.provenance ''');

        -- Step 2: Extract distinct place IDs in this dataset
        CREATE OR REPLACE TEMPORARY TABLE `temp_dataset_places` AS
        SELECT DISTINCT observation_about FROM `temp_series_summary`;

        SET place_count = (SELECT COUNT(*) FROM `temp_dataset_places`);

        -- Step 3: Fetch place types for places in this dataset directly from Spanner
        -- If place_count <= 10000, push down IN filter; otherwise stream all typeOf edges
        IF place_count <= 10000 THEN
          SET place_dcids_str = (
            SELECT IFNULL(STRING_AGG(FORMAT("'%s'", REPLACE(observation_about, "'", "\\'")), ','), "''")
            FROM `temp_dataset_places`
          );

          EXECUTE IMMEDIATE FORMAT('''
            CREATE OR REPLACE TEMPORARY TABLE `temp_type_edges_filtered` AS
            SELECT subject_id, object_id as place_type
            FROM EXTERNAL_QUERY("{connection_id}",
              "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf' AND subject_id IN (%s)"
            );
          ''', place_dcids_str);
        ELSE
          CREATE OR REPLACE TEMPORARY TABLE `temp_type_edges_filtered` AS
          SELECT subject_id, object_id as place_type
          FROM EXTERNAL_QUERY("{connection_id}",
            "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf'"
          );
        END IF;

        -- Step 4: Join observations with filtered place_type only
        CREATE OR REPLACE TEMPORARY TABLE `temp_prepared` AS
        SELECT 
          raw.variable_measured,
          raw.observation_about,
          raw.facet_id,
          raw.provenance,
          raw.min_value,
          raw.max_value,
          edges.place_type
        FROM `temp_series_summary` raw
        LEFT JOIN `temp_type_edges_filtered` edges ON raw.observation_about = edges.subject_id;

        -- Step 5: Extract top 3 sample place DCIDs per summary group
        CREATE OR REPLACE TEMPORARY TABLE `temp_top_place_dcids` AS
        WITH distinct_places AS (
          SELECT DISTINCT
            variable_measured,
            provenance,
            facet_id,
            place_type,
            observation_about as dcid
          FROM `temp_prepared`
          WHERE place_type IS NOT NULL
        )
        SELECT
          variable_measured,
          provenance,
          facet_id,
          place_type,
          ARRAY_AGG(dcid ORDER BY dcid LIMIT 3) as top_dcids
        FROM distinct_places
        GROUP BY variable_measured, provenance, facet_id, place_type;

        -- Step 6: Fetch place names in bounded batches. A single large result
        -- can exceed the Spanner federation message limit.
        CREATE OR REPLACE TEMPORARY TABLE `temp_sample_dcids` AS
        SELECT
          dcid,
          DIV(ROW_NUMBER() OVER (ORDER BY dcid) - 1, sample_dcid_batch_size) AS batch_index
        FROM (
          SELECT DISTINCT dcid
          FROM `temp_top_place_dcids`
          CROSS JOIN UNNEST(top_dcids) as dcid
        );

        SET sample_batch_count = (
          SELECT IFNULL(MAX(batch_index) + 1, 0)
          FROM `temp_sample_dcids`
        );

        CREATE OR REPLACE TEMPORARY TABLE `temp_node_names_filtered` (
          subject_id STRING,
          name STRING
        );

        WHILE sample_batch_index < sample_batch_count DO
          SET sample_dcids_str = (
            SELECT STRING_AGG(FORMAT("'%s'", REPLACE(dcid, "'", "\\'")), ',')
            FROM `temp_sample_dcids`
            WHERE batch_index = sample_batch_index
          );

          EXECUTE IMMEDIATE FORMAT('''
            INSERT INTO `temp_node_names_filtered` (subject_id, name)
            SELECT subject_id, name
            FROM EXTERNAL_QUERY("{connection_id}",
              "SELECT subject_id, SUBSTR(name, 1, 1024) AS name FROM Node WHERE subject_id IN (%s)"
            );
          ''', sample_dcids_str);

          SET sample_batch_index = sample_batch_index + 1;
        END WHILE;

        -- Step 7: Aggregate Place Type Summaries and attach names to top 3 sample places
        CREATE OR REPLACE TEMPORARY TABLE `temp_place_type_summary` AS
        WITH place_stats AS (
          SELECT
            variable_measured,
            provenance,
            facet_id,
            place_type,
            MIN(min_value) as min_val,
            MAX(max_value) as max_val,
            COUNT(DISTINCT observation_about) as place_count
          FROM `temp_prepared`
          WHERE place_type IS NOT NULL
          GROUP BY variable_measured, provenance, facet_id, place_type
        ),
        aggregated_places AS (
          SELECT
            tp.variable_measured,
            tp.provenance,
            tp.facet_id,
            tp.place_type,
            ARRAY_AGG(
              STRUCT(dcid, names.name)
              ORDER BY dcid
            ) as top_places
          FROM `temp_top_place_dcids` tp
          CROSS JOIN UNNEST(tp.top_dcids) as dcid
          LEFT JOIN `temp_node_names_filtered` names ON dcid = names.subject_id
          GROUP BY tp.variable_measured, tp.provenance, tp.facet_id, tp.place_type
        )
        SELECT
          ps.variable_measured,
          ps.provenance as provenance_dcid,
          ps.facet_id,
          ps.place_type,
          ps.place_count,
          ps.min_val,
          ps.max_val,
          ap.top_places
        FROM place_stats ps
        JOIN aggregated_places ap USING (variable_measured, provenance, facet_id, place_type);

        -- Step 8: Final aggregation and export to Cache
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Cache"}}' ) AS
        WITH facet_base AS (
          SELECT 
            variable_measured, provenance as provenance_dcid, facet_id,
            ANY_VALUE(IF(provenance LIKE 'dc/base/%', SUBSTR(provenance, 9), provenance)) as import_name,
            ANY_VALUE(measurement_method) as measurement_method,
            ANY_VALUE(observation_period) as observation_period,
            ANY_VALUE(unit) as unit,
            ANY_VALUE(scaling_factor) as scaling_factor,
            ANY_VALUE(is_dc_aggregate) as is_dc_aggregate,
            MIN(min_date) as min_date,
            MAX(max_date) as max_date,
            MIN(min_value) as facet_min,
            MAX(max_value) as facet_max,
            SUM(observation_count) as facet_obs_count,
            COUNT(*) as facet_ts_count
          FROM `temp_series_summary`
          GROUP BY variable_measured, provenance, facet_id
        ),
        facet_summaries AS (
          SELECT 
            fb.variable_measured,
            fb.provenance_dcid,
            fb.facet_id,
            fb.import_name,
            fb.measurement_method,
            fb.observation_period,
            fb.unit,
            fb.scaling_factor,
            fb.is_dc_aggregate,
            fb.min_date,
            fb.max_date,
            fb.facet_min,
            fb.facet_max,
            fb.facet_obs_count,
            fb.facet_ts_count,
            ARRAY_AGG(STRUCT(pts.place_type, pts.place_count, pts.min_val, pts.max_val, pts.top_places)) as pt_summaries
          FROM facet_base fb
          LEFT JOIN `temp_place_type_summary` pts USING (variable_measured, provenance_dcid, facet_id)
          GROUP BY 
            variable_measured, provenance_dcid, facet_id, import_name, measurement_method,
            observation_period, unit, scaling_factor, is_dc_aggregate, min_date, max_date,
            facet_min, facet_max, facet_obs_count, facet_ts_count
        )
        SELECT
          'ProvenanceSummary' as type,
          variable_measured as key,
          provenance_dcid as provenance,
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
                  'is_dc_aggregate', COALESCE(is_dc_aggregate, false)
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
                      ARRAY_AGG(place_type) as keys,
                      ARRAY_AGG(JSON_OBJECT(
                        'place_count', place_count,
                        'min_value', min_val,
                        'max_value', max_val,
                        'top_places', (
                          SELECT ARRAY_AGG(JSON_OBJECT('dcid', tp.dcid, 'name', tp.name))
                          FROM UNNEST(top_places) tp
                        )
                      )) as vals
                    FROM UNNEST(pt_summaries)
                    WHERE place_type IS NOT NULL
                  )
                )
              )
            )
          ) as value
        FROM facet_summaries
        GROUP BY variable_measured, provenance_dcid;
        """
        return self.executor.execute(query)
