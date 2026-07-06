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
        -- Step 1: Fetch joined TimeSeries and Observation data from Spanner
        -- We filter by provenance (which corresponds to import_name with prefix)
        CREATE OR REPLACE TEMPORARY TABLE `temp_obs_flat` AS
        SELECT 
          variable_measured, 
          entity1 as observation_about, 
          facet_id, 
          provenance,
          observation_period,
          measurement_method,
          unit,
          JSON_VALUE(facet, '$.scalingFactor') as scaling_factor,
          CAST(JSON_VALUE(facet, '$.isDcAggregate') AS BOOL) as is_dc_aggregate,
          date as date_val,
          SAFE_CAST(value AS FLOAT64) as value_num
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT 
               variable_measured, 
               entity1, 
               facet_id, 
               provenance,
               observation_period,
               measurement_method,
               unit,               
               facet,
               date,
               value
             FROM TimeSeries
             JOIN Observation USING (variable_measured, entity1, extra_entities_id, facet_id)
             WHERE provenance IN ({provenances_str}) ''');

        -- Step 2: Fetch ALL Node names (Narrow selection to reduce data transfer)
        CREATE OR REPLACE TEMPORARY TABLE `temp_node_names` AS
        SELECT subject_id, name 
        FROM EXTERNAL_QUERY("{connection_id}",
          "SELECT subject_id, name FROM Node WHERE name IS NOT NULL");

        -- Step 3: Fetch ALL typeOf edges (Narrow selection)
        CREATE OR REPLACE TEMPORARY TABLE `temp_type_edges` AS
        SELECT subject_id, object_id as place_type
        FROM EXTERNAL_QUERY("{connection_id}",
          "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf'");

        -- Step 4: Join and Flatten in BigQuery
        CREATE OR REPLACE TEMPORARY TABLE `temp_prepared` AS
        SELECT 
          raw.*,
          IF(raw.provenance LIKE 'dc/base/%', SUBSTR(raw.provenance, 9), raw.provenance) as import_name,
          nodes.name as place_name,
          edges.place_type
        FROM `temp_obs_flat` raw
        LEFT JOIN `temp_node_names` nodes ON raw.observation_about = nodes.subject_id
        LEFT JOIN `temp_type_edges` edges ON raw.observation_about = edges.subject_id;

        -- Step 5: Aggregate Place Type Summaries (with distinct top_places)
        CREATE OR REPLACE TEMPORARY TABLE `temp_place_type_summary` AS
        WITH place_stats AS (
          SELECT
            variable_measured,
            provenance,
            facet_id,
            place_type,
            MIN(value_num) as min_val,
            MAX(value_num) as max_val,
            COUNT(DISTINCT observation_about) as place_count
          FROM `temp_prepared`
          WHERE place_type IS NOT NULL
          GROUP BY variable_measured, provenance, facet_id, place_type
        ),
        distinct_places AS (
          SELECT DISTINCT
            variable_measured,
            provenance,
            facet_id,
            place_type,
            observation_about as dcid,
            place_name as name
          FROM `temp_prepared`
          WHERE place_type IS NOT NULL
        ),
        aggregated_places AS (
          SELECT
            variable_measured,
            provenance,
            facet_id,
            place_type,
            ARRAY_AGG(
              STRUCT(dcid, name)
              ORDER BY dcid LIMIT 3
            ) as top_places
          FROM distinct_places
          GROUP BY variable_measured, provenance, facet_id, place_type
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

        -- Step 6: Final aggregation and export to Cache
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
            MIN(date_val) as min_date,
            MAX(date_val) as max_date,
            MIN(value_num) as facet_min,
            MAX(value_num) as facet_max,
            COUNT(*) as facet_obs_count,
            COUNT(DISTINCT observation_about) as facet_ts_count
          FROM `temp_obs_flat`
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
