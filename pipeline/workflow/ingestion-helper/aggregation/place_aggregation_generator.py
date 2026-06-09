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
"""Place-based aggregations."""

import logging
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class PlaceAggregationGenerator:
    """Generates and runs place-based aggregations using BigQuery Federation."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self, import_names: List[str], variable: str,
                measurement_method: str,
                date: str) -> List[bigquery.job.QueryJob]:
        """Runs all place aggregations asynchronously and returns their jobs.

        Args:
            import_names: List of import names to filter by.
            variable: The variable to aggregate (e.g., 'Count_Person').
            measurement_method: The measurement method to filter by (e.g.,
              'CensusACS5yrSurvey').
            date: The date to filter by (e.g., '2020').
        """
        if not import_names:
            logging.info("No imports specified. Skipping place aggregations.")
            return []

        logging.info(f"Running place aggregations for imports: {import_names}")
        jobs = [
            self.aggregate_us_population_from_states(import_names, variable,
                                                     measurement_method, date),
            self.aggregate_us_state_population_from_counties(
                import_names, variable, measurement_method, date)
        ]
        return [job for job in jobs if job]

    def aggregate_us_population_from_states(
            self, import_names: List[str], variable: str,
            measurement_method: str,
            date: str) -> Optional[bigquery.job.QueryJob]:
        """Calculates US country population by summing up populations of its states.

        Args:
            import_names: List of import names to filter by.
            variable: The variable to aggregate (e.g., 'Count_Person').
            measurement_method: The measurement method to filter by (e.g.,
              'CensusACS5yrSurvey').
            date: The date to filter by (e.g., '2020').
        """
        if not import_names:
            return None

        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        imports_str = ", ".join([f"'{name}'" for name in safe_names])

        obs_import_filter = ""
        if imports_str:
            obs_import_filter = f"AND import_name IN ({imports_str})"

        # Query TimeSeries and Observation separately from Spanner and JOIN in BigQuery
        # to support Spanner Databoost.
        query = f"""
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation_final_v2"}}' ) AS
        SELECT 
            "country/USA" AS entity1, 
            ts.variable_measured,
            ts.facets_id,
            ts.extra_entities_id,
            obs.date,
            SUM(obs.double_value) AS double_value
        FROM (
          SELECT variable_measured, entity1, extra_entities_id, facets_id
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT variable_measured, entity1, extra_entities_id, facets_id
            FROM TimeSeries_final_v2
            WHERE variable_measured = '{variable}'
              AND STARTS_WITH(entity1, 'geoId/') 
              AND LENGTH(entity1) = 8
              AND JSON_VALUE(facets, '$.measurementMethod') = '{measurement_method}'
            '''
          )
        ) ts
        JOIN (
          SELECT variable_measured, entity1, extra_entities_id, facets_id, date, double_value
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT variable_measured, entity1, extra_entities_id, facets_id, date, double_value
            FROM Observation_final_v2
            WHERE variable_measured = '{variable}'
              AND STARTS_WITH(entity1, 'geoId/') 
              AND LENGTH(entity1) = 8
              AND date = '{date}'
              {obs_import_filter}
            '''
          )
        ) obs ON 
            ts.variable_measured = obs.variable_measured AND 
            ts.entity1 = obs.entity1 AND 
            ts.extra_entities_id = obs.extra_entities_id AND 
            ts.facets_id = obs.facets_id
        GROUP BY ts.variable_measured, ts.facets_id, ts.extra_entities_id, obs.date
        """

        logging.info("Running US population aggregation query...")
        return self.executor.execute(query)

    def aggregate_us_state_population_from_counties(
            self, import_names: List[str], variable: str,
            measurement_method: str,
            date: str) -> Optional[bigquery.job.QueryJob]:
        """Calculates US state populations by summing up populations of their counties.

        Args:
            import_names: List of import names to filter by.
            variable: The variable to aggregate (e.g., 'Count_Person').
            measurement_method: The measurement method to filter by (e.g.,
              'CensusACS5yrSurvey').
            date: The date to filter by (e.g., '2020').
        """
        if not import_names:
            return None

        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        imports_str = ", ".join([f"'{name}'" for name in safe_names])

        obs_import_filter = ""
        if imports_str:
            obs_import_filter = f"AND import_name IN ({imports_str})"

        # Query TimeSeries and Observation separately from Spanner and JOIN in BigQuery
        # to support Spanner Databoost.
        query = f"""
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Observation_final_v2"}}' ) AS
        SELECT 
            SUBSTR(ts.entity1, 1, 8) AS entity1, 
            ts.variable_measured,
            ts.facets_id,
            ts.extra_entities_id,
            obs.date,
            SUM(obs.double_value) AS double_value
        FROM (
          SELECT variable_measured, entity1, extra_entities_id, facets_id
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT variable_measured, entity1, extra_entities_id, facets_id
            FROM TimeSeries_final_v2
            WHERE variable_measured = '{variable}'
              AND STARTS_WITH(entity1, 'geoId/') 
              AND LENGTH(entity1) = 11
              AND JSON_VALUE(facets, '$.measurementMethod') = '{measurement_method}'
            '''
          )
        ) ts
        JOIN (
          SELECT variable_measured, entity1, extra_entities_id, facets_id, date, double_value
          FROM EXTERNAL_QUERY(
            "{connection_id}",
            '''
            SELECT variable_measured, entity1, extra_entities_id, facets_id, date, double_value
            FROM Observation_final_v2
            WHERE variable_measured = '{variable}'
              AND STARTS_WITH(entity1, 'geoId/') 
              AND LENGTH(entity1) = 11
              AND date = '{date}'
              {obs_import_filter}
            '''
          )
        ) obs ON 
            ts.variable_measured = obs.variable_measured AND 
            ts.entity1 = obs.entity1 AND 
            ts.extra_entities_id = obs.extra_entities_id AND 
            ts.facets_id = obs.facets_id
        GROUP BY entity1, ts.variable_measured, ts.facets_id, ts.extra_entities_id, obs.date
        """

        logging.info("Running State population aggregation query...")
        return self.executor.execute(query)
