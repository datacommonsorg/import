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
"""Place-based aggregation generator."""

import logging
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor


class PlaceAggregationGenerator:
    """Generates and runs place-based aggregations (e.g., summing sub-place data to parent places)."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self, import_names: List[str]) -> List[bigquery.job.QueryJob]:
        """Runs all place aggregations asynchronously and returns their jobs."""
        if not import_names:
            logging.info("No imports specified. Skipping place aggregations.")
            return []

        logging.info(f"Running place aggregations for imports: {import_names}")
        jobs = [
            self.aggregate_us_population_from_states(import_names),
            self.aggregate_us_state_population_from_counties(import_names)
        ]
        return [job for job in jobs if job]

    def aggregate_us_population_from_states(
            self, import_names: List[str]) -> Optional[bigquery.job.QueryJob]:
        """Calculates US country population by summing up populations of its states."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id
        imports_str = ", ".join([f"'{name}'" for name in import_names])

        # Placeholder SQL query that shows how the aggregation from US states to US country would work.
        query = f"""
        -- Placeholder: Calculate US population by summing state populations
        -- 1. Fetch all population observations for US states and US country.
        -- 2. Sum state-level populations and write/update country-level population.

        CREATE OR REPLACE TEMPORARY TABLE `temp_state_populations` AS
        SELECT
          observation_about,
          variable_measured,
          value_num,
          date_val
        FROM (
          -- This is a conceptual query structure reflecting state population lookup
          SELECT
            'geoId/06' as observation_about, -- California as placeholder
            'Count_Person' as variable_measured,
            39000000.0 as value_num,
            '2020' as date_val
        );

        -- Conceptual aggregation logic:
        -- SELECT SUM(value_num) as us_population, date_val
        -- FROM temp_state_populations
        -- GROUP BY date_val;

        SELECT 'USA' as place, 'Count_Person' as variable, 330000000.0 as sum_population;
        """

        logging.info("Running placeholder US population aggregation query...")
        return self.executor.execute(query)

    def aggregate_us_state_population_from_counties(
            self, import_names: List[str]) -> Optional[bigquery.job.QueryJob]:
        """Calculates US state populations by summing up populations of their counties."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        connection_id = self.executor.connection_id
        imports_str = ", ".join([f"'{name}'" for name in import_names])

        # Placeholder SQL query that shows how the aggregation from US counties to US states would work.
        query = f"""
        -- Placeholder: Calculate US state populations by summing county populations
        -- 1. Fetch all population observations for US counties and US states.
        -- 2. Sum county-level populations and write/update state-level populations.

        CREATE OR REPLACE TEMPORARY TABLE `temp_county_populations` AS
        SELECT
          observation_about,
          variable_measured,
          value_num,
          date_val
        FROM (
          -- This is a conceptual query structure reflecting county population lookup
          SELECT
            'geoId/06075' as observation_about, -- San Francisco County as placeholder
            'Count_Person' as variable_measured,
            800000.0 as value_num,
            '2020' as date_val
        );

        -- Conceptual aggregation logic:
        -- SELECT SUM(value_num) as state_population, date_val
        -- FROM temp_county_populations
        -- GROUP BY date_val;

        SELECT 'geoId/06' as place, 'Count_Person' as variable, 39000000.0 as sum_population;
        """

        logging.info("Running placeholder State population aggregation query...")
        return self.executor.execute(query)
