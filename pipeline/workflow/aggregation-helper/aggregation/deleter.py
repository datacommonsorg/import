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

"""Deletes aggregated data in Spanner using Partitioned DML."""

import concurrent.futures
import logging
from typing import List
from google.cloud import spanner

from .common import get_provenance_name

class AggregationDeleter:
    """Handles deletion of aggregated data in Spanner."""

    def __init__(self, project_id: str, instance_id: str, database_id: str, is_base_dc: bool = True):
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.is_base_dc = is_base_dc
        self._spanner_database = None

    @property
    def spanner_database(self):
        """Lazily initializes and returns the Spanner Database client."""
        if self._spanner_database is None:
            spanner_client = spanner.Client(project=self.project_id)
            instance = spanner_client.instance(self.instance_id)
            self._spanner_database = instance.database(self.database_id)
            logging.info(f"Initialized Spanner client for deleter: {self._spanner_database.name}")
        return self._spanner_database

    def delete_aggregated_data(self, imports_to_delete: List[str]) -> None:
        """Deletes aggregated data for the specified imports from Spanner concurrently.

        Uses Partitioned DML to execute deletions, which is safe for large volumes.
        Runs deletions for Edge, TimeSeries, and Cache in parallel using ThreadPoolExecutor.

        Args:
            imports_to_delete: List of import names (without dc/base/ prefix) to delete.
        """
        if not imports_to_delete:
            return

        logging.info(f"Deleting existing aggregated data for imports: {imports_to_delete}")

        provenance_names = [get_provenance_name(name, self.is_base_dc) for name in imports_to_delete]
        
        db = self.spanner_database
        params = {"provenances": provenance_names}
        param_types = {"provenances": spanner.param_types.Array(spanner.param_types.STRING)}

        delete_queries = [
            ("Edge", "DELETE FROM Edge WHERE provenance IN UNNEST(@provenances)", ""),
            ("TimeSeries", "DELETE FROM TimeSeries WHERE provenance IN UNNEST(@provenances)", " (and cascaded Observations)"),
            ("Cache", "DELETE FROM Cache WHERE type = 'ProvenanceSummary' AND provenance IN UNNEST(@provenances)", "")
        ]

        def _execute_delete(table_name: str, sql: str, extra_desc: str) -> int:
            rows = db.execute_partitioned_dml(
                sql, params=params, param_types=param_types
            )
            logging.info(f"Deleted {rows} rows from {table_name} table{extra_desc}.")
            return rows

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=len(delete_queries)) as executor:
                futures = [
                    executor.submit(_execute_delete, table, sql, desc)
                    for table, sql, desc in delete_queries
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # Propagate any worker thread exceptions to main thread
        except Exception as e:
            logging.error(f"Failed to execute partitioned DML for deletions: {e}")
            raise
