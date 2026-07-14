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

import logging
from typing import List
from google.cloud import spanner

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
        """Deletes aggregated data for the specified imports from Spanner.

        Uses Partitioned DML to execute deletions, which is safe for large volumes.

        Args:
            imports_to_delete: List of import names (without dc/base/ prefix) to delete.
        """
        if not imports_to_delete:
            return

        logging.info(f"Deleting existing aggregated data for imports: {imports_to_delete}")

        prefix = "dc/base/" if self.is_base_dc else ""
        provenance_names = [f"{prefix}{name}" for name in imports_to_delete]
        
        db = self.spanner_database
        edge_params = {"provenances": provenance_names}
        edge_param_types = {"provenances": spanner.param_types.Array(spanner.param_types.STRING)}

        try:
            # 1. Delete from Edge
            edge_delete_sql = "DELETE FROM Edge WHERE provenance IN UNNEST(@provenances)"
            edge_rows = db.execute_partitioned_dml(
                edge_delete_sql, params=edge_params, param_types=edge_param_types
            )
            logging.info(f"Deleted {edge_rows} rows from Edge table.")

            # 2. Delete from TimeSeries (cascades to Observation)
            ts_delete_sql = "DELETE FROM TimeSeries WHERE provenance IN UNNEST(@provenances)"
            ts_rows = db.execute_partitioned_dml(
                ts_delete_sql, params=edge_params, param_types=edge_param_types
            )
            logging.info(f"Deleted {ts_rows} rows from TimeSeries table (and cascaded Observations).")

            # 3. Delete from Cache (type = 'ProvenanceSummary')
            cache_delete_sql = (
                "DELETE FROM Cache WHERE type = 'ProvenanceSummary' AND provenance IN UNNEST(@provenances)"
            )
            cache_rows = db.execute_partitioned_dml(
                cache_delete_sql, params=edge_params, param_types=edge_param_types
            )
            logging.info(f"Deleted {cache_rows} rows from Cache table.")
        except Exception as e:
            logging.error(f"Failed to execute partitioned DML for deletions: {e}")
            raise
