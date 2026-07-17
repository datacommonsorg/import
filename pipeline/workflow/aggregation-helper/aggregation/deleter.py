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

    def delete_aggregated_data(
        self,
        imports_to_delete: List[str],
        imports_to_delete_generated: List[str]
    ) -> None:
        """Deletes aggregated data and generated graphs from Spanner concurrently.

        Uses Partitioned DML to execute deletions, which is safe for large volumes.
        Runs deletions for Edge, TimeSeries, and KeyValueStore in parallel using ThreadPoolExecutor.

        Args:
            imports_to_delete: List of import names to delete all data for (TimeSeries, Obs, KeyValueStore, Edge).
            imports_to_delete_generated: List of import names to delete generated graphs for (Edge only).
        """
        if not imports_to_delete and not imports_to_delete_generated:
            return

        logging.info(
            f"Deleting aggregated data for: {imports_to_delete}, "
            f"and generated graphs for: {imports_to_delete_generated}"
        )

        db = self.spanner_database
        
        # 1. Resolve standard provenances for data deletion
        data_provenances = [get_provenance_name(name, self.is_base_dc) for name in imports_to_delete]
        
        # 2. Resolve generated provenances for generated graph deletion
        gen_prefix = "dc/base/generated/" if self.is_base_dc else "generated/"
        generated_provenances = [f"{gen_prefix}{name}" for name in imports_to_delete_generated]

        # We need to delete from Edge table if provenance is in data_provenances OR generated_provenances
        edge_provenances = data_provenances + generated_provenances

        # Setup parameters for partitioned DML
        params_data = {"provenances": data_provenances}
        param_types_data = {"provenances": spanner.param_types.Array(spanner.param_types.STRING)}

        params_edge = {"provenances": edge_provenances}
        param_types_edge = {"provenances": spanner.param_types.Array(spanner.param_types.STRING)}

        delete_queries = []
        if data_provenances:
            delete_queries.extend([
                ("TimeSeries", "DELETE FROM TimeSeries WHERE provenance IN UNNEST(@provenances)", " (and cascaded Observations)", params_data, param_types_data),
                ("KeyValueStore", "DELETE FROM KeyValueStore WHERE type = 'ProvenanceSummary' AND provenance IN UNNEST(@provenances)", "", params_data, param_types_data)
            ])
        
        if edge_provenances:
            delete_queries.append(
                ("Edge", "DELETE FROM Edge WHERE provenance IN UNNEST(@provenances)", "", params_edge, param_types_edge)
            )

        def _execute_delete(table_name: str, sql: str, extra_desc: str, params, param_types) -> int:
            rows = db.execute_partitioned_dml(
                sql, params=params, param_types=param_types
            )
            logging.info(f"Deleted {rows} rows from {table_name} table{extra_desc}.")
            return rows

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(delete_queries))) as executor:
                futures = [
                    executor.submit(_execute_delete, table, sql, desc, params, param_types)
                    for table, sql, desc, params, param_types in delete_queries
                ]
                for future in concurrent.futures.as_completed(futures):
                    future.result()  # Propagate any worker thread exceptions to main thread
        except Exception as e:
            logging.error(f"Failed to execute partitioned DML for deletions: {e}")
            raise
