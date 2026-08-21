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
from dataclasses import dataclass
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .common import _escape_sql_literal


@dataclass
class NodePropertiesConfig:
    """Configuration for node properties aggregation."""
    import_names: Optional[List[str]] = None


class NodePropertiesGenerator:
    """Generates node properties (types and resolved names) and exports them to Spanner Node table."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the NodePropertiesGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self,
                config: Optional[NodePropertiesConfig] = None) -> List[bigquery.job.QueryJob]:
        """Runs node properties aggregations (types and names) asynchronously and returns their jobs."""
        logging.info("Running Node Properties aggregation (types and names)...")
        jobs = [
            self.run_node_types(),
            self.run_node_names(),
        ]
        return [job for job in jobs if job]

    def run_node_types(self) -> Optional[bigquery.job.QueryJob]:
        """Aggregates typeOf edges into Node types and exports to Spanner Node table."""
        dest_uri = _escape_sql_literal(self.executor.get_spanner_destination_uri())
        conn_id = _escape_sql_literal(self.executor.connection_id)

        query = f"""  # nosec
        CREATE OR REPLACE TEMP TABLE edges AS
        SELECT DISTINCT subject_id, object_id FROM EXTERNAL_QUERY(
          "{conn_id}",
          '''SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf' '''
        );

        CREATE OR REPLACE TEMP TABLE types AS
        SELECT subject_id, ARRAY_AGG(object_id ORDER BY object_id) AS types
        FROM edges
        GROUP BY subject_id;

        EXPORT DATA
          OPTIONS(
            uri="{dest_uri}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}'
          ) AS
        SELECT * FROM types;
        """
        logging.info("Submitting Node Types aggregation BQ job...")
        return self.executor.execute(query)

    def run_node_names(self) -> Optional[bigquery.job.QueryJob]:
        """Resolves name edges against literal nodes and exports to Spanner Node table."""
        dest_uri = _escape_sql_literal(self.executor.get_spanner_destination_uri())
        conn_id = _escape_sql_literal(self.executor.connection_id)

        query = f"""  # nosec
        CREATE OR REPLACE TEMP TABLE edges AS
        SELECT DISTINCT subject_id, object_id FROM EXTERNAL_QUERY(
          "{conn_id}",
          '''SELECT subject_id, object_id FROM Edge WHERE predicate = 'name' '''
        );

        CREATE OR REPLACE TEMP TABLE nodes AS
        SELECT * FROM EXTERNAL_QUERY(
          "{conn_id}",
          '''SELECT subject_id, value FROM Node WHERE types IS NULL OR ARRAY_LENGTH(types) = 0'''
        );

        CREATE OR REPLACE TEMP TABLE resolved AS
        SELECT edges.subject_id, nodes.value
        FROM edges
        JOIN nodes ON edges.object_id = nodes.subject_id;

        CREATE OR REPLACE TEMP TABLE name AS
        -- Pick lexicographic first for determinism (matches Prophet)
        SELECT subject_id, ARRAY_AGG(value ORDER BY value)[SAFE_OFFSET(0)] AS name
        FROM resolved
        GROUP BY subject_id;

        EXPORT DATA
          OPTIONS(
            uri="{dest_uri}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}'
          ) AS
        SELECT * FROM name;
        """
        logging.info("Submitting Node Names aggregation BQ job...")
        return self.executor.execute(query)
