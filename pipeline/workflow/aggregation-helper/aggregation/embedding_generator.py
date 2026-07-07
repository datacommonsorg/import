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

import csv
from functools import lru_cache
import io
import json
import logging
import os
from typing import Any, Dict, List, Optional

from google.cloud import bigquery
from google.cloud import storage
from pydantic import BaseModel
from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

_NL_STAT_VAR_FILE = "gs://datcom-nl-models/base_uae_mem_2025_11_03_07_10_42/embeddings.csv"

class EmbeddingSpec(BaseModel):
    embedding_label: str
    model_name: str
    model_endpoint: str = "text-embedding-005"
    task_type: str
    node_types: List[str]
    node_filter_type: str

_DEFAULT_EMBEDDING_SPECS = [
    EmbeddingSpec(
        embedding_label="base_text_embedding",
        model_name="NodeEmbeddingModel",
        model_endpoint="text-embedding-005",
        task_type="RETRIEVAL_QUERY",
        node_types=["StatisticalVariable", "Topic"],
        node_filter_type="NoFilter"
    )
]


@lru_cache(maxsize=1)
def _extract_nl_stat_var() -> List[str]:
    path = _NL_STAT_VAR_FILE
    content = ""
    if path.startswith("gs://"):
        try:
            import urllib.request
            url = "https://storage.googleapis.com/" + path[5:]
            with urllib.request.urlopen(url) as resp:
                content = resp.read().decode("utf-8")
        except Exception as e:
            logging.info(f"HTTP fetch for NL stat var file failed ({e}), falling back to GCS client.")
            parts = path[5:].split("/", 1)
            client = storage.Client()
            content = client.bucket(parts[0]).blob(parts[1]).download_as_text()
    else:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

    dcids = set()
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        dcid_str = row.get("dcid")
        if dcid_str:
            for item in dcid_str.split(";"):
                item = item.strip()
                if item:
                    dcids.add(item)
    return list(dcids)


class EmbeddingGenerator:
    """Generates embeddings using Vertex AI remote models and saves them to Spanner."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the EmbeddingGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self, specs: Optional[List[Any]] = None, import_names: List[str] = None) -> List[bigquery.job.QueryJob]:
        """Runs all embedding generations asynchronously and returns their jobs."""
        enable_embeddings = os.environ.get('ENABLE_EMBEDDINGS', 'false').lower() == 'true'
        if not enable_embeddings or not self.is_base_dc:
            logging.info("Embeddings generation is disabled in config/env or not in base DC. Skipping.")
            return []

        if not specs:
            specs = _DEFAULT_EMBEDDING_SPECS

        logging.info(f"Running embedding generation aggregation for {len(specs)} spec(s)...")
        jobs = []
        for spec in specs:
            job = self.run_embedding_spec(spec)
            if job:
                jobs.append(job)
        return jobs

    def run_embedding_spec(self, spec: Any) -> Optional[bigquery.job.QueryJob]:
        """Runs the embedding generation query for a single spec."""
        if isinstance(spec, dict):
            spec = EmbeddingSpec(**spec)

        dest = self.executor.get_spanner_destination_uri()
        conn_id = self.executor.connection_id
        embedding_conn_id = 'embedding_vai_model_connection'
        project_id = self.executor.project_id
        bq_dataset_id = os.environ.get('BQ_DATASET_ID', 'datacommons')
        location = self.executor.location or os.environ.get('LOCATION') or os.environ.get('REGION', 'us-central1')
        embedding_table = os.environ.get('EMBEDDING_TABLE', 'NodeEmbedding')

        embedding_label = spec.embedding_label
        model_name = spec.model_name
        model_endpoint = spec.model_endpoint
        task_type = spec.task_type
        node_types = spec.node_types
        node_filter_type = spec.node_filter_type

        # 1. Format node types list for Spanner
        safe_types = [f"'{nt.replace(chr(39), chr(92) + chr(39))}'" for nt in node_types]
        node_types_list_sql = f"[{', '.join(safe_types)}]"

        # 2. Build the filter condition for Spanner
        if node_filter_type == "NoFilter":
            filter_condition_sql = "TRUE"
        elif node_filter_type == "NLStatisticalVariable":
            nl_stat_vars = _extract_nl_stat_var()
            safe_vars = [f"'{var.replace(chr(39), chr(92) + chr(39))}'" for var in nl_stat_vars]
            filter_condition_sql = f"subject_id IN UNNEST([{', '.join(safe_vars)}])"
        else:
            logging.error(f"Unknown node filter type: {node_filter_type}")
            return None

        # 3. Construct the query to extract raw nodes from Spanner, generate embeddings in BigQuery, and export back to Spanner
        spanner_query = f"""
            SELECT 
              subject_id, 
              JSON_OBJECT("title", subject_id, "name", name) AS embedding_content, 
              types AS node_types
            FROM Node
            WHERE name IS NOT NULL
              AND %s
              AND {filter_condition_sql}
              AND EXISTS (
                SELECT 1 FROM UNNEST(types) AS t WHERE t IN UNNEST({node_types_list_sql})
              )
        """
        spanner_query_str = f'"""{spanner_query}"""'

        query = f"""
        DECLARE latest_lock_timestamp TIMESTAMP;
        DECLARE timelock_condition STRING;

        CREATE OR REPLACE MODEL `{project_id}.{bq_dataset_id}.{model_name}`
        REMOTE WITH CONNECTION `{project_id}.{location}.{embedding_conn_id}`
        OPTIONS (
          ENDPOINT = "{model_endpoint}"
        );

        SET latest_lock_timestamp = (
          SELECT MAX(CAST(val AS TIMESTAMP))
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT AcquiredTimestamp AS val FROM IngestionLock")
        );
        
        IF latest_lock_timestamp IS NOT NULL THEN
          SET timelock_condition = FORMAT('last_update_timestamp > \\\'%s\\\'', CAST(latest_lock_timestamp AS STRING));
        ELSE
          SET timelock_condition = 'TRUE';
        END IF;

        EXECUTE IMMEDIATE FORMAT('''
          -- 1. Get raw text from Spanner
          CREATE TEMP TABLE raw_nodes AS
          SELECT * FROM EXTERNAL_QUERY("{conn_id}", {spanner_query_str});
        ''', 
        timelock_condition
        );

        -- 2. Generate embeddings natively in BigQuery
        CREATE TEMP TABLE embedding_staging AS
        SELECT 
          subject_id, 
          "{embedding_label}" AS embedding_label, 
          embedding_content, 
          node_types, 
          ml_generate_embedding_result AS embeddings
        FROM ML.GENERATE_EMBEDDING(
          MODEL `{project_id}.{bq_dataset_id}.{model_name}`,
          (SELECT subject_id, TO_JSON_STRING(embedding_content) AS content, embedding_content, node_types FROM raw_nodes),
          STRUCT("{task_type}" AS task_type)
        );

        -- 3. Export back to Spanner
        EXPORT DATA OPTIONS(
          uri="{dest}",
          format="CLOUD_SPANNER",
          spanner_options='{{"table": "{embedding_table}"}}'
        ) AS
        SELECT * FROM embedding_staging;
        """
        logging.info(f"Submitting embedding generation job for {embedding_label}...")
        return self.executor.execute(query)
