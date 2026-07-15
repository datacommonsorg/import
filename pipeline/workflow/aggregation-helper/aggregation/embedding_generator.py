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
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error

from google.cloud import bigquery
from google.cloud import storage
from pydantic import BaseModel
from .bq_executor import BigQueryExecutor

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
def _extract_nl_stat_var() -> dict[str, str]:
    path = _NL_STAT_VAR_FILE
    content = ""
    if path.startswith("gs://"):
        try:
            url = "https://storage.googleapis.com/" + path[5:]
            with urllib.request.urlopen(url) as resp:
                content = resp.read().decode("utf-8")
        except urllib.error.URLError as e:
            logging.info(f"HTTP fetch for NL stat var file failed ({e}), falling back to GCS client.")
            parts = path[5:].split("/", 1)
            client = storage.Client()
            content = client.bucket(parts[0]).blob(parts[1]).download_as_text()
    else:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()

    dcids = dict()
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        dcid_str = row.get("dcid")
        sentence = row.get("sentence")
        if dcid_str:
            for item in dcid_str.split(";"):
                item = item.strip()
                if item:
                    dcids[item] = sentence
    return dcids


class EmbeddingGenerator:
    """Generates embeddings using Vertex AI remote models and saves them to Spanner."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the EmbeddingGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self, specs: Optional[List[Any]] = None, embedding_table: str = "NodeEmbedding") -> List[bigquery.job.QueryJob]:
        """Runs all embedding generations asynchronously and returns their jobs."""
        if not self.executor.enable_embeddings or not self.is_base_dc:
            logging.info("Embeddings generation is disabled in config/env or not in base DC. Skipping.")
            return []

        if not specs:
            specs = _DEFAULT_EMBEDDING_SPECS

        logging.info(f"Running embedding generation aggregation for {len(specs)} spec(s)...")
        jobs = []
        for spec in specs:
            job = self.run_embedding_spec(spec, embedding_table=embedding_table)
            if job:
                jobs.append(job)
        return jobs

    def run_embedding_spec(self, spec: Any, embedding_table: str = "NodeEmbedding") -> Optional[bigquery.job.QueryJob]:
        """Runs the embedding generation query for a single spec."""
        if isinstance(spec, dict):
            spec = EmbeddingSpec(**spec)

        dest = self.executor.get_spanner_destination_uri()
        conn_id = self.executor.connection_id
        embedding_conn_id = self.executor.embedding_conn_id
        project_id = self.executor.project_id
        bq_dataset_id = self.executor.bq_dataset_id
        location = self.executor.location

        embedding_label = spec.embedding_label
        model_name = spec.model_name
        model_endpoint = spec.model_endpoint
        task_type = spec.task_type
        node_types = spec.node_types
        node_filter_type = spec.node_filter_type

        # 1. Format node types list for Spanner
        safe_types = [f"'{nt.replace(chr(39), chr(92) + chr(39))}'" for nt in node_types]
        node_types_list_sql = f"[{', '.join(safe_types)}]"

        # 2. Build the select query and query parameters for BigQuery
        job_config = None
        if node_filter_type == "NoFilter":
            select_nodes_sql = """
                SELECT 
                  subject_id, 
                  JSON_VALUE(embedding_content.name) AS content, 
                  embedding_content, 
                  node_types 
                FROM raw_nodes
            """
        elif node_filter_type == "NLStatisticalVariable":
            select_nodes_sql = """
                SELECT 
                  r.subject_id, 
                  m.sentence AS content, 
                  JSON_OBJECT("title", r.subject_id, "name", m.sentence) AS embedding_content, 
                  r.node_types 
                FROM UNNEST(@nl_stat_vars) m
                INNER JOIN raw_nodes r ON r.subject_id = m.dcid
            """
            nl_dict = _extract_nl_stat_var()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(
                        "nl_stat_vars",
                        "RECORD",
                        [
                            bigquery.StructQueryParameter(
                                "",
                                bigquery.ScalarQueryParameter("dcid", "STRING", k),
                                bigquery.ScalarQueryParameter("sentence", "STRING", v)
                            )
                            for k, v in nl_dict.items()
                        ]
                    )
                ]
            )
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
              AND name <> ''
              AND %s
              AND EXISTS (
                SELECT 1 FROM UNNEST(types) AS t WHERE t IN UNNEST({node_types_list_sql})
              )
        """
        spanner_query_str = f'"""{spanner_query}"""'

        query = f"""
        DECLARE latest_lock_timestamp TIMESTAMP;
        DECLARE timelock_condition STRING;

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
          ({select_nodes_sql}),
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
        return self.executor.execute(query, job_config=job_config)
