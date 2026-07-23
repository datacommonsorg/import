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
from dataclasses import dataclass
from functools import lru_cache
import io
import itertools
import json
import logging
from typing import Any, Dict, List, Optional
import urllib.request
import urllib.error

from google.cloud import bigquery
from google.cloud import spanner
from google.cloud import storage
from pydantic import BaseModel
from .bq_executor import BigQueryExecutor


@dataclass
class EmbeddingGenerationConfig:
    """Configuration for embedding generation."""
    specs: Optional[List[Any]] = None
    embedding_table: str = "NodeEmbedding"


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
def _extract_nl_stat_var() -> list[dict[str, str]]:
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

    seen = set()
    records = []
    reader = csv.DictReader(io.StringIO(content))
    for row in reader:
        dcid_str = row.get("dcid")
        sentence = row.get("sentence")
        if dcid_str and sentence:
            sentence = sentence.strip()
            for item in dcid_str.split(";"):
                item = item.strip()
                if item and sentence:
                    pair = (item, sentence)
                    if pair not in seen:
                        seen.add(pair)
                        records.append({"dcid": item, "sentence": sentence})
    return records


class EmbeddingGenerator:
    """Generates Node embeddings asynchronously in BigQuery and ingests them into Spanner."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the EmbeddingGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc
        self._spanner_database = None

    @property
    def spanner_database(self):
        """Lazily initializes and returns the Spanner Database client."""
        if self._spanner_database is None:
            spanner_client = spanner.Client(project=self.executor.project_id)
            instance = spanner_client.instance(self.executor.instance_id)
            self._spanner_database = instance.database(self.executor.database_id)
            logging.info(f"Initialized Spanner client for EmbeddingGenerator: {self._spanner_database.name}")
        return self._spanner_database

    def _delete_existing_embeddings(self, spec: EmbeddingSpec, embedding_table: str = "NodeEmbedding") -> int:
        """Deletes existing embeddings in Spanner for nodes matching the spec before re-generation."""
        try:
            db = self.spanner_database
            lock_sql = "SELECT MAX(AcquiredTimestamp) FROM IngestionLock"
            latest_lock_timestamp = None
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(lock_sql)
                for row in results:
                    latest_lock_timestamp = row[0]

            params = {"node_types": spec.node_types}
            param_types = {"node_types": spanner.param_types.Array(spanner.param_types.STRING)}

            timestamp_condition = "last_update_timestamp > @timestamp" if latest_lock_timestamp else "TRUE"
            if latest_lock_timestamp:
                params["timestamp"] = latest_lock_timestamp
                param_types["timestamp"] = spanner.param_types.TIMESTAMP

            if spec.node_filter_type == "NLStatisticalVariable":
                nl_records = _extract_nl_stat_var()
                dcids = sorted(list({r["dcid"] for r in nl_records}))
                params["nl_stat_vars"] = dcids
                param_types["nl_stat_vars"] = spanner.param_types.Array(spanner.param_types.STRING)
                filter_condition = "subject_id IN UNNEST(@nl_stat_vars)"
            else:
                filter_condition = "TRUE"

            node_select_sql = f"""
                SELECT subject_id FROM Node
                WHERE name IS NOT NULL
                  AND name <> ''
                  AND {timestamp_condition}
                  AND {filter_condition}
                  AND EXISTS (
                    SELECT 1 FROM UNNEST(types) AS t WHERE t IN UNNEST(@node_types)
                  )
            """

            subject_ids = []
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(node_select_sql, params=params, param_types=param_types)
                subject_ids = [row[0] for row in results]

            if not subject_ids:
                logging.info(f"No nodes found to delete existing embeddings for label '{spec.embedding_label}'.")
                return 0

            logging.info(f"Deleting existing embeddings in {embedding_table} for {len(subject_ids)} nodes (label: {spec.embedding_label})...")
            delete_sql = f"""
                DELETE FROM {embedding_table}
                WHERE embedding_label = @embedding_label
                  AND subject_id IN UNNEST(@subject_ids)
            """

            def chunked(iterable, n):
                it = iter(iterable)
                while True:
                    chunk = list(itertools.islice(it, n))
                    if not chunk:
                        break
                    yield chunk

            total_deleted = 0
            for batch in chunked(subject_ids, 1000):
                del_params = {
                    "embedding_label": spec.embedding_label,
                    "subject_ids": batch
                }
                del_param_types = {
                    "embedding_label": spanner.param_types.STRING,
                    "subject_ids": spanner.param_types.Array(spanner.param_types.STRING)
                }
                rows = db.execute_partitioned_dml(delete_sql, params=del_params, param_types=del_param_types)
                total_deleted += rows

            logging.info(f"Deleted {total_deleted} existing embedding rows for label '{spec.embedding_label}'.")
            return total_deleted
        except Exception as e:
            logging.error(f"Failed to delete existing embeddings in Spanner: {e}")
            raise

    def run_all(self,
                config: EmbeddingGenerationConfig) -> List[bigquery.job.QueryJob]:
        """Runs all embedding generations asynchronously and returns their jobs."""
        specs = config.specs
        embedding_table = config.embedding_table
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
        project_id = self.executor.project_id
        bq_dataset_id = self.executor.bq_dataset_id
        location = self.executor.location

        embedding_label = spec.embedding_label
        model_name = spec.model_name
        model_endpoint = spec.model_endpoint
        task_type = spec.task_type
        node_types = spec.node_types
        node_filter_type = spec.node_filter_type

        # 1. Pre-delete existing embeddings in Spanner for updated nodes
        self._delete_existing_embeddings(spec, embedding_table=embedding_table)

        # 1. Format node types list for Spanner
        safe_types = [f"'{nt.replace(chr(39), chr(92) + chr(39))}'" for nt in node_types]
        node_types_list_sql = f"[{', '.join(safe_types)}]"

        # 2. Build the select query and query parameters for BigQuery
        job_config = None
        if node_filter_type == "NoFilter":
            select_nodes_sql = """
                SELECT 
                  subject_id, 
                  CAST(FARM_FINGERPRINT(JSON_VALUE(embedding_content, '$.name')) AS STRING) AS embedding_content_key,
                  TO_JSON_STRING(embedding_content) AS content, 
                  embedding_content, 
                  node_types 
                FROM raw_nodes
            """
        elif node_filter_type == "NLStatisticalVariable":
            select_nodes_sql = """
                SELECT 
                  r.subject_id, 
                  CAST(FARM_FINGERPRINT(m.sentence) AS STRING) AS embedding_content_key,
                  m.sentence AS content, 
                  JSON_OBJECT("title", r.subject_id, "name", m.sentence) AS embedding_content, 
                  r.node_types 
                FROM UNNEST(@nl_stat_vars) m
                INNER JOIN raw_nodes r ON r.subject_id = m.dcid
            """
            nl_records = _extract_nl_stat_var()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(
                        "nl_stat_vars",
                        "RECORD",
                        [
                            bigquery.StructQueryParameter(
                                "",
                                bigquery.ScalarQueryParameter("dcid", "STRING", rec["dcid"]),
                                bigquery.ScalarQueryParameter("sentence", "STRING", rec["sentence"])
                            )
                            for rec in nl_records
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
          embedding_content_key,
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
