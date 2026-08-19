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

from dataclasses import dataclass
from functools import lru_cache
import itertools
import json
import logging
from typing import Any, Dict, List, Optional
import pandas as pd

from google.cloud import bigquery
from google.cloud import spanner
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
    node_types: Dict[str, List[str]]
    node_filter_type: str

_DEFAULT_EMBEDDING_SPECS = [
    EmbeddingSpec(
        embedding_label="base_text_embedding",
        model_name="NodeEmbeddingModel",
        model_endpoint="text-embedding-005",
        task_type="RETRIEVAL_QUERY",
        node_types={
            "StatisticalVariable": ["description"],
            "Topic": ["description"]
        },
        node_filter_type="NoFilter"
    )
]


@lru_cache(maxsize=1)
def _extract_nl_stat_var() -> list[dict[str, str]]:
    output_df = pd.read_csv(_NL_STAT_VAR_FILE)
    seen = set()
    records = []
    for _, row in output_df.iterrows():
        dcid_str = row.get("dcid")
        sentence = row.get("sentence")
        if pd.notna(dcid_str) and pd.notna(sentence):
            dcid_str = str(dcid_str)
            sentence = str(sentence).strip()
            for item in dcid_str.split(";"):
                item = item.strip()
                if item and sentence:
                    pair = (item, sentence)
                    if pair not in seen:
                        seen.add(pair)
                        records.append({"dcid": item, "sentence": sentence})
    return records


def _fresh_data_condition(timestamp: Optional[str], predicate_types_list_sql: str) -> tuple[str, str]:
    """Helper function to construct the timelock SQL condition string.

    If timestamp is None/NULL, returns "TRUE".
    Otherwise, returns the timelock condition string using predicate_types_list_sql.
    """
    if timestamp is None:
        return "TRUE", "TRUE"

    update_node_cond = f"n.last_update_timestamp > TIMESTAMP('{timestamp}')"
    update_property_cond = f"LOGICAL_OR(o.last_update_timestamp > TIMESTAMP('{timestamp}'))"
    return update_node_cond, update_property_cond


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
            spanner_client = spanner.Client(project=self.executor.spanner_project_id)
            instance = spanner_client.instance(self.executor.instance_id)
            self._spanner_database = instance.database(self.executor.database_id)
            logging.info(f"Initialized Spanner client for EmbeddingGenerator: {self._spanner_database.name}")
        return self._spanner_database

    def _get_latest_lock_timestamp(self) -> Optional[str]:
        """Fetches the latest lock timestamp from IngestionLock table as a formatted string."""
        try:
            db = self.spanner_database
            lock_sql = "SELECT MAX(AcquiredTimestamp) FROM IngestionLock"
            latest_lock_timestamp = None
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(lock_sql)
                for row in results:
                    latest_lock_timestamp = row[0]
            if latest_lock_timestamp is not None:
                val_str = latest_lock_timestamp.isoformat().replace("+00:00", "Z") if hasattr(latest_lock_timestamp, "isoformat") else str(latest_lock_timestamp)
                return val_str
            return None
        except Exception as e:
            logging.error(f"Failed to fetch latest lock timestamp from Spanner: {e}")
            return None

    def _delete_existing_embeddings(self, spec: EmbeddingSpec, latest_lock_timestamp: Optional[str] = None, embedding_table: str = "NodeEmbedding") -> int:
        """Deletes existing embeddings in Spanner for nodes matching the spec before re-generation."""
        try:
            db = self.spanner_database
            params = {}
            param_types = {}

            if spec.node_filter_type == "NLStatisticalVariable":
                nl_records = _extract_nl_stat_var()
                dcids = sorted(list({r["dcid"] for r in nl_records}))
                params["nl_stat_vars"] = dcids
                param_types["nl_stat_vars"] = spanner.param_types.Array(spanner.param_types.STRING)
                filter_condition = "n.subject_id IN UNNEST(@nl_stat_vars)"
            else:
                filter_condition = "TRUE"

            match_clauses = []
            for node_type, predicate_types in spec.node_types.items():
                safe_predicate_types = [f"'{pt.replace(chr(39), chr(92) + chr(39))}'" for pt in predicate_types]
                predicate_types_list_sql = f"[{', '.join(safe_predicate_types)}]"
                update_node_cond, update_property_cond = _fresh_data_condition(latest_lock_timestamp, predicate_types_list_sql)
                graph_query = f"""
                    MATCH
                    (n:Node WHERE "{node_type}" IN UNNEST(n.types) AND {filter_condition})
                    OPTIONAL MATCH
                    (n)-[e: Edge
                        WHERE e.predicate IN UNNEST({predicate_types_list_sql})]->
                    (o:Node
                        WHERE o.value IS NOT NULL
                        AND o.value <> "")
                    WITH
                        n, {update_property_cond} AS update_property_data
                    GROUP BY n
                    RETURN
                    n.subject_id AS subject_id,
                    {update_node_cond} AS update_node_data,
                    IFNULL(update_property_data, FALSE) AS update_property_data
                """
                match_clauses.append(graph_query)

            inner_gql = "\nUNION DISTINCT\n".join(match_clauses)
            node_select_sql = f"""
                SELECT
                    subject_id
                FROM GRAPH_TABLE(DCGraph
                    {inner_gql}
                )
                WHERE update_node_data OR update_property_data
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

    @staticmethod
    def _generate_spanner_query(nodes: Any, latest_lock_timestamp: Optional[str] = None, filter_condition: str = "TRUE") -> str:
        """Generates the Spanner GQL query for extracting node data and JSON embedding_content."""
        if hasattr(nodes, 'node_types'):
            spec = nodes
            nodes = spec.node_types
            if getattr(spec, 'node_filter_type', None) == "NLStatisticalVariable":
                nl_records = _extract_nl_stat_var()
                dcids = sorted(list({r["dcid"] for r in nl_records}))
                quoted_dcids = ", ".join([f"'{d}'" for d in dcids])
                filter_condition = f"n.subject_id IN ({quoted_dcids})"

        match_clauses = []
        for node_type, predicate_types in nodes.items():
            safe_predicate_types = [f"'{pt.replace(chr(39), chr(92) + chr(39))}'" for pt in predicate_types]
            predicate_types_list_sql = f"[{', '.join(safe_predicate_types)}]"
            update_node_cond, update_property_cond = _fresh_data_condition(latest_lock_timestamp, predicate_types_list_sql)
            spanner_query_template = f"""    MATCH
    (n:Node WHERE "{node_type}" IN UNNEST(n.types) AND {filter_condition})
    OPTIONAL MATCH
    (n)-[e: Edge
        WHERE e.predicate IN UNNEST({predicate_types_list_sql})]->
    (o:Node
        WHERE o.value IS NOT NULL
        AND o.value <> "")
    WITH
        n,
        e.predicate AS pred,
        STRING_AGG(o.value, ". ") AS values,
        {update_property_cond} AS update_property_data
    GROUP BY n, pred
    RETURN
    n.subject_id AS subject_id,
    n.types AS node_types,
    {update_node_cond} AS update_node_data,
    CASE 
        WHEN COUNT(pred) > 0 THEN
        JSON_OBJECT(
            "subject_id", n.subject_id,
            "name", n.name,
            "properties", JSON_OBJECT(
            ARRAY_AGG(pred IGNORE NULLS),
            ARRAY_AGG(TO_JSON(values) IGNORE NULLS)
            )
        )
        ELSE
        JSON_OBJECT(
            "subject_id", n.subject_id,
            "name", n.name
        )
    END AS embedding_content,
    CASE 
        WHEN COUNT(pred) > 0 THEN
            LOGICAL_OR(update_property_data)
        ELSE
            FALSE
    END AS update_property_data
    GROUP BY n"""
            match_clauses.append(spanner_query_template)

        inner_gql = "\nUNION ALL\n".join(match_clauses)
        return f"""
SELECT
    subject_id,
    node_types,
    embedding_content
FROM GRAPH_TABLE(DCGraph
{inner_gql}
)
WHERE update_node_data OR update_property_data"""

    def _stream_spanner_to_bq(self, spanner_query: str, raw_nodes_table_id: str, batch_size: int = 5000) -> None:
        """Streams Spanner query results in batches into a BigQuery table."""
        bq_schema = [
            bigquery.SchemaField("subject_id", "STRING"),
            bigquery.SchemaField("node_types", "STRING", mode="REPEATED"),
            bigquery.SchemaField("embedding_content", "JSON"),
        ]
        db = self.spanner_database
        bq_client = self.executor.client

        total_rows = 0
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(spanner_query)
            batch = []
            first_batch = True
            for row in results:
                subj_id = row[0]
                types_list = row[1] if isinstance(row[1], list) else list(row[1]) if row[1] else []
                emb_content = row[2]
                if isinstance(emb_content, str):
                    try:
                        emb_content = json.loads(emb_content)
                    except Exception:
                        pass

                batch.append({
                    "subject_id": subj_id,
                    "node_types": types_list,
                    "embedding_content": emb_content
                })
                total_rows += 1

                if len(batch) >= batch_size:
                    write_disp = "WRITE_TRUNCATE" if first_batch else "WRITE_APPEND"
                    load_config = bigquery.LoadJobConfig(schema=bq_schema, write_disposition=write_disp)
                    load_job = bq_client.load_table_from_json(batch, raw_nodes_table_id, job_config=load_config)
                    load_job.result()
                    logging.info(f"Streamed {total_rows} rows from Spanner to BigQuery ({raw_nodes_table_id})...")
                    first_batch = False
                    batch = []

            if batch or first_batch:
                write_disp = "WRITE_TRUNCATE" if first_batch else "WRITE_APPEND"
                load_config = bigquery.LoadJobConfig(schema=bq_schema, write_disposition=write_disp)
                load_job = bq_client.load_table_from_json(batch, raw_nodes_table_id, job_config=load_config)
                load_job.result()

        logging.info(f"Successfully finished streaming to BigQuery table {raw_nodes_table_id}. Total ingested: {total_rows} rows.")

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
        model_project_id = self.executor.spanner_project_id
        bq_dataset_id = self.executor.bq_dataset_id
        location = self.executor.location

        embedding_label = spec.embedding_label
        model_name = spec.model_name
        model_endpoint = spec.model_endpoint
        task_type = spec.task_type
        node_types = spec.node_types
        node_filter_type = spec.node_filter_type

        latest_lock_timestamp = self._get_latest_lock_timestamp()

        # 1. Pre-delete existing embeddings in Spanner for updated nodes
        self._delete_existing_embeddings(spec, latest_lock_timestamp=latest_lock_timestamp, embedding_table=embedding_table)

        # 2. Execute GQL query directly in Spanner and stream results in batches to a BigQuery table
        raw_nodes_table_id = f"{project_id}.{bq_dataset_id}.temp_raw_nodes_{embedding_label}"
        spanner_query = self._generate_spanner_query(node_types, latest_lock_timestamp=latest_lock_timestamp)
        logging.info(f"Querying Spanner directly for '{embedding_label}' nodes and streaming to BigQuery...")
        self._stream_spanner_to_bq(spanner_query, raw_nodes_table_id)

        # Update select_nodes_sql to query the streamed BigQuery raw_nodes table
        job_config = None
        if node_filter_type == "NoFilter":
            select_nodes_sql = f"""
                SELECT 
                  subject_id, 
                  CAST(FARM_FINGERPRINT(TO_JSON_STRING(embedding_content)) AS STRING) AS embedding_content_key,
                  TO_JSON_STRING(embedding_content) AS content, 
                  embedding_content, 
                  node_types 
                FROM `{raw_nodes_table_id}`
            """
        elif node_filter_type == "NLStatisticalVariable":
            select_nodes_sql = f"""
                SELECT 
                  r.subject_id, 
                  CAST(FARM_FINGERPRINT(m.sentence) AS STRING) AS embedding_content_key,
                  m.sentence AS content, 
                  JSON_OBJECT("title", r.subject_id, "sentence", m.sentence) AS embedding_content, 
                  r.node_types 
                FROM UNNEST(@nl_stat_vars) m
                INNER JOIN `{raw_nodes_table_id}` r ON r.subject_id = m.dcid
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

        query = f"""
        -- 1. Generate embeddings natively in BigQuery
        CREATE TEMP TABLE embedding_staging AS
        SELECT 
          subject_id, 
          "{embedding_label}" AS embedding_label, 
          embedding_content_key,
          embedding_content, 
          node_types, 
          ml_generate_embedding_result AS embeddings
        FROM ML.GENERATE_EMBEDDING(
          MODEL `{model_project_id}.{bq_dataset_id}.{model_name}`,
          ({select_nodes_sql}),
          STRUCT("{task_type}" AS task_type)
        );

        -- 2. Export back to Spanner
        EXPORT DATA OPTIONS(
          uri="{dest}",
          format="CLOUD_SPANNER",
          spanner_options='{{"table": "{embedding_table}"}}'
        ) AS
        SELECT * FROM embedding_staging;
        """
        logging.info(f"Submitting embedding generation job for {embedding_label}...")
        job = self.executor.execute(query, job_config=job_config)

        if job:
            try:
                job.result()
            except Exception:
                pass
            try:
                self.executor.client.delete_table(raw_nodes_table_id, not_found_ok=True)
            except Exception as e:
                logging.warning(f"Failed to delete temp table {raw_nodes_table_id}: {e}")

        return job
