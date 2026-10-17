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
from datetime import datetime
from functools import lru_cache
import itertools
import json
import logging
from typing import Any, Dict, List, Optional, Union
import pandas as pd

from google.cloud import bigquery
from google.cloud import spanner
from pydantic import BaseModel
from .bq_executor import BigQueryExecutor
from .spanner_query import EMBEDDING_CONTENT_QUERY_BY_NODE_TYPE


@dataclass
class EmbeddingGenerationConfig:
    """Configuration for embedding generation."""
    specs: Optional[List[Any]] = None
    embedding_table: str = "NodeEmbedding"


_NL_STAT_VAR_FILE = "gs://datcom-nl-models/base_uae_mem_2025_11_03_07_10_42/embeddings.csv"

_PLACE_TYPE_ORDER = [
    "Place",
    "OceanicBasin",
    "Continent",
    "Country",
    "CensusRegion",
    "CensusDivision",
    "State",
    "AdministrativeArea1",
    "County",
    "AdministrativeArea2",
    "CensusCountyDivision",
    "EurostatNUTS1",
    "EurostatNUTS2",
    "CongressionalDistrict",
    "UDISEDistrict",
    "CensusCoreBasedStatisticalArea",
    "EurostatNUTS3",
    "SuperfundSite",
    "Glacier",
    "AdministrativeArea3",
    "AdministrativeArea4",
    "PublicUtility",
    "CollegeOrUniversity",
    "EpaParentCompany",
    "UDISEBlock",
    "AdministrativeArea5",
    "EpaReportingFacility",
    "SchoolDistrict",
    "CensusZipCodeTabulationArea",
    "PrivateSchool",
    "CensusTract",
    "City",
    "AirQualitySite",
    "PublicSchool",
    "Neighborhood",
    "CensusBlockGroup",
    "AdministrativeArea",
    "Village",
]


class EmbeddingSpec(BaseModel):
    """Specification for generating and indexing embeddings for graph nodes.

    Attributes:
        embedding_label: Identifier key for the embedding dataset (e.g. 'base_text_embedding').
        model_name: Name of the BigQuery ML model endpoint used for vector generation.
        model_endpoint: Vertex AI model endpoint (e.g. 'text-embedding-005').
        task_type: Embedding task type passed to ML.GENERATE_EMBEDDING (e.g. 'RETRIEVAL_QUERY').
        node_types: Maps each node type (e.g. 'StatisticalVariable', 'Topic') to the list of
            predicate names (e.g. ['description']) whose connected object values will be embedded.
        node_filter_type: Node filtering strategy ('NoFilter', 'NLStatisticalVariable', or 'EntityTypes').
    """
    embedding_label: str
    model_name: str
    model_endpoint: str = "text-embedding-005"
    task_type: str
    # Maps each node type to the list of predicate names to be read and embedded.
    node_types: Dict[str, List[str]]
    node_filter_type: str


_DEFAULT_EMBEDDING_SPECS = [
    EmbeddingSpec(
        embedding_label="base_text_embedding",
        model_name="NodeEmbeddingModel",
        model_endpoint="text-embedding-005",
        task_type="RETRIEVAL_QUERY",
        node_types={
            "StatisticalVariable": [],
            "Topic": []
        },
        node_filter_type="NoFilter"
    )
]


def _recording_nl_dcid_sentence_pair(
    dcid_str: str, sentence: str, seen: set, records: list[dict[str, str]]
) -> None:
    """Parses semicolon-separated dcids and records new (dcid, sentence) pairs."""
    for item in str(dcid_str).split(";"):
        dcid = item.strip()
        if not dcid or (dcid, sentence) in seen:
            continue
        seen.add((dcid, sentence))
        records.append({"dcid": dcid, "sentence": sentence})


@lru_cache(maxsize=1)
def _extract_nl_stat_var() -> list[dict[str, str]]:
    """Extracts deduplicated (dcid, sentence) pairs from NL stat var CSV file."""
    output_df = pd.read_csv(_NL_STAT_VAR_FILE).dropna(subset=["dcid", "sentence"])
    seen = set()
    records = []
    for _, row in output_df.iterrows():
        sentence = str(row["sentence"]).strip()
        if not sentence:
            continue
        _recording_nl_dcid_sentence_pair(row["dcid"], sentence, seen, records)
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
            spanner_client = spanner.Client(project=self.executor.spanner_project_id)
            instance = spanner_client.instance(self.executor.instance_id)
            self._spanner_database = instance.database(self.executor.database_id)
            logging.info(f"Initialized Spanner client for EmbeddingGenerator: {self._spanner_database.name}")
        return self._spanner_database

    def _get_observation_entity_types(self) -> List[str]:
        """Finds distinct node types (Class nodes) that have at least one observation entity
        in TimeSeries, using an index-backed short-circuit EXISTS check, and merges with _PLACE_TYPE_ORDER."""
        db = self.spanner_database
        sql = """
            WITH all_type_table AS (
                SELECT DISTINCT
                    subject_id AS t
                FROM
                    Edge@{FORCE_INDEX=InEdge}
                WHERE
                    object_id = 'Class'
                    AND predicate = 'typeOf'
            )
            SELECT
                c.t
            FROM
                all_type_table c
            INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
                UNNEST(
                    ARRAY(
                        SELECT
                            1
                        FROM
                            Edge@{FORCE_INDEX=InEdge} e
                        INNER JOIN@{JOIN_METHOD=APPLY_JOIN}
                            TimeSeries@{FORCE_INDEX=TimeSeriesByEntity1} ts
                            ON e.subject_id = ts.entity1
                        WHERE
                            e.object_id = c.t
                            AND e.predicate = 'typeOf'
                        LIMIT 1
                    )
                )
            ORDER BY
                c.t
        """
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(sql)
            spanner_types = [row[0] for row in results]
        return sorted(set(spanner_types) | set(_PLACE_TYPE_ORDER))

    def _get_latest_lock_timestamp(self) -> Optional[datetime]:
        """Gets the latest AcquiredTimestamp from IngestionLock table.

        Returns:
            The latest AcquiredTimestamp as a datetime, or None if no entries exist.
        """
        lock_sql = "SELECT MAX(AcquiredTimestamp) FROM IngestionLock"
        try:
            db = self.spanner_database
            latest_lock_timestamp = None
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(lock_sql)
                for row in results:
                    latest_lock_timestamp = row[0]
            return latest_lock_timestamp
        except Exception as e:
            logging.error(f"Failed to fetch latest lock timestamp from Spanner: {e}")
            return None

    def _get_node_filter_condition(
        self,
        node_filter_type: str,
        params: Dict[str, Any],
        param_types: Dict[str, Any],
    ) -> str:
        """Builds the GQL filter condition and populates Spanner query parameters."""
        if node_filter_type == "NoFilter":
            return "TRUE"
        elif node_filter_type == "NLStatisticalVariable":
            nl_records = _extract_nl_stat_var()
            dcids = sorted(list({r["dcid"] for r in nl_records}))
            params["nl_stat_vars"] = dcids
            param_types["nl_stat_vars"] = spanner.param_types.Array(spanner.param_types.STRING)
            return "n.subject_id IN UNNEST(@nl_stat_vars)"
        elif node_filter_type == "EntityTypes":
            dcids = self._get_observation_entity_types()
            params["entity_types"] = dcids
            param_types["entity_types"] = spanner.param_types.Array(spanner.param_types.STRING)
            return "n.subject_id IN UNNEST(@entity_types)"
        else:
            logging.error(f"Unknown node filter type: {node_filter_type}")
            raise ValueError(f"Unknown node filter type: {node_filter_type}")

    def _delete_existing_embeddings(
        self,
        spec: EmbeddingSpec,
        latest_lock_timestamp: Optional[Union[datetime, str]] = None,
        embedding_table: str = "NodeEmbedding",
    ) -> int:
        """Deletes existing embeddings in Spanner for nodes matching the spec before re-generation."""
        try:
            db = self.spanner_database
            params: Dict[str, Any] = {"timestamp": latest_lock_timestamp}
            param_types: Dict[str, Any] = {"timestamp": spanner.param_types.TIMESTAMP}

            filter_condition = self._get_node_filter_condition(
                spec.node_filter_type, params, param_types
            )
            node_select_sql = self._generate_spanner_query(
                spec.node_types, filter_condition
            )

            subject_ids = []
            with db.snapshot() as snapshot:
                results = snapshot.execute_sql(
                    node_select_sql, params=params, param_types=param_types
                )
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
    def _generate_spanner_query(
        nodes: Dict[str, List[str]], filter_condition: str = "TRUE"
    ) -> str:
        """Generates the Spanner GQL statement to perform a graph query that reads all related predicates and constructs JSON content to be embedded.

        Args:
            nodes: Mapping of node types to the list of predicate names to read and embed.
            filter_condition: Additional SQL/GQL condition to filter nodes (e.g. 'TRUE' or node ID filter).

        Returns:
            The generated Spanner GQL query string.
        """
        list_of_graph_traversal_statements = []
        for node_type, predicate_types in nodes.items():
            # Escape single quotes and wrap each predicate string in single quotes to safely construct
            # an inlined GQL array literal (e.g. ['description', 'name']).
            safe_predicate_types = [
                f"'{pt.replace(chr(39), chr(92) + chr(39))}'"
                for pt in predicate_types
            ]
            predicate_types_list_sql = f"[{', '.join(safe_predicate_types)}]"
            graph_traversal_statement = EMBEDDING_CONTENT_QUERY_BY_NODE_TYPE.format(
                node_type=node_type,
                filter_condition=filter_condition,
                predicate_types_list_sql=predicate_types_list_sql,
            )
            list_of_graph_traversal_statements.append(graph_traversal_statement)

        unioned_graph_statement_over_type = "\nUNION ALL\n".join(
            list_of_graph_traversal_statements
        )
        return f"""
Graph DCGraph
{unioned_graph_statement_over_type}
"""

    def _stream_spanner_to_bq(
        self,
        spanner_query: str,
        raw_nodes_table_id: str,
        latest_lock_timestamp: Optional[Union[datetime, str]] = None,
        batch_size: int = 5000,
    ) -> None:
        """Streams Spanner query results in batches into a BigQuery table."""
        bq_schema = [
            bigquery.SchemaField("subject_id", "STRING"),
            bigquery.SchemaField("node_types", "STRING", mode="REPEATED"),
            bigquery.SchemaField("embedding_content", "JSON"),
        ]
        db = self.spanner_database
        bq_client = self.executor.client
        params = {"timestamp": latest_lock_timestamp}
        param_types = {"timestamp": spanner.param_types.TIMESTAMP}

        total_rows = 0
        with db.snapshot() as snapshot:
            results = snapshot.execute_sql(
                spanner_query, params=params, param_types=param_types
            )
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

            if batch:
                write_disp = "WRITE_TRUNCATE" if first_batch else "WRITE_APPEND"
                load_config = bigquery.LoadJobConfig(schema=bq_schema, write_disposition=write_disp)
                load_job = bq_client.load_table_from_json(batch, raw_nodes_table_id, job_config=load_config)
                load_job.result()
            elif first_batch:
                bq_client.delete_table(raw_nodes_table_id, not_found_ok=True)
                table = bigquery.Table(raw_nodes_table_id, schema=bq_schema)
                bq_client.create_table(table)

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

        if node_filter_type not in ("NoFilter", "NLStatisticalVariable", "EntityTypes"):
            logging.error(f"Unknown node filter type: {node_filter_type}")
            return None

        latest_lock_timestamp = self._get_latest_lock_timestamp()

        # 1. Pre-delete existing embeddings in Spanner for updated nodes
        self._delete_existing_embeddings(
            spec,
            latest_lock_timestamp=latest_lock_timestamp,
            embedding_table=embedding_table,
        )

        # 2. Execute GQL query directly in Spanner and stream results in batches to a BigQuery table
        raw_nodes_table_id = f"{project_id}.{bq_dataset_id}.temp_raw_nodes_{embedding_label}"
        spanner_query = self._generate_spanner_query(node_types)
        logging.info(f"Querying Spanner directly for '{embedding_label}' nodes and streaming to BigQuery...")
        self._stream_spanner_to_bq(
            spanner_query,
            raw_nodes_table_id,
            latest_lock_timestamp=latest_lock_timestamp,
        )

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
        elif node_filter_type == "EntityTypes":
            select_nodes_sql = f"""
                SELECT 
                  subject_id, 
                  CAST(FARM_FINGERPRINT(TO_JSON_STRING(embedding_content)) AS STRING) AS embedding_content_key,
                  TO_JSON_STRING(embedding_content) AS content, 
                  embedding_content, 
                  node_types 
                FROM `{raw_nodes_table_id}`
                WHERE subject_id IN UNNEST(@entity_types)
            """
            entity_types = self._get_observation_entity_types()
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ArrayQueryParameter(
                        "entity_types",
                        "STRING",
                        entity_types
                    )
                ]
            )

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
          spanner_options='{{"table": "{embedding_table}", "priority": "LOW"}}'
        ) AS
        SELECT * FROM embedding_staging;
        """
        logging.info(f"Submitting embedding generation job for {embedding_label}...")
        job = self.executor.execute(query, job_config=job_config)

        if job:
            try:
                job.result()
            finally:
                self.executor.client.delete_table(raw_nodes_table_id, not_found_ok=True)

        return job
