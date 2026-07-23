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

"""Helper utilities for embedding workflows."""

from collections import OrderedDict
from functools import lru_cache
import itertools
import json
import logging
import time
from datetime import datetime
import pandas as pd
from google.cloud.spanner_v1.param_types import TIMESTAMP, STRING, Array, Struct, StructField, JSON
from clients.spanner import SpannerClient
import config


_BATCH_SIZE = 1000
_NL_STAT_VAR_FILE = f"gs://datcom-nl-models/base_uae_mem_2025_11_03_07_10_42/embeddings.csv"

@lru_cache(maxsize=1)
def _extract_nl_stat_var() -> list[dict[str, str]]:
    """Extracts deduplicated (dcid, sentence) pairs from NL stat var CSV file."""
    output_df = pd.read_csv(_NL_STAT_VAR_FILE)
    output_df = output_df.dropna(subset=['dcid', 'sentence'])
    output_df['dcid'] = output_df['dcid'].apply(lambda x: [item.strip() for item in str(x).split(';') if item.strip()])
    output_df['sentence'] = output_df['sentence'].astype(str).str.strip()
    
    exploded = output_df.explode('dcid')
    exploded = exploded[(exploded['dcid'] != '') & (exploded['sentence'] != '')]
    deduped = exploded[['dcid', 'sentence']].drop_duplicates()
    return deduped.to_dict(orient='records')


class EmbeddingUtils:
    """Orchestrates the embedding ingestion workflow."""

    def __init__(self, spanner: SpannerClient) -> None:
        self.spanner = spanner

    def _get_latest_lock_timestamp(self):
        """Gets the latest AcquiredTimestamp from IngestionLock table.

        Returns:
            The latest AcquiredTimestamp as a datetime object, or None if no entries exist.
        """
        time_lock_sql = "SELECT MAX(AcquiredTimestamp) FROM IngestionLock"
        try:
            with self.spanner.database.snapshot() as snapshot:
                results = snapshot.execute_sql(time_lock_sql)
                for row in results:
                    return row[0]
        except Exception as e:
            logging.error(f"Error fetching latest lock timestamp: {e}")
            raise
        return None

    def _get_node_filter_condition(self, node_filter_type, params, param_types):
        if node_filter_type == "NoFilter":
            return "TRUE"
        elif node_filter_type == "NLStatisticalVariable":
            nl_records = _extract_nl_stat_var()
            dcids = sorted(list({r["dcid"] for r in nl_records}))
            params["nl_stat_vars"] = dcids
            param_types["nl_stat_vars"] = Array(STRING)
            return "subject_id IN UNNEST(@nl_stat_vars)"
        else:
            logging.error(f"Unknown node filter type: {node_filter_type}")
            raise ValueError(f"Unknown node filter type: {node_filter_type}")

    def _get_updated_nodes(self, timestamp, node_types, node_filter_type, timeout):
        """Gets subject_ids and names from Node table where last_update_timestamp > timestamp.
        Yields results to avoid loading all into memory.

        Args:
            timestamp: datetime object to filter by.
            node_types: A list of strings representing the node types to filter by.
            node_filter_type: String specifying the node filtering logic.
            timeout: Timeout for the spanner client to execute queries.

        Yields:
            Dictionaries containing subject_id and name.
        """
        params = {"node_types": node_types}
        param_types = {"node_types": Array(STRING)}

        filter_condition = self._get_node_filter_condition(node_filter_type, params, param_types)
        timestamp_condition = "last_update_timestamp > @timestamp" if timestamp else "TRUE"

        updated_node_sql = f"""
            SELECT subject_id, name, types FROM Node 
            WHERE name IS NOT NULL
              AND {timestamp_condition}
              AND {filter_condition}
              AND EXISTS (
                SELECT 1 FROM UNNEST(types) AS t WHERE t IN UNNEST(@node_types)
              )
        """

        if timestamp:
            logging.info(f"Filtering valid nodes updated after {timestamp}")
            params["timestamp"] = timestamp
            param_types["timestamp"] = TIMESTAMP
        else:
            logging.info("No timestamp provided, reading all valid nodes.")

        try:
            with self.spanner.database.snapshot() as snapshot:
                results = snapshot.execute_sql(updated_node_sql, params=params, param_types=param_types, timeout=timeout)
                fields = None
                for row in results:
                    if fields is None:
                        fields = [field.name for field in results.fields]
                    yield dict(zip(fields, row))
        except Exception as e:
            logging.error(f"Error fetching updated nodes: {e}")
            raise

    def _filter_and_convert_nodes(self, nodes_generator, node_filter_type="NoFilter"):
        """Filters out nodes without a name and converts dictionaries to tuples.
        Reads from a generator and yields results.

        Args:
            nodes_generator: A generator yielding dictionaries containing subject_id, name, and types.
            node_filter_type: String specifying the node filtering logic.

        Yields:
            Tuples (subject_id, embedding_content, types).
        """
        if node_filter_type == "NLStatisticalVariable":
            nl_records = _extract_nl_stat_var()
            dcid_to_sentences = {}
            for r in nl_records:
                dcid_to_sentences.setdefault(r["dcid"], []).append(r["sentence"])

            for node in nodes_generator:
                subject_id = node.get("subject_id")
                sentences = dcid_to_sentences.get(subject_id, [])
                for sentence in sentences:
                    embedding_content = json.dumps(OrderedDict([
                        ("title", subject_id),
                        ("name", sentence)
                    ]))
                    yield (subject_id, embedding_content, node.get("types"))
        else:
            for node in nodes_generator:
                name = node.get("name")
                subject_id = node.get("subject_id")
                if name:
                    embedding_content = json.dumps(OrderedDict([
                        ("title", subject_id),
                        ("name", name)
                    ]))
                    yield (subject_id, embedding_content, node.get("types"))

    def _delete_existing_embeddings(self, embedding_table: str, embedding_label: str, subject_ids_iterable, timeout: int) -> int:
        """Deletes existing embeddings for subject_ids from a generator or iterable in batches.

        Args:
            embedding_table: Name of the embedding table.
            embedding_label: Embedding label key to delete.
            subject_ids_iterable: An iterable or generator yielding subject IDs.
            timeout: Timeout for the spanner client to execute queries.

        Returns:
            The number of affected rows.
        """
        global _BATCH_SIZE
        delete_sql = f"""
            DELETE FROM {embedding_table}
            WHERE embedding_label = @embedding_label
              AND subject_id IN UNNEST(@subject_ids)
        """
        total_deleted = 0

        def chunked(iterable, n):
            it = iter(iterable)
            while True:
                chunk = list(itertools.islice(it, n))
                if not chunk:
                    break
                yield chunk

        seen_subject_ids = set()
        def deduplicated_stream(iterable):
            for item in iterable:
                if item not in seen_subject_ids:
                    seen_subject_ids.add(item)
                    yield item

        for batch in chunked(deduplicated_stream(subject_ids_iterable), _BATCH_SIZE):
            params = {
                "embedding_label": embedding_label,
                "subject_ids": batch
            }
            param_types = {
                "embedding_label": STRING,
                "subject_ids": Array(STRING)
            }

            def _execute_dml(transaction):
                return transaction.execute_update(delete_sql, params=params, param_types=param_types, timeout=timeout)

            try:
                row_count = self.spanner.database.run_in_transaction(_execute_dml)
                total_deleted += row_count
            except Exception as e:
                logging.error(f"Error deleting existing embeddings for batch: {e}")
                raise

        logging.info(f"Deleted {total_deleted} existing embedding rows (embedding_label: {embedding_label}).")
        return total_deleted

    def _generate_embeddings_partitioned(self, nodes_generator, model_name, embedding_table, embedding_label, task_type, timeout):
        """Generates embeddings in batches using standard transactions.
        Processes nodes in chunks of 500 to avoid transaction size limits.
        Accepts a generator or list to avoid loading all nodes into memory.

        Args:
            nodes_generator: An iterable yielding tuples containing (subject_id, embedding_content, types).
            model_name: Name of the remote model defined in Spanner DDL.
            embedding_table: Name of the embedding table.
            embedding_label: Embedding label key (e.g. model ID) to insert.
            task_type: Task type parameter for ML.PREDICT (e.g. "RETRIEVAL_QUERY").
            timeout: Timeout for the spanner client to execute queries.

        Returns:
            The number of affected rows.
        """
        global _BATCH_SIZE
        total_rows_affected = 0

        logging.info(f"Generating embeddings in batches of {_BATCH_SIZE}.")

        embeddings_sql = f"""
            INSERT OR UPDATE INTO {embedding_table} (subject_id, embedding_label, embedding_content_key, embedding_content, embeddings, node_types)
            SELECT subject_id, @embedding_label, CAST(FARM_FINGERPRINT(JSON_VALUE(embedding_content, '$.name')) AS STRING), embedding_content, embeddings.values, node_types
            FROM ML.PREDICT(
                MODEL {model_name},
                (SELECT subject_id, TO_JSON_STRING(embedding_content) AS content, embedding_content, node_types, @task_type AS task_type FROM UNNEST(@nodes))
            )
        """

        struct_type = Struct([
            StructField("subject_id", STRING),
            StructField("embedding_content", JSON),
            StructField("node_types", Array(STRING))
        ])

        def chunked(iterable, n):
            it = iter(iterable)
            while True:
                chunk = list(itertools.islice(it, n))
                if not chunk:
                    break
                yield chunk

        for batch in chunked(nodes_generator, _BATCH_SIZE):
            params = {
                "nodes": batch,
                "embedding_label": embedding_label,
                "task_type": task_type
            }
            param_types = {
                "nodes": Array(struct_type),
                "embedding_label": STRING,
                "task_type": STRING
            }

            def _execute_dml(transaction):
                return transaction.execute_update(embeddings_sql, params=params, param_types=param_types, timeout=timeout)

            try:
                row_count = self.spanner.database.run_in_transaction(_execute_dml)
                total_rows_affected += row_count
                logging.info(f"Processed batch of {len(batch)} nodes. Affected total {total_rows_affected} rows.")
                time.sleep(0.5)
            except Exception as e:
                logging.error(f"Error executing batch transaction: {e}")
                raise

        logging.info(f"Completed batch processing. Total affected rows: {total_rows_affected}")
        return total_rows_affected

    def ingest_embeddings(self) -> int:
        """Generates and writes embeddings for all configured specs in the database.

        Returns:
            The total number of affected rows.
        """
        timestamp = self._get_latest_lock_timestamp()
        total_affected_rows = 0
        for spec in config.EMBEDDING_SPECS:
            node_types = spec.node_types
            model_name = spec.model_name
            embedding_label = spec.embedding_label
            task_type = spec.task_type
            node_filter_type = spec.node_filter_type

            logging.info(f"Job started for {embedding_label}. Streaming subject_ids for pre-deletion...")
            nodes_for_ids = self._get_updated_nodes(timestamp, node_types, node_filter_type, timeout=config.TIMEOUT)
            converted_stream = self._filter_and_convert_nodes(nodes_for_ids, node_filter_type=node_filter_type)
            subject_ids_generator = (item[0] for item in converted_stream)

            self._delete_existing_embeddings(
                embedding_table=self.spanner.embedding_table,
                embedding_label=embedding_label,
                subject_ids_iterable=subject_ids_generator,
                timeout=config.TIMEOUT
            )

            logging.info(f"Fetching and streaming nodes for types: {node_types}")
            nodes = self._get_updated_nodes(timestamp, node_types, node_filter_type, timeout=config.TIMEOUT)
            converted_nodes = self._filter_and_convert_nodes(nodes, node_filter_type=node_filter_type)

            logging.info(f"Generating embeddings for model {model_name} (embedding_label: {embedding_label})")
            affected_rows = self._generate_embeddings_partitioned(
                converted_nodes,
                model_name=model_name,
                embedding_table=self.spanner.embedding_table,
                embedding_label=embedding_label,
                task_type=task_type,
                timeout=config.TIMEOUT
            )
            total_affected_rows += affected_rows
        return total_affected_rows
