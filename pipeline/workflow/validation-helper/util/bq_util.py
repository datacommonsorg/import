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
"""Utilities for streaming MCF files and loading them into BigQuery tables."""

import csv
from datetime import datetime, timedelta, timezone
import os
import re
import sys
import tempfile
from typing import Dict, List, Optional, Tuple

from absl import logging
from google.cloud import bigquery

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(_SCRIPT_DIR)
sys.path.append(os.path.dirname(_SCRIPT_DIR))

from file_util import FileIO, file_get_matching
from mcf_file_util import normalize_value

OBSERVATION_KEY_PROPERTIES = [
    'variableMeasured',
    'observationAbout',
    'observationDate',
    'observationPeriod',
    'measurementMethod',
    'unit',
    'scalingFactor',
]

OBS_BQ_SCHEMA = [
    bigquery.SchemaField('key_combined', 'STRING'),
    bigquery.SchemaField('variableMeasured', 'STRING'),
    bigquery.SchemaField('value', 'STRING'),
]

SCHEMA_NODE_BQ_SCHEMA = [
    bigquery.SchemaField('dcid', 'STRING'),
    bigquery.SchemaField('value_combined', 'STRING'),
]

# Strict regex for validating BigQuery identifiers (project, dataset, table)
_BQ_IDENTIFIER_REGEX = re.compile(r'^[a-zA-Z0-9_-]+$')


def validate_bq_identifier(identifier: str) -> str:
    """Validates a BigQuery identifier to prevent SQL injection."""
    if not identifier or not _BQ_IDENTIFIER_REGEX.match(identifier):
        raise ValueError(f'Invalid BigQuery identifier: {identifier}')
    return identifier


def ensure_bq_dataset(bq_client: bigquery.Client,
                      dataset_ref: str,
                      expiration_hours: int = 12) -> None:
    """Creates the BigQuery dataset if it does not already exist."""
    try:
        bq_client.get_dataset(dataset_ref)
    except Exception:
        logging.info('Creating BigQuery dataset %s', dataset_ref)
        ds = bigquery.Dataset(dataset_ref)
        ds.default_table_expiration_ms = expiration_hours * 3600 * 1000
        bq_client.create_dataset(ds, exists_ok=True)


def _normalize_prop_val(value) -> str:
    """Normalizes a property value string identically to ImportDiffer."""
    if isinstance(value, list):
        return ', '.join(sorted([_normalize_prop_val(v) for v in value]))
    if isinstance(value, str):
        return str(normalize_value(value))
    return str(value) if value is not None else ''


def _flush_node_to_csv(node: Dict[str, str], obs_writer: csv.writer,
                       schema_writer: csv.writer) -> Tuple[int, int]:
    """Writes a single parsed MCF node to either the observation or schema CSV writer.

    Returns:
        Tuple of (obs_count_increment, schema_count_increment).
    """
    type_of = node.get('typeOf', '')
    if 'StatVarObservation' in type_of:
        key_parts = [
            _normalize_prop_val(node.get(prop, ''))
            for prop in OBSERVATION_KEY_PROPERTIES
        ]
        key_combined = ';'.join(key_parts)
        var_measured = _normalize_prop_val(node.get('variableMeasured', ''))
        val = _normalize_prop_val(node.get('value', ''))
        obs_writer.writerow([key_combined, var_measured, val])
        return 1, 0
    else:
        raw_id = node.get('dcid') or node.get('Node', '')
        dcid = _normalize_prop_val(raw_id)
        if dcid and not dcid.startswith('dcid:'):
            dcid = f'dcid:{dcid}'
        props = []
        for k in sorted(node.keys()):
            if k not in ('Node', 'dcid'):
                props.append(f'{k}:{_normalize_prop_val(node[k])}')
        value_combined = ';'.join(props)
        schema_writer.writerow([dcid, value_combined])
        return 0, 1


def stream_mcf_to_csv(mcf_pattern: str, obs_csv_path: str,
                      schema_csv_path: str) -> Tuple[int, int]:
    """Streams MCF files node-by-node into observation and schema CSVs in O(1) memory.

    Args:
        mcf_pattern: Path or wildcard pattern to MCF files (local or GCS).
        obs_csv_path: Destination CSV path for observations (local or GCS).
        schema_csv_path: Destination CSV path for schema nodes (local or GCS).

    Returns:
        Tuple of (total_obs_count, total_schema_count).
    """
    mcf_files = file_get_matching(mcf_pattern)
    total_obs = 0
    total_schema = 0

    with FileIO(obs_csv_path, mode='w', encoding='utf-8') as obs_file, \
         FileIO(schema_csv_path, mode='w', encoding='utf-8') as schema_file:
        obs_writer = csv.writer(obs_file)
        schema_writer = csv.writer(schema_file)
        obs_writer.writerow(['key_combined', 'variableMeasured', 'value'])
        schema_writer.writerow(['dcid', 'value_combined'])

        for mcf_file in mcf_files:
            logging.info('Streaming MCF file to CSV: %s', mcf_file)
            with FileIO(mcf_file, mode='r', encoding='utf-8') as in_f:
                current_node: Dict[str, str] = {}
                for raw_line in in_f:
                    line = raw_line.strip()
                    if not line or line.startswith('#'):
                        if current_node:
                            o_inc, s_inc = _flush_node_to_csv(
                                current_node, obs_writer, schema_writer)
                            total_obs += o_inc
                            total_schema += s_inc
                            current_node = {}
                        continue
                    if ':' in line:
                        k, v = line.split(':', 1)
                        k = k.strip()
                        v = v.strip()
                        if k == 'Node' and current_node:
                            # New Node block started without empty line separator
                            o_inc, s_inc = _flush_node_to_csv(
                                current_node, obs_writer, schema_writer)
                            total_obs += o_inc
                            total_schema += s_inc
                            current_node = {}
                        if k in current_node:
                            current_node[k] = f'{current_node[k]}, {v}'
                        else:
                            current_node[k] = v
                if current_node:
                    o_inc, s_inc = _flush_node_to_csv(current_node, obs_writer,
                                                      schema_writer)
                    total_obs += o_inc
                    total_schema += s_inc

    logging.info('Completed streaming MCF to CSV: %d obs, %d schema nodes',
                 total_obs, total_schema)
    return total_obs, total_schema


def load_csv_to_bq_table(bq_client: bigquery.Client,
                         csv_path: str,
                         table_ref: str,
                         schema: List[bigquery.SchemaField],
                         expiration_hours: int = 12) -> None:
    """Loads a local or GCS CSV file into a BigQuery table with a specified TTL."""
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        allow_quoted_newlines=True,
    )
    if csv_path.startswith('gs://'):
        load_job = bq_client.load_table_from_uri(csv_path,
                                                 table_ref,
                                                 job_config=job_config)
    else:
        with open(csv_path, 'rb') as f:
            load_job = bq_client.load_table_from_file(f,
                                                      table_ref,
                                                      job_config=job_config)
    load_job.result()

    table = bq_client.get_table(table_ref)
    table.expires = datetime.now(timezone.utc) + timedelta(
        hours=expiration_hours)
    bq_client.update_table(table, ['expires'])


def load_mcf_to_bq_tables(bq_client: bigquery.Client,
                          mcf_pattern: str,
                          obs_table_ref: str,
                          schema_table_ref: str,
                          temp_dir: Optional[str] = None,
                          suffix: str = '',
                          expiration_hours: int = 12) -> Tuple[int, int]:
    """Streams MCF files to CSVs and loads them into BigQuery observation and schema tables.

    Args:
        bq_client: BigQuery client instance.
        mcf_pattern: Path or wildcard pattern to MCF files (local or GCS).
        obs_table_ref: Destination BigQuery table ID for observations.
        schema_table_ref: Destination BigQuery table ID for schema nodes.
        temp_dir: Optional directory (local or GCS) for intermediate CSV files.
        suffix: Optional suffix for temporary CSV filenames.
        expiration_hours: Table expiration TTL in hours.

    Returns:
        Tuple of (obs_count, schema_count).
    """
    with tempfile.TemporaryDirectory() as local_tmpdir:
        base_dir = temp_dir if temp_dir else local_tmpdir
        tag = f'_{suffix}' if suffix else ''
        obs_csv = os.path.join(base_dir, f'obs{tag}.csv')
        schema_csv = os.path.join(base_dir, f'schema{tag}.csv')

        obs_count, schema_count = stream_mcf_to_csv(mcf_pattern, obs_csv,
                                                    schema_csv)
        load_csv_to_bq_table(bq_client, obs_csv, obs_table_ref, OBS_BQ_SCHEMA,
                             expiration_hours)
        load_csv_to_bq_table(bq_client, schema_csv, schema_table_ref,
                             SCHEMA_NODE_BQ_SCHEMA, expiration_hours)
        return obs_count, schema_count
