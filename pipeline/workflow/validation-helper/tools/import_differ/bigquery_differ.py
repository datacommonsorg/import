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
"""BigQuery-based differ for low-memory execution."""

import os
import re
import sys
from typing import Dict, Optional
import uuid

from absl import logging
from google.cloud import bigquery

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
sys.path.append(_SCRIPT_DIR)
sys.path.append(os.path.join(_DATA_DIR, 'util'))

import bq_util
import differ_utils

# Re-export symbols for backward compatibility and tests
OBSERVATION_KEY_PROPERTIES = bq_util.OBSERVATION_KEY_PROPERTIES
stream_mcf_to_csv = bq_util.stream_mcf_to_csv


def _sanitize_job_suffix(job_name: str) -> str:
    """Converts a job name into a safe BigQuery table suffix."""
    cleaned = re.sub(r'[^a-zA-Z0-9_]', '_', job_name)
    return f'{cleaned}_{uuid.uuid4().hex[:8]}'


def run_bigquery_differ(
    current_data: str,
    previous_data: str,
    output_location: str,
    project_id: str,
    job_name: str = 'differ',
    dataset_id: str = 'datcom_import_differ',
    gcs_temp_dir: Optional[str] = None,
) -> Dict:
    """Executes dataset diff using streaming MCF-to-CSV and BigQuery FULL OUTER JOIN.

    Keeps local memory strictly bounded (< 100 MB RAM) regardless of dataset size.
    Writes differ_summary.json and differ_summary.csv to output_location.
    """
    if not project_id:
        project_id = os.environ.get('GOOGLE_CLOUD_PROJECT') or os.environ.get(
            'PROJECT_ID', '')
    bq_util.validate_bq_identifier(project_id)
    bq_util.validate_bq_identifier(dataset_id)

    bq_client = bigquery.Client(project=project_id)
    dataset_ref = f'{project_id}.{dataset_id}'
    bq_util.ensure_bq_dataset(bq_client, dataset_ref)

    suffix = _sanitize_job_suffix(job_name)
    curr_obs_table = f'{dataset_ref}.curr_obs_{suffix}'
    prev_obs_table = f'{dataset_ref}.prev_obs_{suffix}'
    curr_schema_table = f'{dataset_ref}.curr_schema_{suffix}'
    prev_schema_table = f'{dataset_ref}.prev_schema_{suffix}'

    try:
        logging.info('Step 1/3: Loading current MCF data into BigQuery...')
        curr_obs_count, curr_schema_count = bq_util.load_mcf_to_bq_tables(
            bq_client,
            current_data,
            curr_obs_table,
            curr_schema_table,
            temp_dir=gcs_temp_dir,
            suffix=f'curr_{suffix}')

        logging.info('Step 2/3: Loading previous MCF data into BigQuery...')
        prev_obs_count, prev_schema_count = bq_util.load_mcf_to_bq_tables(
            bq_client,
            previous_data,
            prev_obs_table,
            prev_schema_table,
            temp_dir=gcs_temp_dir,
            suffix=f'prev_{suffix}')

        logging.info('Step 3/3: Running BigQuery FULL OUTER JOIN diff...')
        obs_diff_sql = f"""
        WITH diff AS (
          SELECT
            COALESCE(c.variableMeasured, p.variableMeasured) AS variableMeasured,
            CASE
              WHEN p.key_combined IS NULL THEN 'ADDED'
              WHEN c.key_combined IS NULL THEN 'DELETED'
              WHEN c.value != p.value THEN 'MODIFIED'
              ELSE 'UNMODIFIED'
            END AS diff_type
          FROM `{curr_obs_table}` c
          FULL OUTER JOIN `{prev_obs_table}` p
            ON c.key_combined = p.key_combined
          WHERE c.value IS DISTINCT FROM p.value
        )
        SELECT
          REGEXP_REPLACE(variableMeasured, '^dcid:', '') AS StatVar,
          COUNTIF(diff_type = 'ADDED') AS ADDED,
          COUNTIF(diff_type = 'DELETED') AS DELETED,
          COUNTIF(diff_type = 'MODIFIED') AS MODIFIED
        FROM diff
        GROUP BY StatVar
        ORDER BY StatVar
        """
        obs_diff_df = bq_client.query(obs_diff_sql).to_dataframe()

        schema_diff_sql = f"""
        WITH diff AS (
          SELECT
            CASE
              WHEN p.dcid IS NULL THEN 'ADDED'
              WHEN c.dcid IS NULL THEN 'DELETED'
              WHEN c.value_combined != p.value_combined THEN 'MODIFIED'
              ELSE 'UNMODIFIED'
            END AS diff_type
          FROM `{curr_schema_table}` c
          FULL OUTER JOIN `{prev_schema_table}` p
            ON c.dcid = p.dcid
          WHERE c.value_combined IS DISTINCT FROM p.value_combined
        )
        SELECT
          COUNTIF(diff_type = 'ADDED') AS added_schema_count,
          COUNTIF(diff_type = 'DELETED') AS deleted_schema_count,
          COUNTIF(diff_type = 'MODIFIED') AS modified_schema_count
        FROM diff
        """
        schema_rows = list(bq_client.query(schema_diff_sql).result())
        if schema_rows:
            added_schema = int(schema_rows[0].added_schema_count or 0)
            deleted_schema = int(schema_rows[0].deleted_schema_count or 0)
            modified_schema = int(schema_rows[0].modified_schema_count or 0)
        else:
            added_schema = deleted_schema = modified_schema = 0

    finally:
        # Always clean up temporary BigQuery tables immediately
        for table_id in (curr_obs_table, prev_obs_table, curr_schema_table,
                         prev_schema_table):
            bq_client.delete_table(table_id, not_found_ok=True)

    added_obs = int(obs_diff_df['ADDED'].sum()) if not obs_diff_df.empty else 0
    deleted_obs = int(
        obs_diff_df['DELETED'].sum()) if not obs_diff_df.empty else 0
    modified_obs = int(
        obs_diff_df['MODIFIED'].sum()) if not obs_diff_df.empty else 0
    obs_diff_total = added_obs + deleted_obs + modified_obs
    schema_diff_total = added_schema + deleted_schema + modified_schema

    differ_summary = {
        'current_version': current_data,
        'previous_version': previous_data,
        'current_obs_count': curr_obs_count,
        'previous_obs_count': prev_obs_count,
        'current_schema_count': curr_schema_count,
        'previous_schema_count': prev_schema_count,
        'added_obs_count': added_obs,
        'deleted_obs_count': deleted_obs,
        'modified_obs_count': modified_obs,
        'added_schema_count': added_schema,
        'deleted_schema_count': deleted_schema,
        'modified_schema_count': modified_schema,
        'obs_diff_count': obs_diff_total,
        'schema_diff_count': schema_diff_total,
    }

    os.makedirs(output_location, exist_ok=True)
    differ_utils.write_json_data(differ_summary, output_location,
                                 'differ_summary.json')
    differ_utils.write_csv_data(obs_diff_df, output_location,
                                'differ_summary.csv')

    logging.info('BigQuery Differ summary: %s', differ_summary)
    return differ_summary
