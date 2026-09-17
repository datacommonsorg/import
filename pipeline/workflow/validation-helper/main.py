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
"""Standalone Cloud Run Job entrypoint for Differ + Validation."""

import json
import os
import sys
import tempfile
import time
import urllib.request
from typing import Dict, List

from absl import app
from absl import flags
from absl import logging
from google.cloud import storage

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _APP_DIR)
sys.path.insert(0, os.path.join(_APP_DIR, 'util'))
sys.path.insert(0, os.path.join(_APP_DIR, 'tools', 'import_differ'))
sys.path.insert(0, os.path.join(_APP_DIR, 'tools', 'import_validation'))

from tools.import_differ import bigquery_differ
from tools.import_validation.runner import ValidationRunner
from util import file_util

FLAGS = flags.FLAGS
flags.DEFINE_string(
    'import_name', '',
    'Import name in format <relative_import_dir>:<import_name> (required).')
flags.DEFINE_string('import_config', '{}',
                    'JSON string of import executor config overrides.')
flags.DEFINE_string('version', '',
                    'Explicit candidate version override (optional).')
flags.DEFINE_string('gcs_bucket', '',
                    'GCS bucket name (defaults to env GCS_BUCKET_ID).')
if 'bq_dataset' not in FLAGS:
    flags.DEFINE_string(
        'bq_dataset', 'datcom_import_differ',
        'BigQuery dataset ID for temporary differ tables.')


def _read_gcs_text(client: storage.Client, bucket_name: str,
                   blob_path: str) -> str:
    """Reads text content from a GCS blob if it exists, else returns ''."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    if not blob.exists():
        return ''
    return blob.download_as_text().strip()


def _write_gcs_text(client: storage.Client, bucket_name: str, blob_path: str,
                    content: str) -> None:
    """Uploads text content to a GCS blob."""
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_string(content)


def _discover_input_prefixes(client: storage.Client, bucket_name: str,
                             output_dir: str, version: str) -> List[str]:
    """Discovers input0, input1, ... prefixes under gs://<bucket>/<output_dir>/<version>/."""
    prefix = f'{output_dir}/{version}/input'
    bucket = client.bucket(bucket_name)
    prefixes = set()
    for blob in client.list_blobs(bucket, prefix=prefix):
        rel = blob.name[len(f'{output_dir}/{version}/'):]
        top_folder = rel.split('/', 1)[0]
        if top_folder.startswith('input'):
            prefixes.add(top_folder)
    if not prefixes:
        return ['input0']
    return sorted(prefixes)


def _resolve_validation_config(client: storage.Client, bucket_name: str,
                               output_dir: str, relative_import_dir: str,
                               import_name: str, version: str,
                               default_val_config: str, tmpdir: str) -> str:
    """Resolves validation_config.json from GCS or GitHub manifest."""
    custom_cfg_gcs = (
        f'gs://{bucket_name}/{output_dir}/{version}/validation/validation_config.json'
    )
    if file_util.file_get_matching(custom_cfg_gcs):
        local_custom_cfg = os.path.join(tmpdir, 'validation_config.json')
        file_util.file_copy(custom_cfg_gcs, local_custom_cfg)
        return local_custom_cfg

    manifest_text = _read_gcs_text(client, bucket_name,
                                   f'{output_dir}/{version}/manifest.json')
    if not manifest_text:
        return default_val_config

    try:
        manifest = json.loads(manifest_text)
        custom_cfg_rel = manifest.get('validation_config_file')
        for spec in manifest.get('import_specifications', []):
            if spec.get('import_name') == import_name and spec.get(
                    'validation_config_file'):
                custom_cfg_rel = spec.get('validation_config_file')
                break
        if not custom_cfg_rel:
            return default_val_config

        github_raw_url = (
            f'https://raw.githubusercontent.com/datacommonsorg/data/master/{relative_import_dir}/{custom_cfg_rel}'
        )
        logging.info('Fetching custom validation config from %s',
                     github_raw_url)
        with urllib.request.urlopen(github_raw_url, timeout=30) as resp:
            custom_data = json.loads(resp.read().decode('utf-8'))

        with open(default_val_config, 'r', encoding='utf-8') as f:
            base_data = json.load(f)

        rules_map = {
            r['rule_id']: r
            for r in base_data.get('rules', [])
            if 'rule_id' in r
        }
        for r in custom_data.get('rules', []):
            if 'rule_id' in r:
                rules_map[r['rule_id']] = r
        merged = {'rules': list(rules_map.values())}
        local_merged = os.path.join(tmpdir, 'validation_config.json')
        with open(local_merged, 'w', encoding='utf-8') as f:
            json.dump(merged, f, indent=2)
        return local_merged
    except Exception as exc:
        logging.warning(
            'Failed to load custom validation config from manifest (%s); using default.',
            exc)
        return default_val_config


def run_validation_job(absolute_import_name: str, import_config_str: str,
                       version_override: str, bucket_name: str,
                       bq_dataset: str) -> int:
    """Executes BigQuery differ and ValidationRunner for an import version."""
    start_time = time.time()
    if ':' not in absolute_import_name:
        raise ValueError(
            f'--import_name must be <dir>:<name>, got: {absolute_import_name}')

    relative_import_dir, import_name = absolute_import_name.split(':', 1)
    output_dir = f'{relative_import_dir}/{import_name}'

    user_config = json.loads(import_config_str) if import_config_str else {}
    if not bucket_name:
        bucket_name = (user_config.get('storage_prod_bucket_name') or
                       os.environ.get('GCS_BUCKET_ID') or
                       'datcom-prod-imports')
    project_id = (user_config.get('gcp_project_id') or
                  os.environ.get('PROJECT_ID') or
                  os.environ.get('GOOGLE_CLOUD_PROJECT', ''))

    ignore_validation_status = user_config.get('ignore_validation_status',
                                               False)
    enable_skip_status = user_config.get('enable_skip_status', True)

    gcs_client = storage.Client(project=project_id or None)

    version = version_override
    if not version:
        version = _read_gcs_text(gcs_client, bucket_name,
                                 f'{output_dir}/staging_version.txt')
    if not version:
        raise RuntimeError(
            f'No candidate version found in gs://{bucket_name}/{output_dir}/staging_version.txt'
        )

    latest_version = _read_gcs_text(gcs_client, bucket_name,
                                    f'{output_dir}/latest_version.txt')
    latest_version_uri = (f'gs://{bucket_name}/{output_dir}/{latest_version}'
                          if latest_version else '')

    logging.info('Running validation-helper for %s version=%s (latest=%s)',
                 output_dir, version, latest_version or 'None')

    summary_raw = _read_gcs_text(
        gcs_client, bucket_name,
        f'{output_dir}/{version}/import_summary.json')
    import_summary: Dict = json.loads(summary_raw) if summary_raw else {
        'import_name': import_name,
        'latest_version': f'gs://{bucket_name}/{output_dir}/{version}',
        'import_stats': {},
    }
    import_stats = import_summary.setdefault('import_stats', {})

    input_prefixes = _discover_input_prefixes(gcs_client, bucket_name,
                                              output_dir, version)
    validation_status = True
    differ_status = False
    validation_data_size = 0

    default_val_config = os.path.join(_APP_DIR, 'tools', 'import_validation',
                                      'validation_config.json')

    for input_prefix in input_prefixes:
        logging.info('Processing %s for %s/%s', input_prefix, output_dir,
                     version)
        with tempfile.TemporaryDirectory() as tmpdir:
            val_local_dir = os.path.join(tmpdir, 'validation')
            genmcf_local_dir = os.path.join(tmpdir, 'genmcf')
            os.makedirs(val_local_dir, exist_ok=True)
            os.makedirs(genmcf_local_dir, exist_ok=True)

            # 1. Run BigQuery Differ
            current_mcf_pattern = (
                f'gs://{bucket_name}/{output_dir}/{version}/{input_prefix}/genmcf/*.mcf'
            )
            previous_mcf_pattern = (
                f'{latest_version_uri}/{input_prefix}/genmcf/*.mcf'
                if latest_version_uri else '')

            diff_found = True
            differ_output_dir = ''
            if previous_mcf_pattern and file_util.file_get_matching(
                    previous_mcf_pattern):
                differ_summary = bigquery_differ.run_bigquery_differ(
                    current_data=current_mcf_pattern,
                    previous_data=previous_mcf_pattern,
                    output_location=val_local_dir,
                    project_id=project_id,
                    job_name=f'{import_name}_{input_prefix}',
                    dataset_id=bq_dataset,
                )
                diff_found = (differ_summary.get('obs_diff_count', 1) != 0 or
                              differ_summary.get('schema_diff_count', 1) != 0)
                differ_output_dir = val_local_dir
            else:
                logging.info(
                    'No previous MCF files found at %s; skipping differ.',
                    previous_mcf_pattern)

            if not differ_status:
                differ_status = diff_found

            # 2. Download summary_report.csv and report.json for ValidationRunner
            summary_stats_local = os.path.join(genmcf_local_dir,
                                               'summary_report.csv')
            report_json_local = os.path.join(genmcf_local_dir, 'report.json')
            gcs_summary_stats = (
                f'gs://{bucket_name}/{output_dir}/{version}/{input_prefix}/genmcf/summary_report.csv'
            )
            gcs_report_json = (
                f'gs://{bucket_name}/{output_dir}/{version}/{input_prefix}/genmcf/report.json'
            )
            if file_util.file_get_matching(gcs_summary_stats):
                file_util.file_copy(gcs_summary_stats, summary_stats_local)
            if file_util.file_get_matching(gcs_report_json):
                file_util.file_copy(gcs_report_json, report_json_local)

            val_config_path = _resolve_validation_config(
                gcs_client, bucket_name, output_dir, relative_import_dir,
                import_name, version, default_val_config, tmpdir)

            val_output_file = os.path.join(val_local_dir,
                                           'validation_output.csv')
            runner = ValidationRunner(
                validation_config_path=val_config_path,
                differ_output=differ_output_dir,
                stats_summary=summary_stats_local,
                lint_report=report_json_local,
                validation_output=val_output_file,
            )
            overall_status, _ = runner.run_validations()
            validation_status = validation_status and overall_status

            # 3. Upload validation artifacts to GCS
            gcs_val_dest = f'{output_dir}/{version}/{input_prefix}/validation'
            bucket = gcs_client.bucket(bucket_name)
            for fname in os.listdir(val_local_dir):
                fpath = os.path.join(val_local_dir, fname)
                if os.path.isfile(fpath):
                    validation_data_size += os.path.getsize(fpath)
                    blob = bucket.blob(f'{gcs_val_dest}/{fname}')
                    blob.upload_from_filename(fpath)

    # 4. Update import_summary.json status in GCS
    elapsed = int(time.time() - start_time)
    import_stats['validation_execution_time'] = elapsed
    import_stats['validation_data_size'] = validation_data_size
    import_summary['execution_time'] = int(
        import_summary.get('execution_time', 0)) + elapsed
    import_summary['data_volume'] = int(
        import_stats.get('source_data_size', 0) +
        import_stats.get('mcf_data_size', 0) + validation_data_size)

    if not ignore_validation_status and not validation_status:
        logging.error('Marking import as VALIDATION due to validation failure.')
        import_summary['status'] = 'VALIDATION'
    elif enable_skip_status and not differ_status:
        logging.info('Marking import as SKIP due to no data diff.')
        import_summary['status'] = 'SKIP'
    else:
        import_summary['status'] = 'STAGING'

    _write_gcs_text(
        gcs_client,
        bucket_name,
        f'{output_dir}/{version}/import_summary.json',
        json.dumps(import_summary, default=str),
    )
    logging.info('Completed validation-helper: status=%s in %ds',
                 import_summary['status'], elapsed)
    return 0


def main(_):
    return run_validation_job(
        FLAGS.import_name,
        FLAGS.import_config,
        FLAGS.version,
        FLAGS.gcs_bucket,
        FLAGS.bq_dataset,
    )


if __name__ == '__main__':
    app.run(main)
