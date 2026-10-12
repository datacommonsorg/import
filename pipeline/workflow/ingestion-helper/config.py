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

import os
import json
import logging
import tempfile
from typing import List
from pydantic import BaseModel, Field
import yaml


# ------------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------------

def _parse_duration_seconds(duration_str: str, default_seconds: int = 86400) -> int:
    """Parses duration strings like '24h', '7d', '3600s' or integer seconds into seconds."""
    if not duration_str:
        return default_seconds
    val = duration_str.strip().lower()
    try:
        if val.endswith('d'):
            return int(val[:-1]) * 86400
        if val.endswith('h'):
            return int(val[:-1]) * 3600
        if val.endswith('s'):
            return int(val[:-1])
        return int(val)
    except (ValueError, TypeError):
        return default_seconds


# ------------------------------------------------------------------------------
# Global / GCP Environment Settings
# ------------------------------------------------------------------------------

PROJECT_ID = os.environ.get('PROJECT_ID')
LOCATION = os.environ.get('LOCATION') or os.environ.get('REGION')
GCS_BUCKET_ID = os.environ.get('GCS_BUCKET_ID')
GCS_OUTPUT_PREFIX = os.environ.get('GCS_OUTPUT_PREFIX', '')
IS_BASE_DC = os.environ.get('IS_BASE_DC', 'true').lower() == 'true'
TIMEOUT = int(os.environ.get('TIMEOUT', 1700))


# ------------------------------------------------------------------------------
# Spanner Settings
# ------------------------------------------------------------------------------

SPANNER_DATABASE_PATH = os.environ.get('SPANNER_DATABASE_PATH')
if SPANNER_DATABASE_PATH and len(SPANNER_DATABASE_PATH.split('/')) >= 6:
    _parts = SPANNER_DATABASE_PATH.split('/')
    SPANNER_PROJECT_ID = _parts[1]
    SPANNER_INSTANCE_ID = _parts[3]
    SPANNER_DATABASE_ID = _parts[5]
else:
    SPANNER_PROJECT_ID = os.environ.get('SPANNER_PROJECT_ID')
    SPANNER_INSTANCE_ID = os.environ.get('SPANNER_INSTANCE_ID')
    SPANNER_DATABASE_ID = os.environ.get('SPANNER_DATABASE_ID')

SPANNER_EMULATOR_HOST = os.environ.get('SPANNER_EMULATOR_HOST')
SPANNER_INGESTION_WORKFLOW_NAME = os.environ.get('SPANNER_INGESTION_WORKFLOW_NAME', 'spanner-ingestion-workflow')

# Retention period tracking for Spanner Point-in-Time Recovery (PITR) window protection
SPANNER_VERSION_RETENTION_PERIOD = os.environ.get('SPANNER_VERSION_RETENTION_PERIOD', '24h')
SPANNER_RETENTION_SECONDS = _parse_duration_seconds(SPANNER_VERSION_RETENTION_PERIOD)


# ------------------------------------------------------------------------------
# Redis Settings
# ------------------------------------------------------------------------------

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD') or None
REDIS_CA_CERT = os.environ.get('REDIS_CA_CERT') or None
REDIS_CA_CERT_PATH = None
if REDIS_CA_CERT:
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.pem') as _f:
        _f.write(REDIS_CA_CERT)
        REDIS_CA_CERT_PATH = _f.name


# ------------------------------------------------------------------------------
# Embeddings Configuration
# ------------------------------------------------------------------------------

class EmbeddingSpec(BaseModel):
    embedding_label: str
    model_name: str
    task_type: str
    node_types: List[str]
    node_filter_type: str


ENABLE_EMBEDDINGS = os.environ.get('ENABLE_EMBEDDINGS', 'false').lower() == 'true'

_DEFAULT_EMBEDDING_SPECS = [
    EmbeddingSpec(
        embedding_label="base_text_embedding",
        model_name="NodeEmbeddingModel",
        task_type="RETRIEVAL_QUERY",
        node_types=["StatisticalVariable", "Topic"],
        node_filter_type="NoFilter"
    )
]

spec_path = os.environ.get('EMBEDDING_SPEC_PATH')


def _load_embedding_specs(spec_path: str) -> List[EmbeddingSpec]:
    """Load embedding specs from file. If failed reading, use default spec.

    Args:
        spec_path: Path to the embedding specs file.

    Returns:
        List of EmbeddingSpec objects.
    """
    global _DEFAULT_EMBEDDING_SPECS
    if not spec_path:
        return _DEFAULT_EMBEDDING_SPECS
    resolved_path = os.path.abspath(spec_path)
    if not (os.path.isabs(spec_path) or os.path.exists(resolved_path)):
        resolved_path = os.path.join(os.path.dirname(__file__), spec_path)
    if not os.path.exists(resolved_path):
        logging.warning(f"EMBEDDING_SPEC_PATH file not found: {resolved_path}. Using defaults.")
        return _DEFAULT_EMBEDDING_SPECS
    try:
        with open(resolved_path, 'r') as f:
            parsed = yaml.safe_load(f)
        if isinstance(parsed, dict):
            parsed = [parsed]
        if not isinstance(parsed, list):
            logging.warning(f"Invalid format in EMBEDDING_SPEC_PATH file: {resolved_path}. Must be a list or a dictionary. Using defaults.")
            return _DEFAULT_EMBEDDING_SPECS
        validated = [EmbeddingSpec(**spec) for spec in parsed]
        logging.info(f"Successfully loaded embedding specs from {resolved_path}")
        return validated
    except Exception as e:
        logging.warning(f"Error reading/validating EMBEDDING_SPEC_PATH file: {e}. Using defaults.")
        return _DEFAULT_EMBEDDING_SPECS


EMBEDDING_SPECS = _load_embedding_specs(spec_path)
