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
from typing import List
from pydantic import BaseModel, Field
import yaml

class EmbeddingSpec(BaseModel):
    embedding_label: str
    model_name: str
    task_type: str
    node_types: List[str]
    node_filter_type: str

PROJECT_ID = os.environ.get('PROJECT_ID')
SPANNER_PROJECT_ID = os.environ.get('SPANNER_PROJECT_ID')
SPANNER_INSTANCE_ID = os.environ.get('SPANNER_INSTANCE_ID')
SPANNER_DATABASE_ID = os.environ.get('SPANNER_DATABASE_ID')
SPANNER_GRAPH_DATABASE_ID = os.environ.get('SPANNER_GRAPH_DATABASE_ID')
SPANNER_CONNECTION_ID = os.environ.get('BQ_SPANNER_CONN_ID')
GCS_BUCKET_ID = os.environ.get('GCS_BUCKET_ID')
LOCATION = os.environ.get('LOCATION') or os.environ.get('REGION')
ENABLE_EMBEDDINGS = os.environ.get('ENABLE_EMBEDDINGS', 'false').lower() == 'true'
IS_BASE_DC = os.environ.get('IS_BASE_DC', 'true').lower() == 'true'
TIMEOUT = int(os.environ.get('TIMEOUT', 1700))
EMBEDDING_SPACE = int(os.environ.get('EMBEDDING_SPACE', 768))
EMBEDDING_TABLE = os.environ.get('EMBEDDING_TABLE', 'NodeEmbedding')
EMBEDDING_INDEX = os.environ.get('EMBEDDING_INDEX', 'NodeEmbeddingIndex')
EMBEDDING_LABEL_INDEX = os.environ.get('EMBEDDING_LABEL_INDEX', 'NodeEmbeddingLabelIndex')

_DEFAULT_MODELS = [
    {"name": "NodeEmbeddingModel", "endpoint": "text-embedding-005"}
]

models_env = os.environ.get('EMBEDDING_MODELS')
if models_env:
    try:
        parsed = json.loads(models_env)
        if isinstance(parsed, list) and all(isinstance(m, dict) and "name" in m and "endpoint" in m for m in parsed):
            EMBEDDING_MODELS = parsed
        else:
            EMBEDDING_MODELS = _DEFAULT_MODELS
    except Exception:
        EMBEDDING_MODELS = _DEFAULT_MODELS
else:
    EMBEDDING_MODELS = _DEFAULT_MODELS

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

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
GCS_OUTPUT_PREFIX = os.environ.get('GCS_OUTPUT_PREFIX', '')

SPANNER_EMULATOR_HOST = os.environ.get('SPANNER_EMULATOR_HOST')

