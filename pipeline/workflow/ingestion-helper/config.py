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
    {
        "embedding_label": "base_text_embedding",
        "model_name": "NodeEmbeddingModel",
        "task_type": "RETRIEVAL_QUERY",
        "node_types": ["StatisticalVariable", "Topic"]
    }
]

specs_env = os.environ.get('EMBEDDING_SPECS')
if specs_env:
    try:
        parsed = json.loads(specs_env)
        required_keys = {"embedding_label", "model_name", "task_type", "node_types"}
        if isinstance(parsed, list) and all(isinstance(s, dict) and required_keys.issubset(s.keys()) for s in parsed):
            EMBEDDING_SPECS = parsed
        else:
            EMBEDDING_SPECS = _DEFAULT_EMBEDDING_SPECS
    except Exception:
        EMBEDDING_SPECS = _DEFAULT_EMBEDDING_SPECS
else:
    EMBEDDING_SPECS = _DEFAULT_EMBEDDING_SPECS

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
GCS_OUTPUT_PREFIX = os.environ.get('GCS_OUTPUT_PREFIX', '')

