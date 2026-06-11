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
EMBEDDING_MODEL_ID = os.environ.get('EMBEDDING_MODEL_ID', 'text-embedding-005')
NODE_TYPES = ['StatisticalVariable', 'Topic']

REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT', '6379')
GCS_OUTPUT_PREFIX = os.environ.get('GCS_OUTPUT_PREFIX', '')

