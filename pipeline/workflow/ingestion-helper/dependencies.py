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

from fastapi import HTTPException
from clients.spanner import SpannerClient
from clients.storage import StorageClient
import config

def get_spanner_client() -> SpannerClient:
    if not config.SPANNER_PROJECT_ID or not config.SPANNER_INSTANCE_ID or not config.SPANNER_DATABASE_ID:
        raise HTTPException(
            status_code=500,
            detail="Spanner configuration is missing. Ensure SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, and SPANNER_DATABASE_ID are set."
        )
    return SpannerClient(
        config.SPANNER_PROJECT_ID,
        config.SPANNER_INSTANCE_ID,
        config.SPANNER_DATABASE_ID,
        graph_database_id=config.SPANNER_GRAPH_DATABASE_ID,
        location=config.LOCATION,
        model_id=config.EMBEDDING_MODEL_ID
    )

def get_storage_client() -> StorageClient:
    if not config.GCS_BUCKET_ID:
        raise HTTPException(
            status_code=500,
            detail="GCS Bucket ID configuration is missing. Ensure GCS_BUCKET_ID is set."
        )
    return StorageClient(config.GCS_BUCKET_ID)
