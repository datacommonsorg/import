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

import logging
from fastapi import APIRouter, Depends, HTTPException
from typing import Optional
from pydantic import BaseModel, Field
from clients.spanner import SpannerClient
from dependencies import get_spanner_client
from utils.embeddings import get_latest_lock_timestamp, get_updated_nodes, filter_and_convert_nodes, generate_embeddings_partitioned
import config
from routes.models import BaseResponse, ResponseStatus
from utils.logging import log_start

class EmbeddingIngestionRequest(BaseModel):
    enableEmbeddings: Optional[bool] = None

class EmbeddingIngestionResponse(BaseResponse):
    affected_rows: int = Field(description="Number of rows affected/updated with embeddings")

router = APIRouter(prefix="/embeddings", tags=["embeddings"])

@router.post("/ingest", response_model=EmbeddingIngestionResponse)
@log_start
def embedding_ingestion(req: EmbeddingIngestionRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Generates embeddings for configured node types."""
    enable_embeddings = req.enableEmbeddings if req.enableEmbeddings is not None else config.ENABLE_EMBEDDINGS
    if not enable_embeddings:
        logging.info("Embeddings not enabled, skipping.")
        raise HTTPException(
            status_code=412,
            detail="Precondition Failed: Embedding generation is disabled in this environment."
        )
        
    try:
        timestamp = get_latest_lock_timestamp(spanner.database)
        total_affected_rows = 0
        for spec in config.EMBEDDING_SPECS:
            node_types = spec["node_types"]
            model_name = spec["model_name"]
            embedding_label = spec["embedding_label"]
            task_type = spec["task_type"]

            logging.info(f"Job started for {embedding_label}. Fetching all nodes for types: {node_types}")
            nodes = get_updated_nodes(spanner.database, timestamp, node_types, timeout=config.TIMEOUT)
            # materializing generator to list if necessary, but generator works since it yields
            converted_nodes = list(filter_and_convert_nodes(nodes))

            logging.info(f"Generating embeddings for model {model_name} (embedding_label: {embedding_label})")
            affected_rows = generate_embeddings_partitioned(
                spanner.database,
                converted_nodes,
                model_name=model_name,
                embedding_table=spanner.embedding_table,
                embedding_label=embedding_label,
                task_type=task_type,
                timeout=config.TIMEOUT
            )
            total_affected_rows += affected_rows
        return EmbeddingIngestionResponse(status=ResponseStatus.OK, affected_rows=total_affected_rows)
    except Exception as e:
        logging.error(f"Embedding ingestion failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))
