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
from pydantic import BaseModel
from clients.spanner import SpannerClient
from dependencies import get_spanner_client
from routes.models import BaseResponse, ResponseStatus
from utils.logging import log_start

class LockAcquireRequest(BaseModel):
    workflowId: str
    timeout: int

class LockReleaseRequest(BaseModel):
    workflowId: str

router = APIRouter(prefix="/database", tags=["database"])

@router.post("/initialize", response_model=BaseResponse)
@log_start
def initialize_database(spanner: SpannerClient = Depends(get_spanner_client)):
    """Initializes the database by creating all required tables and proto bundles."""
    try:
        spanner.initialize_database()
        return BaseResponse(status=ResponseStatus.OK)
    except Exception as e:
        logging.error(f"Failed to initialize database: {e}")
        raise HTTPException(status_code=500, detail=f"Database initialization failed: {str(e)}")

@router.post("/seed", response_model=BaseResponse)
@log_start
def seed_database(spanner: SpannerClient = Depends(get_spanner_client)):
    """Seeds the database with base empty nodes."""
    try:
        spanner.seed_database()
        return BaseResponse(status=ResponseStatus.OK)
    except Exception as e:
        logging.error(f"Failed to seed database: {e}")
        raise HTTPException(status_code=500, detail=f"Database seeding failed: {str(e)}")

@router.post("/lock/acquire", response_model=BaseResponse)
def acquire_ingestion_lock(req: LockAcquireRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Attempts to acquire the global lock for ingestion."""
    try:
        status_ok = spanner.acquire_lock(req.workflowId, req.timeout)
        if not status_ok:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to acquire lock: Lock already held or acquisition timed out for workflow {req.workflowId}"
            )
        return BaseResponse(status=ResponseStatus.OK)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error during lock acquisition: {e}")
        raise HTTPException(status_code=500, detail=f"Lock acquisition failed due to database error: {str(e)}")

@router.post("/lock/release", response_model=BaseResponse)
def release_ingestion_lock(req: LockReleaseRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Releases the global ingestion lock."""
    try:
        status_ok = spanner.release_lock(req.workflowId)
        if not status_ok:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to release lock: Lock not held by workflow {req.workflowId} or already released"
            )
        return BaseResponse(status=ResponseStatus.OK)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error during lock release: {e}")
        raise HTTPException(status_code=500, detail=f"Lock release failed due to database error: {str(e)}")
