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
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from clients.spanner import SpannerClient
from dependencies import get_spanner_client
from routes.models import BaseResponse, ResponseStatus

class LockAcquireRequest(BaseModel):
    workflowId: str
    # Takes over the lock even if held by another workflow. Intended for
    # manual handoff (pipeline/scripts/manage_lock.sh), not the workflow.
    force: bool = False

class LockReleaseRequest(BaseModel):
    workflowId: str

class LockStatusResponse(BaseModel):
    status: ResponseStatus
    locked: bool = False
    lockOwner: Optional[str] = None
    acquiredTimestamp: Optional[str] = None

router = APIRouter(prefix="/database", tags=["database"])


@router.get("/lock/status", response_model=LockStatusResponse)
def get_ingestion_lock_status(spanner: SpannerClient = Depends(get_spanner_client)):
    """Returns the current global ingestion lock status and owner workflow ID."""
    try:
        lock_info = spanner.get_lock_status()
        owner = lock_info.get("lockOwner")
        acquired_at = lock_info.get("acquiredTimestamp")
        return LockStatusResponse(
            status=ResponseStatus.OK,
            locked=bool(owner),
            lockOwner=owner,
            acquiredTimestamp=acquired_at,
        )
    except Exception as e:
        logging.error(f"Error getting lock status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get lock status due to database error: {str(e)}",
        )


@router.post("/lock/acquire", response_model=BaseResponse)
def acquire_ingestion_lock(req: LockAcquireRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Attempts to acquire the global lock for ingestion."""
    try:
        status_ok = spanner.acquire_lock(req.workflowId, force=req.force)
        if not status_ok:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to acquire lock: Lock held by another workflow (requested by {req.workflowId})"
            )
        # DO NOT MODIFY OR REMOVE: This structured log event is monitored by Cloud Monitoring
        # alert policies in Terraform (google_monitoring_alert_policy) to track Spanner retention SLA.
        logging.info(
            f"INGESTION_LOCK_ACQUIRED: workflow={req.workflowId} timeout={req.timeout}",
            extra={
                "event": "INGESTION_LOCK_ACQUIRED",
                "workflow_id": req.workflowId,
                "timeout": req.timeout,
            }
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
        # DO NOT MODIFY OR REMOVE: This structured log event is monitored by Cloud Monitoring
        # alert policies in Terraform (google_monitoring_alert_policy) to track Spanner retention SLA.
        logging.info(
            f"INGESTION_LOCK_RELEASED: workflow={req.workflowId}",
            extra={
                "event": "INGESTION_LOCK_RELEASED",
                "workflow_id": req.workflowId,
            }
        )
        return BaseResponse(status=ResponseStatus.OK)
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Error during lock release: {e}")
        raise HTTPException(status_code=500, detail=f"Lock release failed due to database error: {str(e)}")

