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
from typing import Optional, Tuple
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

import config
from clients.spanner import SpannerClient
from dependencies import get_spanner_client
from routes.models import (
    BaseResponse,
    LockAlertLevel,
    LockStatusResponse,
    ResponseStatus,
)

# Threshold ratios for Spanner Point-in-Time Recovery (PITR) window protection
LOCK_WARN_50PCT_RATIO: float = 0.50
LOCK_WARN_90PCT_RATIO: float = 0.90
LOCK_CRIT_100PCT_RATIO: float = 1.00


class LockAcquireRequest(BaseModel):
    workflowId: str
    timeout: Optional[int] = None
    # Takes over the lock even if held by another workflow. Intended for
    # manual handoff (pipeline/scripts/manage_lock.sh), not the workflow.
    force: bool = False


class LockReleaseRequest(BaseModel):
    workflowId: str


router = APIRouter(prefix="/database", tags=["database"])


def _evaluate_lock_retention_alert(
    age_seconds: float,
    retention_seconds: int
) -> Tuple[LockAlertLevel, str | None]:
    """Evaluates lock age against retention thresholds and returns alert level and log event."""
    crit_threshold = retention_seconds * LOCK_CRIT_100PCT_RATIO
    warn_90_threshold = retention_seconds * LOCK_WARN_90PCT_RATIO
    warn_50_threshold = retention_seconds * LOCK_WARN_50PCT_RATIO

    if age_seconds >= crit_threshold:
        return LockAlertLevel.CRITICAL_100PCT, "INGESTION_LOCK_HELD_100PCT_CRITICAL"
    if age_seconds >= warn_90_threshold:
        return LockAlertLevel.WARNING_90PCT, "INGESTION_LOCK_HELD_90PCT_WARNING"
    if age_seconds >= warn_50_threshold:
        return LockAlertLevel.WARNING_50PCT, "INGESTION_LOCK_HELD_50PCT_WARNING"
    return LockAlertLevel.OK, None


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
        timeout_str = f" timeout={req.timeout}" if req.timeout is not None else ""
        extra_payload = {
            "event": "INGESTION_LOCK_ACQUIRED",
            "workflow_id": req.workflowId,
        }
        if req.timeout is not None:
            extra_payload["timeout"] = req.timeout
        logging.info(
            f"INGESTION_LOCK_ACQUIRED: workflow={req.workflowId}{timeout_str}",
            extra=extra_payload
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


@router.get("/lock/status", response_model=LockStatusResponse)
def get_ingestion_lock_status(spanner: SpannerClient = Depends(get_spanner_client)):
    """Returns the status of the global ingestion lock and emits structured retention SLA alerts."""
    try:
        lock_status = spanner.get_lock_status()
        is_locked = lock_status.get("locked", False)
        owner = lock_status.get("lockOwner")
        acquired_ts = lock_status.get("acquiredTimestamp")
        age_seconds = lock_status.get("ageSeconds")

        if not is_locked or not owner:
            return LockStatusResponse(
                status=ResponseStatus.OK,
                locked=False,
                lockOwner=None,
                acquiredTimestamp=None,
                ageSeconds=None,
                alertLevel=LockAlertLevel.OK
            )

        alert_level = LockAlertLevel.OK
        if age_seconds is not None:
            retention_seconds = config.SPANNER_RETENTION_SECONDS
            alert_level, event_name = _evaluate_lock_retention_alert(age_seconds, retention_seconds)

            if event_name:
                log_payload = {
                    "event": event_name,
                    "workflow_id": owner,
                    "age_seconds": age_seconds,
                    "retention_seconds": retention_seconds,
                }
                msg = f"{event_name}: workflow={owner} age_seconds={age_seconds:.0f} retention_seconds={retention_seconds}"
                if alert_level == LockAlertLevel.CRITICAL_100PCT:
                    logging.error(msg, extra=log_payload)
                else:
                    logging.warning(msg, extra=log_payload)

        return LockStatusResponse(
            status=ResponseStatus.OK,
            locked=True,
            lockOwner=owner,
            acquiredTimestamp=acquired_ts,
            ageSeconds=age_seconds,
            alertLevel=alert_level
        )
    except Exception as e:
        logging.error(f"Error checking lock status: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to check lock status due to database error: {str(e)}"
        )
