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
from typing import Tuple
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
    timeout: int


class LockReleaseRequest(BaseModel):
    workflowId: str


router = APIRouter(prefix="/database", tags=["database"])


def _evaluate_lock_retention_alert(
    owner: str,
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
        status_ok = spanner.acquire_lock(req.workflowId, req.timeout)
        if not status_ok:
            raise HTTPException(
                status_code=503,
                detail=f"Failed to acquire lock: Lock already held or acquisition timed out for workflow {req.workflowId}"
            )
        # DO NOT MODIFY OR REMOVE: Structured log event monitored by Cloud Monitoring
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
        # DO NOT MODIFY OR REMOVE: Structured log event monitored by Cloud Monitoring
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
        if not lock_status["is_locked"]:
            return LockStatusResponse(is_locked=False, alert_level=LockAlertLevel.OK)

        age_seconds = lock_status["age_seconds"]
        owner = lock_status["lock_owner"]
        retention_seconds = config.SPANNER_RETENTION_SECONDS

        alert_level, event_name = _evaluate_lock_retention_alert(owner, age_seconds, retention_seconds)

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
            is_locked=True,
            lock_owner=owner,
            acquired_timestamp=lock_status["acquired_timestamp"],
            age_seconds=age_seconds,
            alert_level=alert_level
        )
    except Exception as e:
        logging.error(f"Error checking lock status: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to check lock status: {str(e)}")
