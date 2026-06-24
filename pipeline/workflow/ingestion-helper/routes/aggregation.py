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
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

import config
from routes.models import BaseResponse, ResponseStatus
from aggregation import AggregationOrchestrator
from utils.logging import log_start

# =============================================================================
# Pydantic Models for the New Stateless API
# =============================================================================

class StateObject(BaseModel):
    status: str = Field(..., description="Overall status of the run: RUNNING, SUCCEEDED, FAILED")
    current_stage: int = Field(..., description="The stage currently executing")
    active_job_ids: List[str] = Field(default_factory=list, description="BQ job IDs running in the current stage")
    import_list: List[Dict[str, Any]] = Field(default_factory=list, description="Original list of imports")
    error: Optional[str] = Field(default=None, description="Detailed error message if failed")

class InitiateRequest(BaseModel):
    importList: List[Dict[str, Any]] = Field(default_factory=list)

# =============================================================================
# Pydantic Models for the Legacy API (Backward Compatibility)
# =============================================================================

class LegacyAggregationRequest(BaseModel):
    importList: List[Dict[str, Any]] = Field(default_factory=list)

class LegacyAggregationStatusRequest(BaseModel):
    jobIds: List[str] = Field(default_factory=list)

class LegacyAggregationResponse(BaseResponse):
    jobIds: List[str] = Field(default_factory=list, description="BigQuery job IDs submitted for async aggregation")

class LegacyAggregationStatusResponse(BaseResponse):
    error: Optional[str] = Field(default=None, description="Detailed error message if failed")
    failedJobs: Optional[List[str]] = Field(default_factory=list, description="List of failed BigQuery job IDs")

# =============================================================================
# Router Definition
# =============================================================================

router = APIRouter(prefix="/aggregation", tags=["aggregation"])


def _get_orchestrator() -> AggregationOrchestrator:
    """Helper to initialize the orchestrator using global config."""
    if not all([config.SPANNER_CONNECTION_ID, config.SPANNER_PROJECT_ID, config.SPANNER_INSTANCE_ID, config.SPANNER_GRAPH_DATABASE_ID]):
        raise HTTPException(
            status_code=400,
            detail="Missing required configuration environment variables: SPANNER_CONNECTION_ID, SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, or SPANNER_GRAPH_DATABASE_ID"
        )
    return AggregationOrchestrator(
        connection_id=config.SPANNER_CONNECTION_ID,
        project_id=config.SPANNER_PROJECT_ID,
        instance_id=config.SPANNER_INSTANCE_ID,
        database_id=config.SPANNER_GRAPH_DATABASE_ID,
        location=config.LOCATION,
        is_base_dc=config.IS_BASE_DC,
    )

# -----------------------------------------------------------------------------
# New Stateless API Endpoints (Stage-based)
# -----------------------------------------------------------------------------

@router.post("/initiate", response_model=StateObject)
@log_start
def initiate_aggregation(req: InitiateRequest):
    """Initiates the aggregation run by executing Stage 1 and returning the initial state."""
    if not req.importList:
        logging.info("Empty import list. Skipping aggregation.")
        return StateObject(status="SUCCEEDED", current_stage=0, active_job_ids=[], import_list=[])

    try:
        orchestrator = _get_orchestrator()
        import_names = [item.get('importName') for item in req.importList if item.get('importName')]
        
        # Find the first stage that has active aggregations (usually Stage 1)
        first_stage = 1
        while first_stage <= 10: # Arbitrary upper limit for safety
            if orchestrator.has_stage(first_stage, import_names):
                break
            first_stage += 1
        else:
            logging.info("No stages have active aggregations for the current imports. Completing immediately.")
            return StateObject(status="SUCCEEDED", current_stage=0, active_job_ids=[], import_list=req.importList)

        logging.info(f"Initiating aggregation at Stage {first_stage}")
        job_ids = orchestrator.execute_stage(first_stage, import_names)
        
        return StateObject(
            status="RUNNING",
            current_stage=first_stage,
            active_job_ids=job_ids,
            import_list=req.importList
        )
    except Exception as e:
        logging.error(f"Failed to initiate aggregation: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to initiate aggregation: {str(e)}")


@router.post("/poll", response_model=StateObject)
@log_start
def poll_aggregation(state: StateObject):
    """Checks progress of active jobs and transitions to the next stage if complete."""
    if state.status != "RUNNING":
        return state # Already in a terminal state

    try:
        orchestrator = _get_orchestrator()
        import_names = [item.get('importName') for item in state.import_list if item.get('importName')]

        # 1. Check status of active jobs in BigQuery
        if not state.active_job_ids:
            bq_status = {"status": "DONE"}
        else:
            logging.info(f"Polling status for jobs in Stage {state.current_stage}: {state.active_job_ids}")
            bq_status = orchestrator.check_jobs_status(state.active_job_ids)
        
        # Case A: Any job failed
        if bq_status["status"] == "FAILED":
            logging.error(f"Stage {state.current_stage} failed with error: {bq_status.get('error')}")
            return StateObject(
                status="FAILED",
                current_stage=state.current_stage,
                active_job_ids=[],
                import_list=state.import_list,
                error=bq_status.get("error")
            )
            
        # Case B: Jobs are still executing (explicitly check for DONE to transition)
        if bq_status["status"] != "DONE":
            logging.info(f"Stage {state.current_stage} is still executing (status: {bq_status['status']}).")
            return state # Return unchanged
            
        # Case C: All jobs succeeded -> Find and execute the next active stage
        next_stage = state.current_stage + 1
        while next_stage <= 10: # Arbitrary upper limit
            if orchestrator.has_stage(next_stage, import_names):
                logging.info(f"Stage {state.current_stage} completed. Transitioning to Stage {next_stage}...")
                new_job_ids = orchestrator.execute_stage(next_stage, import_names)
                return StateObject(
                    status="RUNNING",
                    current_stage=next_stage,
                    active_job_ids=new_job_ids,
                    import_list=state.import_list
                )
            next_stage += 1
            
        # If we exit the loop, there are no more active stages left
        logging.info("All aggregation stages completed successfully!")
        return StateObject(
            status="SUCCEEDED",
            current_stage=state.current_stage,
            active_job_ids=[],
            import_list=state.import_list
        )
            
    except Exception as e:
        logging.error(f"Error during polling: {e}")
        return StateObject(
            status="FAILED",
            current_stage=state.current_stage,
            active_job_ids=[],
            import_list=state.import_list,
            error=f"Orchestrator error: {str(e)}"
        )

# -----------------------------------------------------------------------------
# Legacy API Endpoints (Backward Compatibility Mode)
# -----------------------------------------------------------------------------

@router.post("/run", response_model=LegacyAggregationResponse)
@log_start
def run_aggregation_legacy(req: LegacyAggregationRequest):
    """Legacy endpoint. Runs ALL enabled aggregations in parallel (ignores stages)."""
    if not req.importList:
        logging.info("Empty import list. Skipping legacy aggregation.")
        return LegacyAggregationResponse(status=ResponseStatus.SUBMITTED, jobIds=[])
        
    try:
        orchestrator = _get_orchestrator()
        import_names = [item.get('importName') for item in req.importList if item.get('importName')]
        
        # Compatibility Mode: Submit ALL enabled stages in parallel
        job_ids = []
        for stage_num in range(1, 10):
            if orchestrator.has_stage(stage_num, import_names):
                job_ids.extend(orchestrator.execute_stage(stage_num, import_names))
                
        return LegacyAggregationResponse(status=ResponseStatus.SUBMITTED, jobIds=job_ids)
    except Exception as e:
        logging.error(f"Legacy aggregation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Legacy aggregation failed: {str(e)}")


@router.post("/status", response_model=LegacyAggregationStatusResponse)
@log_start
def check_aggregation_status_legacy(req: LegacyAggregationStatusRequest):
    """Legacy endpoint. Checks the status of the submitted BigQuery jobs."""
    if not req.jobIds:
        logging.info("Empty jobIds. Returning status DONE.")
        return LegacyAggregationStatusResponse(status=ResponseStatus.DONE)
        
    try:
        orchestrator = _get_orchestrator()
        status_info = orchestrator.check_jobs_status(req.jobIds)
        return LegacyAggregationStatusResponse(
            status=ResponseStatus.from_str(status_info.get("status", "ERROR")),
            error=status_info.get("error"),
            failedJobs=status_info.get("failedJobs", [])
        )
    except Exception as e:
        logging.error(f"Legacy status check failed: {e}")
        raise HTTPException(status_code=500, detail=f"Legacy status check failed: {str(e)}")
