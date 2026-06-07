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
from fastapi import APIRouter, HTTPException
from utils.aggregation import AggregationUtils
import config
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field
from routes.models import BaseResponse, ResponseStatus
from utils.logging import log_start

class AggregationRequest(BaseModel):
    importList: List[Dict[str, Any]] = Field(default_factory=list)

class AggregationStatusRequest(BaseModel):
    jobIds: List[str] = Field(default_factory=list)

class AggregationResponse(BaseResponse):
    jobIds: List[str] = Field(default_factory=list, description="BigQuery job IDs submitted for async aggregation")

class AggregationStatusResponse(BaseResponse):
    error: Optional[str] = Field(default=None, description="Detailed error message if failed")
    failedJobs: Optional[List[str]] = Field(default_factory=list, description="List of failed BigQuery job IDs")

router = APIRouter(prefix="/aggregation", tags=["aggregation"])

@router.post("/run", response_model=AggregationResponse)
@log_start
def run_aggregation(req: AggregationRequest):
    """Runs aggregation logic asynchronously for the specified imports, returning BigQuery job IDs."""
    if not req.importList:
        logging.info("Empty import list. Skipping aggregation.")
        return AggregationResponse(status=ResponseStatus.SUBMITTED, jobIds=[])
        
    if not all([config.SPANNER_CONNECTION_ID, config.SPANNER_PROJECT_ID, config.SPANNER_INSTANCE_ID, config.SPANNER_GRAPH_DATABASE_ID]):
        raise HTTPException(
            status_code=400,
            detail="Missing required configuration environment variables: SPANNER_CONNECTION_ID, SPANNER_PROJECT_ID, SPANNER_INSTANCE_ID, or SPANNER_GRAPH_DATABASE_ID"
        )
        
    aggregation = AggregationUtils(
        connection_id=config.SPANNER_CONNECTION_ID,
        project_id=config.SPANNER_PROJECT_ID,
        instance_id=config.SPANNER_INSTANCE_ID,
        database_id=config.SPANNER_GRAPH_DATABASE_ID,
        location=config.LOCATION,
        is_base_dc=config.IS_BASE_DC,
    )
    try:
        job_ids = aggregation.run_aggregation(req.importList)
        return AggregationResponse(status=ResponseStatus.SUBMITTED, jobIds=job_ids)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation failed: {str(e)}")

@router.post("/status", response_model=AggregationStatusResponse)
def check_aggregation_status(req: AggregationStatusRequest):
    """Checks the status of the submitted aggregation BigQuery jobs."""
    if not req.jobIds:
        logging.info("Empty jobIds. Returning status DONE.")
        return AggregationStatusResponse(status=ResponseStatus.DONE)
        
    if not all([config.SPANNER_CONNECTION_ID, config.SPANNER_PROJECT_ID, config.SPANNER_INSTANCE_ID, config.SPANNER_GRAPH_DATABASE_ID]):
        raise HTTPException(
            status_code=400,
            detail="Missing required configuration environment variables."
        )
        
    aggregation = AggregationUtils(
        connection_id=config.SPANNER_CONNECTION_ID,
        project_id=config.SPANNER_PROJECT_ID,
        instance_id=config.SPANNER_INSTANCE_ID,
        database_id=config.SPANNER_GRAPH_DATABASE_ID,
        location=config.LOCATION,
        is_base_dc=config.IS_BASE_DC,
    )
    try:
        status_info = aggregation.check_aggregation_status(req.jobIds)
        raw_status = status_info.get("status", "ERROR")
        try:
            enum_status = ResponseStatus(raw_status)
        except ValueError:
            enum_status = ResponseStatus.ERROR
            
        return AggregationStatusResponse(
            status=enum_status,
            error=status_info.get("error"),
            failedJobs=status_info.get("failedJobs", [])
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Aggregation status check failed: {str(e)}")
