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
import logging
from typing import List
from fastapi import APIRouter, Depends, HTTPException, Request
from clients.spanner import SpannerClient
from clients.storage import StorageClient
from dependencies import get_spanner_client, get_storage_client
import config
from utils import imports as import_utils
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
from routes.models import BaseResponse, ResponseStatus

class IngestionState(str, Enum):
    SUCCESS = "SUCCESS"
    PENDING = "PENDING"
    RETRY = "RETRY"

class ImportState(str, Enum):
    STAGING = "STAGING"
    FAILURE = "FAILURE"
    SUCCESS = "SUCCESS"

class ImportItem(BaseModel):
    importName: str
    latestVersion: Optional[str] = None

class ImportInfoRequest(BaseModel):
    importList: Optional[List[str]] = Field(default_factory=list)

class UpdateIngestionStatusRequest(BaseModel):
    importList: List[ImportItem]
    workflowId: str
    jobId: str
    status: IngestionState

class UpdateImportStatusRequest(BaseModel):
    importName: str
    status: ImportState
    jobId: Optional[str] = None
    executionTime: Optional[int] = None
    dataVolume: Optional[int] = None
    latestVersion: Optional[str] = None
    graphPath: Optional[str] = None
    nextRefresh: Optional[str] = None

class UpdateImportVersionRequest(BaseModel):
    importName: str
    version: str
    comment: str
    override: Optional[bool] = False
    triggerIngestion: Optional[bool] = False

class ImportInfoItem(BaseModel):
    importName: str = Field(description="The name of the import")
    latestVersion: str = Field(description="The latest version GCS filename")
    graphPath: str = Field(description="The full GCS path to the import graph files")

router = APIRouter(prefix="/imports", tags=["imports"])

@router.post("/info", response_model=List[ImportInfoItem])
def get_import_info(req: ImportInfoRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Gets the details of imports that are ready for ingestion."""
    return spanner.get_import_info(req.importList)

@router.post("/ingestion-status", response_model=BaseResponse)
def update_ingestion_status(req: UpdateIngestionStatusRequest, spanner: SpannerClient = Depends(get_spanner_client)):
    """Updates the status of imports after ingestion."""
    ingested_imports = [item.importName for item in req.importList]
    spanner.update_ingestion_status(ingested_imports, req.workflowId, req.status.value)
    
    metrics = import_utils.get_ingestion_metrics(config.PROJECT_ID, config.LOCATION, req.jobId)
    spanner.update_ingestion_history(req.workflowId, req.jobId, ingested_imports, metrics)
    
    if req.status == IngestionState.SUCCESS:
        import_list_dicts = [item.model_dump() for item in req.importList]
        spanner.update_import_version_history(import_list_dicts, req.workflowId)
    return BaseResponse(status=ResponseStatus.OK)

@router.post("/status", response_model=BaseResponse)
def update_import_status(
    req: UpdateImportStatusRequest,
    spanner: SpannerClient = Depends(get_spanner_client),
    storage: StorageClient = Depends(get_storage_client)
):
    """Updates the status of a specific import job."""
    logging.info(f"Updating import {req.importName} to status {req.status}")
    
    req_dict = req.model_dump(exclude_none=True)
    params = import_utils.get_import_params(req_dict)
    
    next_refresh = None
    if config.IS_BASE_DC:
        next_refresh = import_utils.get_next_refresh(config.PROJECT_ID, config.LOCATION, req.importName)
        
    if next_refresh:
        params['next_refresh'] = next_refresh
        
    if req.status == ImportState.STAGING:
        version = os.path.basename(req.latestVersion or '')
        if not version:
            raise HTTPException(status_code=400, detail=f"Empty version for import {req.importName}")
        storage.update_version_file(req.importName, version, is_staging=True)
        storage.update_provenance_file(req.importName, version)
        storage.update_import_summary(params)
        storage.update_version_file(req.importName, version, is_staging=False)
        comment = f"import-workflow:{req.jobId or ''}"
        spanner.update_version_history(req.importName, version, comment)
        
    spanner.update_import_status(params)
    return BaseResponse(status=ResponseStatus.OK)

@router.post("/version", response_model=BaseResponse)
def update_import_version(
    req: UpdateImportVersionRequest,
    request: Request,
    spanner: SpannerClient = Depends(get_spanner_client),
    storage: StorageClient = Depends(get_storage_client)
):
    """Updates the version and status of an import."""
    logging.info(f"Updating import {req.importName} to version {req.version} comment: {req.comment}")
    
    version = req.version
    if version == 'STAGING':
        version = storage.get_staging_version(req.importName)
        
    summary = storage.get_import_summary(req.importName, version)
    params = import_utils.get_import_params(summary)
    
    comment = req.comment
    if req.override:
        params['status'] = 'STAGING'
        caller = import_utils.get_caller_identity(request)
        comment = f'version-override:{caller} {comment}'
        
    if params['status'] == 'STAGING':
        storage.update_provenance_file(req.importName, version)
        storage.update_version_file(req.importName, version, is_staging=False)
        spanner.update_version_history(req.importName, version, comment)
        logging.info(f"Updated import {req.importName} to version {version}")
    else:
        logging.info(f"Skipping {req.importName} version update")
        
    spanner.update_import_status(params)
    return BaseResponse(
        status=ResponseStatus.OK, 
        message=f"Import: {req.importName} Version: {version} Status: {params['status']}"
    )
