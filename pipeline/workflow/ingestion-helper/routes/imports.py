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
from clients.spanner import SpannerClient, IngestionState, IngestionStage
from clients.storage import StorageClient
from dependencies import get_spanner_client, get_storage_client
import config
from utils import imports as import_utils
from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field
from routes.models import BaseResponse, ResponseStatus


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
    status: IngestionState
    jobId: Optional[str] = None


class UpdateIngestionHistoryRequest(BaseModel):
    workflowId: str
    status: IngestionState
    stage: Optional[IngestionStage] = None
    importList: Optional[List[ImportItem]] = Field(default_factory=list)
    importName: Optional[str] = None
    jobId: Optional[str] = None


class ImportStatusItem(BaseModel):
    importName: str
    status: ImportState
    latestVersion: Optional[str] = None
    graphPath: Optional[str] = None


class UpdateImportStatusRequest(BaseModel):
    imports: List[ImportStatusItem]
    jobId: Optional[str] = None
    workflowId: Optional[str] = None
    executionTime: Optional[int] = None
    dataVolume: Optional[int] = None
    nextRefresh: Optional[str] = None


class UpdateImportVersionRequest(BaseModel):
    imports: List[str]
    version: str
    comment: str
    workflowId: Optional[str] = None
    jobId: Optional[str] = None
    override: Optional[bool] = False
    triggerIngestion: Optional[bool] = False


class ImportInfoItem(BaseModel):
    importName: str = Field(description="The name of the import")
    latestVersion: str = Field(description="The latest version GCS filename")
    graphPath: str = Field(
        description="The full GCS path to the import graph files")


router = APIRouter(prefix="/imports", tags=["imports"])


@router.post("/info", response_model=List[ImportInfoItem])
def get_import_info(req: ImportInfoRequest,
                    spanner: SpannerClient = Depends(get_spanner_client)):
    """Gets the details of imports that are ready for ingestion."""
    return spanner.get_import_info(req.importList)


def _extract_import_names(
        import_list: Optional[List[ImportItem]]) -> Optional[List[str]]:
    if not import_list:
        return None
    return [item.importName for item in import_list]


@router.post("/ingestion-status", response_model=BaseResponse)
def update_ingestion_status(
    req: UpdateIngestionStatusRequest,
    spanner: SpannerClient = Depends(get_spanner_client)):
    """Updates the status of imports after ingestion."""
    ingested_imports = _extract_import_names(req.importList)
    status_str = req.status.value if hasattr(req.status, 'value') else req.status
    spanner.update_ingestion_status(ingested_imports, req.workflowId, status_str)

    metrics = None
    if req.jobId and req.jobId != "N/A":
        try:
            metrics = import_utils.get_ingestion_metrics(
                config.PROJECT_ID, config.LOCATION, req.jobId)
        except Exception as e:
            logging.error(f"Failed to fetch metrics for job {req.jobId}: {e}")
            metrics = None

    if req.status == IngestionState.SUCCESS:
        import_list_dicts = [item.model_dump() for item in req.importList]
        spanner.update_import_version_history(import_list_dicts,
                                              req.workflowId,
                                              status=status_str,
                                              metrics=metrics)
    return BaseResponse(status=ResponseStatus.OK)


@router.post("/ingestion-history", response_model=BaseResponse)
def update_ingestion_history(
    req: UpdateIngestionHistoryRequest,
    spanner: SpannerClient = Depends(get_spanner_client)):
    """Updates the ingestion history record for the workflow execution."""
    if not config.ENABLE_UNIQUE_INGESTION_RUNS:
        logging.warning(
            "Received update_ingestion_history request but ENABLE_UNIQUE_INGESTION_RUNS is disabled. No-op."
        )
        return BaseResponse(status=ResponseStatus.OK)

    ingested_imports = _extract_import_names(req.importList)
    if req.importName:
        ingested_imports.append(req.importName)

    metrics = None
    if req.status in (
            IngestionState.SUCCESS, IngestionState.RETRY
    ) and req.stage == IngestionStage.DATAFLOW and req.jobId and req.jobId != "N/A":
        # Only update metrics for successful or retried jobs running after the dataflow stage.
        try:
            metrics = import_utils.get_ingestion_metrics(
                config.PROJECT_ID, config.LOCATION, req.jobId)
        except Exception as e:
            logging.error(f"Failed to fetch metrics for job {req.jobId}: {e}")
            metrics = None

    spanner.update_ingestion_history(workflow_id=req.workflowId,
                                     status=req.status,
                                     stage=req.stage,
                                     job_id=req.jobId,
                                     ingested_imports=ingested_imports,
                                     metrics=metrics)

    return BaseResponse(status=ResponseStatus.OK)


@router.post("/status", response_model=BaseResponse)
def update_import_status(req: UpdateImportStatusRequest,
                         spanner: SpannerClient = Depends(get_spanner_client),
                         storage: StorageClient = Depends(get_storage_client)):
    """Updates the status of import jobs."""
    for item in req.imports:
        logging.info(
            f"Updating import {item.importName} to status {item.status}")

        # Construct dictionary parameters for the individual import
        import_req = {
            "importName": item.importName,
            "status": item.status,
            "jobId": req.jobId,
            "executionTime": req.executionTime,
            "dataVolume": req.dataVolume,
            "latestVersion": item.latestVersion,
            "graphPath": item.graphPath,
            "nextRefresh": req.nextRefresh,
        }
        req_dict = {k: v for k, v in import_req.items() if v is not None}
        params = import_utils.get_import_params(req_dict)

        next_refresh = None
        if config.IS_BASE_DC:
            next_refresh = import_utils.get_next_refresh(
                config.PROJECT_ID, config.LOCATION, item.importName)

        if next_refresh:
            params['next_refresh'] = next_refresh

        if item.status == ImportState.STAGING:
            version = os.path.basename(item.latestVersion or '')
            if not version:
                raise HTTPException(
                    status_code=400,
                    detail=f"Empty version for import {item.importName}")
            storage.update_version_file(item.importName,
                                        version,
                                        is_staging=True)
            storage.update_provenance_file(item.importName, version)
            storage.update_import_summary(params)
            storage.update_version_file(item.importName,
                                        version,
                                        is_staging=False)
            wf_id = req.workflowId or req.jobId
            comment = f"import-workflow:{wf_id or ''}"
            status_val = item.status.value if hasattr(item.status,
                                                      'value') else item.status
            spanner.update_version_history(item.importName,
                                           version,
                                           comment,
                                           workflow_id=wf_id,
                                           status=status_val)

        spanner.update_import_status(params)
    return BaseResponse(status=ResponseStatus.OK)


@router.post("/version", response_model=BaseResponse)
def update_import_version(req: UpdateImportVersionRequest,
                          request: Request,
                          spanner: SpannerClient = Depends(get_spanner_client),
                          storage: StorageClient = Depends(get_storage_client)):
    """Updates the version and status of multiple imports."""
    updated_imports = []
    caller = import_utils.get_caller_identity(request) if req.override else None
    for import_name in req.imports:
        logging.info(
            f"Updating import {import_name} to version {req.version} comment: {req.comment}"
        )

        version = req.version
        if version == 'STAGING':
            version = storage.get_staging_version(import_name)

        summary = storage.get_import_summary(import_name, version)
        params = import_utils.get_import_params(summary)

        comment = req.comment
        if req.override:
            params['status'] = 'STAGING'
            comment = f'version-override:{caller} {comment}'

        if params['status'] == 'STAGING':
            storage.update_provenance_file(import_name, version)
            storage.update_version_file(import_name, version, is_staging=False)
            wf_id = req.workflowId or req.jobId
            spanner.update_version_history(import_name,
                                           version,
                                           comment,
                                           workflow_id=wf_id,
                                           status="STAGING")
            logging.info(f"Updated import {import_name} to version {version}")
        else:
            logging.info(f"Skipping {import_name} version update")

        spanner.update_import_status(params)
        updated_imports.append(
            f"Import: {import_name} Version: {version} Status: {params['status']}"
        )

    return BaseResponse(status=ResponseStatus.OK,
                        message="; ".join(updated_imports))
