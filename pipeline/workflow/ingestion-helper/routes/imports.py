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

import json
import logging
from typing import List, Optional

from clients.spanner import IngestionStage, IngestionState, SpannerClient
import config
from dependencies import get_spanner_client, get_workflow_client
from fastapi import APIRouter, Depends, HTTPException
from google.cloud.workflows import executions_v1
from pydantic import BaseModel, Field
from routes.models import BaseResponse, ResponseStatus
from utils import imports as import_utils
from utils import rollback_helper


class ImportItem(BaseModel):
    importName: str
    latestVersion: Optional[str] = None


class IngestRequest(BaseModel):
    importList: Optional[List[ImportItem]] = Field(default_factory=list)
    forceIngestion: Optional[bool] = False
    dryRun: Optional[bool] = False


class IngestResponse(BaseResponse):
    executionName: Optional[str] = None
    importList: List[ImportItem] = Field(default_factory=list)


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


class RevertImportRequest(BaseModel):
    importName: Optional[str] = Field(default=None, description="Import name to revert")
    workflowId: Optional[str] = Field(default=None, description="Workflow execution ID")
    dryRun: bool = Field(default=False, description="Dry run mode")


class RevertImportResultItem(BaseModel):
    importName: str
    latestVersion: Optional[str] = None
    failedVersion: Optional[str] = None
    restoredVersion: Optional[str] = None


class RevertImportResponse(BaseResponse):
    reverted: bool = False
    revertedImports: List[RevertImportResultItem] = Field(default_factory=list)
    dryRun: bool = False


router = APIRouter(prefix="/imports", tags=["imports"])


@router.post("/ingest", response_model=IngestResponse)
def ingest_imports(
    req: IngestRequest,
    spanner: SpannerClient = Depends(get_spanner_client),
    workflow_client: executions_v1.ExecutionsClient = Depends(get_workflow_client),
):
    """Checks Spanner for ready imports and triggers ingestion workflow if needed."""
    ready_imports = spanner.get_import_info(
        req.importList, force_ingestion=bool(req.forceIngestion)
    )
    if not ready_imports:
        return IngestResponse(
            status=ResponseStatus.SKIPPED,
            message="No imports need ingestion",
            executionName=None,
            importList=[],
        )

    import_items = [
        ImportItem(**item) if isinstance(item, dict) else item
        for item in ready_imports
    ]

    if req.dryRun:
        return IngestResponse(
            status=ResponseStatus.SKIPPED,
            message="Dry run: skipped triggering ingestion workflow",
            executionName=None,
            importList=import_items,
        )

    if not config.PROJECT_ID or not config.LOCATION:
        raise HTTPException(
            status_code=500,
            detail="PROJECT_ID or LOCATION configuration is missing. Ensure PROJECT_ID and LOCATION/REGION are set."
        )

    parent = executions_v1.ExecutionsClient.workflow_path(
        config.PROJECT_ID,
        config.LOCATION,
        config.SPANNER_INGESTION_WORKFLOW_NAME,
    )
    execution_payload = {
        "importList": [item.model_dump() for item in import_items]
    }
    execution = executions_v1.Execution(
        argument=json.dumps(execution_payload)
    )

    try:
        execution_resp = workflow_client.create_execution(
            parent=parent, execution=execution
        )
    except Exception as e:
        logging.error(f"Failed to trigger ingestion workflow {parent}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger ingestion workflow: {e}"
        )

    return IngestResponse(
        status=ResponseStatus.SUBMITTED,
        message=f"Triggered ingestion workflow execution: {execution_resp.name}",
        executionName=execution_resp.name,
        importList=import_items,
    )


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
    import_list_dicts = [item.model_dump() for item in req.importList]
    status_str = req.status.value if hasattr(req.status, 'value') else req.status
    spanner.update_ingestion_status(import_list_dicts, req.workflowId, status_str)

    if req.status == IngestionState.SUCCESS:
        metrics = None
        if req.jobId and req.jobId != "N/A":
            try:
                metrics = import_utils.get_ingestion_metrics(
                    config.PROJECT_ID, config.LOCATION, req.jobId)
            except Exception as e:
                logging.error(f"Failed to fetch metrics for job {req.jobId}: {e}")
                metrics = None

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
    ingested_imports = _extract_import_names(req.importList)
    if req.importName:
        ingested_imports.append(req.importName)

    metrics = None
    if req.status in (
            IngestionState.SUCCESS, IngestionState.RETRY
    ) and req.jobId and req.jobId != "N/A":
        # Only update metrics for successful or retried jobs when a valid Dataflow jobId is provided.
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


@router.post("/revert", response_model=RevertImportResponse)
def revert_imports(
    req: RevertImportRequest,
    spanner: SpannerClient = Depends(get_spanner_client)
):
    """Reverts import(s) to their previous version and resets status to STAGING in Spanner."""
    if not req.importName and not req.workflowId:
        raise HTTPException(
            status_code=400,
            detail="Either importName or workflowId must be provided."
        )

    items = []
    if req.importName:
        items = [req.importName]
    elif req.workflowId:
        items = spanner.get_imports_for_workflow(req.workflowId)

    results = rollback_helper.revert_imports(
        spanner, items, workflow_id=req.workflowId, dry_run=req.dryRun
    )

    any_reverted = any(r.get("reverted", False) for r in results)
    status = ResponseStatus.OK if (any_reverted or not results) else ResponseStatus.SKIPPED

    reverted_successful = [r for r in results if r.get("reverted", False)]
    if reverted_successful:
        summaries = [
            f"{r['importName']}: {r.get('restoredVersion')}"
            if not r.get('failedVersion')
            else f"{r['importName']}: {r.get('failedVersion')} -> {r.get('restoredVersion')}"
            for r in reverted_successful
        ]
        msg = f"Reverted {', '.join(summaries)}"
    else:
        errors = [r["error"] for r in results if "error" in r]
        msg = "; ".join(errors) if errors else "No imports provided to revert."

    reverted_items = [
        RevertImportResultItem(
            importName=r["importName"],
            latestVersion=r.get("latestVersion") or r.get("restoredVersion"),
            failedVersion=r.get("failedVersion"),
            restoredVersion=r.get("restoredVersion")
        ) for r in results
    ]

    return RevertImportResponse(
        status=status,
        message=msg,
        reverted=any_reverted,
        revertedImports=reverted_items,
        dryRun=req.dryRun
    )
