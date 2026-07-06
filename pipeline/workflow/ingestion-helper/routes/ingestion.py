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
import os
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, ConfigDict, Field
from pydantic.alias_generators import to_camel
import requests
import google.auth
from google.auth.transport.requests import AuthorizedSession

from clients.storage import StorageClient
from dependencies import get_storage_client
from routes.models import BaseResponse, ResponseStatus
from utils.logging import log_start

router = APIRouter(prefix="/ingestion", tags=["ingestion"])


class StartIngestionRequest(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )
    job_name: Optional[str] = Field(
        default=None,
        description=(
            "Cloud Run job resource name to start. If not provided, it is "
            "retrieved from the INGESTION_JOB_NAME environment variable."
        )
    )
    input_directory: Optional[str] = Field(
        default=None,
        description=(
            "The GCS path/directory where config.json resides. If not provided, "
            "it is auto-discovered from the job configuration."
        )
    )
    gcs_bucket: Optional[str] = Field(
        default=None,
        description=(
            "The GCS bucket override. If not specified, GCS_BUCKET_ID env is used."
        )
    )
    imports: Optional[str] = Field(
        default=None,
        description="Optional filter string for --imports argument"
    )
    force: Optional[bool] = Field(
        default=False,
        description="Bypass validation warnings"
    )


class StartIngestionResponse(BaseResponse):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )
    operation_name: Optional[str] = Field(
        default=None,
        description="The Cloud Run operation name representing the running job"
    )


class JobConfigRequest(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        from_attributes=True,
    )
    job_name: Optional[str] = Field(
        default=None,
        description=(
            "Cloud Run job resource name. If not provided, it is retrieved "
            "from the INGESTION_JOB_NAME environment variable."
        )
    )


class JobConfigResponse(BaseResponse):
    config: list[dict] = Field(
        default=[],
        description="List of environment variables configured on the job"
    )


def is_emulator() -> bool:
    """Checks if the service is running in emulator mode."""
    return bool(
        os.environ.get("STORAGE_EMULATOR_HOST") or
        os.environ.get("SPANNER_EMULATOR_HOST")
    )


def _resolve_job_name(job_name_in: Optional[str]) -> str:
    """Resolves the full Cloud Run job name resource."""
    job_name = job_name_in or os.environ.get("INGESTION_JOB_NAME")
    if not job_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "Cloud Run job name is missing. Please configure "
                "INGESTION_JOB_NAME or pass jobName in the request."
            )
        )

    if not job_name.startswith("projects/") and not is_emulator():
        project_id = os.environ.get("PROJECT_ID")
        location = os.environ.get("LOCATION")
        if not project_id or not location:
            raise HTTPException(
                status_code=500,
                detail=(
                    "PROJECT_ID or LOCATION environment variable is missing on "
                    "the helper service, cannot resolve short job name."
                )
            )
        return f"projects/{project_id}/locations/{location}/jobs/{job_name}"
    return job_name


def _auto_discover_input_dir(full_job_name: str) -> Optional[str]:
    """Queries Cloud Run configuration to extract GCS input directory folder."""
    try:
        logging.info(
            f"Auto-discovering input directory from Cloud Run job: {full_job_name}"
        )
        base_credentials, _ = google.auth.default()
        session = AuthorizedSession(base_credentials)
        job_resp = session.get(
            f"https://run.googleapis.com/v2/{full_job_name}",
            timeout=10
        )
        if job_resp.status_code == 200:
            job_data = job_resp.json()
            containers = (
                job_data.get("template", {})
                .get("template", {})
                .get("containers", [])
            )
            if containers:
                env_vars = containers[0].get("env", [])
                for env_var in env_vars:
                    if env_var.get("name") == "GCS_INPUT_FOLDER":
                        return env_var.get("value")
                    elif env_var.get("name") == "INPUT_DIR":
                        val = env_var.get("value")
                        if val:
                            parts = val.removeprefix("gs://").split("/", 1)
                            if len(parts) > 1:
                                return parts[1]
        else:
            logging.warning(
                "Failed to fetch Cloud Run job details for auto-discovery "
                f"(HTTP {job_resp.status_code}): {job_resp.text}"
            )
    except Exception as e:
        logging.warning(
            "Failed to auto-discover input directory from job config: "
            f"{e}. Falling back to default."
        )
    return None


def _resolve_gcs_input(
    req: StartIngestionRequest,
    storage: StorageClient,
    full_job_name: str
) -> tuple[str, str]:
    """Resolves and validates the GCS bucket and input directory path."""
    bucket_name = req.gcs_bucket or os.environ.get("GCS_BUCKET_ID")
    if not bucket_name:
        raise HTTPException(
            status_code=400,
            detail=(
                "GCS Bucket name is missing. Please configure GCS_BUCKET_ID "
                "or pass gcsBucket in the request."
            )
        )

    if not storage.check_bucket_exists(bucket_name):
        raise HTTPException(
            status_code=400,
            detail=f"GCS bucket '{bucket_name}' does not exist or is not accessible."
        )

    input_dir = req.input_directory
    if not input_dir and not is_emulator():
        input_dir = _auto_discover_input_dir(full_job_name)

    if not input_dir:
        input_dir = "ingestion/input"

    config_path = f"{input_dir.strip('/')}/config.json"
    if not storage.check_file_exists(bucket_name, config_path):
        raise HTTPException(
            status_code=400,
            detail=(
                f"Required config file 'config.json' not found or not accessible "
                f"at gs://{bucket_name}/{config_path}"
            )
        )

    return bucket_name, input_dir


def _validate_api_key(api_key: str, force: bool) -> None:
    """Validates the Data Commons API Key against api.datacommons.org."""
    if api_key in ["dummy-key-for-test", "YOUR_API_KEY_HERE", "dummy-key"]:
        if not force:
            raise HTTPException(
                status_code=400,
                detail=(
                    "The Data Commons API Key is set to a dummy value. Please "
                    "provide your actual API key, or pass 'force=True' in the "
                    "request to proceed anyway."
                )
            )
        logging.warning("Proceeding with dummy API key as force=True was set.")
    else:
        logging.info("Validating Data Commons API Key against api.datacommons.org...")
        try:
            val_resp = requests.post(
                "https://api.datacommons.org/v2/node",
                json={"nodes": ["country/USA"], "property": "->name"},
                headers={"x-api-key": api_key},
                timeout=5,
            )
            if val_resp.status_code in (401, 403):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Data Commons API Key was rejected by api.datacommons.org "
                        f"(HTTP {val_resp.status_code}). Please verify your API key."
                    )
                )
            elif val_resp.status_code != 200:
                logging.warning(
                    "Key validation returned unexpected status: "
                    f"{val_resp.status_code}. Skipping validation."
                )
        except requests.RequestException as e:
            logging.warning(
                "Could not connect to api.datacommons.org to validate key: "
                f"{e}. Skipping validation."
            )


def _check_active_executions(
    session: AuthorizedSession,
    full_job_name: str
) -> None:
    """Checks if there are any active executions for the Cloud Run job."""
    executions_url = f"https://run.googleapis.com/v2/{full_job_name}/executions"
    try:
        logging.info(f"Querying active executions for Cloud Run job: {full_job_name}")
        exec_resp = session.get(executions_url, timeout=30)
        if exec_resp.status_code == 200:
            executions_data = exec_resp.json()
            for execution in executions_data.get("executions", []):
                if "completionTime" not in execution:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "An ingestion job execution is already running: "
                            f"{execution.get('name')}"
                        )
                    )
        else:
            logging.warning(
                "Failed to query active executions "
                f"(HTTP {exec_resp.status_code}): {exec_resp.text}"
            )
    except requests.RequestException as e:
        logging.warning(
            f"Failed to check running executions: {e}. Proceeding anyway."
        )


def _trigger_cloud_run_job(
    session: AuthorizedSession,
    full_job_name: str,
    bucket_name: str,
    input_dir: str,
    imports: Optional[str]
) -> str:
    """Triggers job execution via Cloud Run Admin API and returns operation name."""
    run_url = f"https://run.googleapis.com/v2/{full_job_name}:run"
    args = ["--mode=dcpbridge"]
    if imports:
        args.append(f"--imports={imports}")
    args.append(f"--input_directory=gs://{bucket_name}/{input_dir}")

    json_payload = {"overrides": {"containerOverrides": [{"args": args}]}}
    try:
        logging.info(f"Triggering execution of Cloud Run job: {full_job_name}")
        run_resp = session.post(run_url, json=json_payload, timeout=30)
        run_resp.raise_for_status()
        result_data = run_resp.json()
        return (
            result_data.get("name") or
            result_data.get("metadata", {}).get("name")
        )
    except Exception as e:
        logging.error(f"Failed to trigger Cloud Run job execution: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to trigger Cloud Run job execution: {str(e)}"
        )


@router.post("/start", response_model=StartIngestionResponse)
@log_start
def start_ingestion(
    req: StartIngestionRequest,
    storage: StorageClient = Depends(get_storage_client)
):
    """Validates parameters, checks pre-conditions, and triggers the Cloud Run job."""
    full_job_name = _resolve_job_name(req.job_name)
    bucket_name, input_dir = _resolve_gcs_input(req, storage, full_job_name)

    api_key = (
        os.environ.get("DC_API_KEY") or
        os.environ.get("AUTH_GOOGLE_DATACOMMONS_API_KEY")
    )
    if not api_key:
        raise HTTPException(
            status_code=400,
            detail=(
                "Data Commons API Key is missing. Please configure "
                "DC_API_KEY in the environment."
            )
        )
    _validate_api_key(api_key, bool(req.force))

    if is_emulator():
        logging.info(
            "Running in emulator mode. Stubbing Cloud Run job execution."
        )
        return StartIngestionResponse(
            status=ResponseStatus.SUBMITTED,
            message="Ingestion job submitted successfully (stubbed in emulator mode).",
            operation_name=(
                "projects/test-project/locations/us-central1/"
                "operations/mock-operation"
            )
        )

    try:
        base_credentials, _ = google.auth.default()
        session = AuthorizedSession(base_credentials)
    except Exception as e:
        logging.error(f"Failed to load GCP credentials: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to load GCP credentials: {e}"
        )

    _check_active_executions(session, full_job_name)
    operation_name = _trigger_cloud_run_job(
        session, full_job_name, bucket_name, input_dir, req.imports
    )

    return StartIngestionResponse(
        status=ResponseStatus.SUBMITTED,
        message="Ingestion job submitted successfully.",
        operation_name=operation_name
    )


def _get_cloud_run_job_config(full_job_name: str) -> list[dict]:
    """Queries GCP Cloud Run API for the job details."""
    try:
        base_credentials, _ = google.auth.default()
        session = AuthorizedSession(base_credentials)
        job_resp = session.get(
            f"https://run.googleapis.com/v2/{full_job_name}",
            timeout=30
        )
        job_resp.raise_for_status()
        job_data = job_resp.json()
    except Exception as e:
        logging.error(f"Failed to fetch Cloud Run job details: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to fetch Cloud Run job details: {str(e)}"
        )

    containers = (
        job_data.get("template", {})
        .get("template", {})
        .get("containers", [])
    )
    if not containers:
        return []
    return containers[0].get("env", [])


@router.post("/config", response_model=JobConfigResponse)
@log_start
def get_job_config(req: JobConfigRequest):
    """Retrieves the configuration (environment variables) of the Cloud Run ingestion job."""
    job_name = _resolve_job_name(req.job_name)

    if is_emulator():
        logging.info(
            "Running in emulator mode. Returning mock environment variables."
        )
        return JobConfigResponse(
            status=ResponseStatus.SUCCESS,
            message=(
                "Successfully retrieved job configuration "
                "(stubbed in emulator mode)."
            ),
            config=[
                {"name": "DATA_RUN_MODE", "value": "ingest"},
                {"name": "GCS_BUCKET", "value": "mock-bucket"},
                {"name": "INPUT_DIR", "value": "gs://mock-bucket/ingestion/input"},
            ]
        )

    job_env = _get_cloud_run_job_config(job_name)
    if not job_env:
        return JobConfigResponse(
            status=ResponseStatus.SUCCESS,
            message="No containers found in job configuration.",
            config=[]
        )

    return JobConfigResponse(
        status=ResponseStatus.SUCCESS,
        message="Successfully retrieved job configuration.",
        config=job_env
    )
