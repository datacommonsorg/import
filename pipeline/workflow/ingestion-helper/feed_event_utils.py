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

import base64
import json
import logging
import os
import croniter
from datetime import datetime, timezone
from google.cloud import storage
from google.cloud.workflows import executions_v1
import import_utils

logging.getLogger().setLevel(logging.INFO)

SPANNER_INGESTION_WORKFLOW_ID = 'spanner-ingestion-workflow'
IMPORT_AUTOMATION_WORKFLOW_ID = 'import-automation-workflow'


def invoke_spanner_ingestion_workflow(project_id: str, location: str, import_name: str):
    """Triggers the spanner ingestion workflow.

    Args:
        project_id: GCP project ID.
        location: Workflow location.
        import_name: The name of the import.
    """
    workflow_args = {"importList": [import_name.split(':')[-1]]}

    logging.info(f"Invoking {SPANNER_INGESTION_WORKFLOW_ID} for {import_name}")
    execution_client = executions_v1.ExecutionsClient()
    parent = f"projects/{project_id}/locations/{location}/workflows/{SPANNER_INGESTION_WORKFLOW_ID}"
    execution_req = executions_v1.Execution(argument=json.dumps(workflow_args))
    response = execution_client.create_execution(parent=parent,
                                                 execution=execution_req)
    logging.info(
        f"Triggered workflow {SPANNER_INGESTION_WORKFLOW_ID} for {import_name}. Execution ID: {response.name}"
    )


def invoke_import_automation_workflow(project_id: str,
                                      location: str,
                                      import_name: str,
                                      latest_version: str,
                                      import_size: str,
                                      graph_path: str,
                                      cron_schedule: str,
                                      run_ingestion: bool = False):
    """Triggers the import automation workflow.

    Args:
        project_id: GCP project ID.
        location: Workflow location.
        import_name: The name of the import.
        latest_version: The version of the import.
        import_size: The size of the import ('small', 'medium', 'large').
        graph_path: The graph path for the import.
        cron_schedule: The cron schedule for the import.
        run_ingestion: Whether to run the ingestion workflow after the import.
    """
    import_config = {
        "user_script_args": [f"--version={latest_version}"],
        "import_version_override": latest_version,
        "graph_data_path": graph_path,
        "cron_schedule_override": cron_schedule
    }
    workflow_args = {
        "importName": import_name,
        "importConfig": json.dumps(import_config),
        "runIngestion": run_ingestion
    }

    if import_size == 'large':
        workflow_args["resources"] = {
            "machine": "n2-highmem-16",
            "cpu": 16000,
            "memory": 131072,
            "disk": 100
        }

    logging.info(f"Invoking {IMPORT_AUTOMATION_WORKFLOW_ID} for {import_name}")
    execution_client = executions_v1.ExecutionsClient()
    parent = f"projects/{project_id}/locations/{location}/workflows/{IMPORT_AUTOMATION_WORKFLOW_ID}"
    execution_req = executions_v1.Execution(argument=json.dumps(workflow_args))
    response = execution_client.create_execution(parent=parent,
                                                 execution=execution_req)
    logging.info(
        f"Triggered workflow {IMPORT_AUTOMATION_WORKFLOW_ID} for {import_name}. Execution ID: {response.name}"
    )


def calculate_next_refresh(cron_schedule: str) -> str:
    """Calculates next refresh ISO timestamp from cron schedule.

    Args:
        cron_schedule: The cron schedule for the import.

    Returns:
        The next refresh timestamp as an ISO-8601 string, or current UTC timestamp if error.
    """
    if cron_schedule:
        try:
            return croniter.croniter(
                cron_schedule,
                datetime.now(timezone.utc)).get_next(datetime).isoformat()
        except Exception as e:
            logging.error(
                f"Error calculating next refresh from schedule '{cron_schedule}': {e}"
            )
    return datetime.now(timezone.utc).isoformat()


def check_duplicate(message_id: str, gcs_bucket_id: str) -> bool:
    """Checks for duplicate messages using a GCS file.

    Args:
        message_id: The ID of the message to check.
        gcs_bucket_id: The GCS bucket ID to check against.

    Returns:
        True if the message is a duplicate, False otherwise.
    """
    duplicate = False
    if not message_id:
        return duplicate
    logging.info(f"Checking for existing message: {message_id}")
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket_id)
    blob = bucket.blob(f"google3/transfers/{message_id}")
    try:
        blob.upload_from_string("", if_generation_match=0)
    except Exception:
        duplicate = True
    return duplicate


def parse_message(request_json: dict) -> dict:
    """Processes the incoming Pub/Sub message.

    Args:
        request_json: The request JSON dictionary.

    Returns:
        A dictionary containing the message data, or None if invalid.
    """
    if not request_json or 'message' not in request_json:
        logging.error('Invalid Pub/Sub message format')
        return None

    pubsub_message = request_json['message']
    logging.info(f"Received Pub/Sub message: {pubsub_message}")
    try:
        data_bytes = base64.b64decode(pubsub_message.get("data", ""))
        notification_json = data_bytes.decode("utf-8")
        logging.info(f"Notification content: {notification_json}")
    except Exception as e:
        logging.error(f"Error decoding message data: {e}")

    return pubsub_message


def handle_feed_event(request_json: dict,
                      spanner,
                      storage,
                      project_id: str,
                      location: str,
                      gcs_bucket_id: str,
                      is_base_dc: bool) -> tuple:
    """Handles GCS CDA Pub/Sub feed events and invokes workflows.

    Args:
        request_json: The incoming Pub/Sub JSON payload.
        spanner: Instantiated SpannerClient object.
        storage: Instantiated StorageClient object.
        project_id: GCP Project ID.
        location: Cloud location.
        gcs_bucket_id: The GCS bucket ID.
        is_base_dc: Flag indicating if this is base DC.

    Returns:
        A tuple of (response_message, HTTP_status_code).
    """
    message = parse_message(request_json)
    if not message:
        return 'Invalid Pub/Sub message format', 400

    attributes = message.get('attributes', {})
    message_id = message.get('messageId', '')
    if attributes.get('transfer_status') != 'TRANSFER_COMPLETED':
        return 'OK', 200

    duplicate = check_duplicate(message_id, gcs_bucket_id)
    if duplicate:
        logging.info(f"Message {message_id} already processed. Skipping.")
        return 'OK', 200

    import_name = attributes.get('import_name')
    latest_version = attributes.get(
        'import_version',
        datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    import_step = attributes.get('import_step', '')
    graph_path = attributes.get('graph_path', "/**/*.mcf*")
    import_size = attributes.get('import_size', '')
    cron_schedule = attributes.get('cron_schedule', '')

    if import_step in ('ingestion_workflow_single', 'ingestion_workflow_batch'):
        import_status = 'STAGING'
        job_id = attributes.get('feed_name', 'cda_feed')
        
        # Build request dict to update status internally
        latest_version_gcs = 'gs://' + gcs_bucket_id + '/' + import_name.replace(
            ':', '/') + '/' + latest_version
        update_request = {
            'importName': import_name,
            'status': import_status,
            'jobId': job_id,
            'latestVersion': latest_version_gcs,
            'graphPath': graph_path,
        }
        if cron_schedule:
            update_request['nextRefresh'] = calculate_next_refresh(cron_schedule)
            
        import_utils.handle_update_import_status(
            update_request, spanner, storage, is_base_dc, project_id, location
        )

        if import_step == 'ingestion_workflow_single':
            invoke_spanner_ingestion_workflow(project_id, location, import_name)
            
    elif import_step in ('import_automation_job', 'import_automation_e2e'):
        run_ingestion = True if import_step == 'import_automation_e2e' else False
        invoke_import_automation_workflow(project_id, location, import_name,
                                          latest_version, import_size,
                                          graph_path, cron_schedule, run_ingestion)
    else:
        logging.info("Skipping import post processing.")

    return 'OK', 200
