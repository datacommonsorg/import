# Copyright 2026 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Trigger the Data Commons ingestion workflow.

Used by the Stats import runner to trigger the ingestion workflow after the Stats import is complete.
"""

import json
import logging
import os

import google.auth
import google.auth.transport.requests
import requests


def _get_env_vars():
  """Helper to get and validate required environment variables."""
  required_env_vars = [
      "GCP_SPANNER_INSTANCE_ID", "GCP_SPANNER_DATABASE_NAME", "PROJECT_ID",
      "WORKFLOW_LOCATION", "TEMP_LOCATION", "REGION", "INGESTION_WORKFLOW_NAME"
  ]
  missing_vars = [var for var in required_env_vars if not os.getenv(var)]
  if missing_vars:
    logging.error(
        f"Cannot trigger ingestion workflow. Missing environment variables: {', '.join(missing_vars)}"
    )
    logging.warning("Skipping auto-trigger of ingestion workflow.")
    return None
  return {var: os.getenv(var) for var in required_env_vars}


def trigger_ingestion_workflow(gcs_path: str = None,
                               import_name: str = "default_import_name",
                               import_list: list[dict] = None):
  """Triggers the Data Commons ingestion workflow via Google Cloud Workflows API."""
  logging.info("Attempting to auto-trigger ingestion workflow via API...")

  env_vars = _get_env_vars()
  if not env_vars:
    return

  if import_list is None:
    import_list = [{"importName": import_name, "graphPath": gcs_path}]

  data_payload = {
      "spannerInstanceId":
          env_vars["GCP_SPANNER_INSTANCE_ID"],
      "spannerDatabaseId":
          env_vars["GCP_SPANNER_DATABASE_NAME"],
      "importName":
          import_list[0]["importName"] if import_list else import_name,
      "importList":
          json.dumps(import_list),
      "tempLocation":
          env_vars["TEMP_LOCATION"],
      "region":
          env_vars["REGION"]
  }

  try:
    credentials, _ = google.auth.default()
    auth_request = google.auth.transport.requests.Request()
    credentials.refresh(auth_request)

    project_id = env_vars["PROJECT_ID"]
    location = env_vars["WORKFLOW_LOCATION"]
    workflow_name = env_vars["INGESTION_WORKFLOW_NAME"]
    url = f"https://workflowexecutions.googleapis.com/v1/projects/{project_id}/locations/{location}/workflows/{workflow_name}/executions"

    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json"
    }

    payload = {"argument": json.dumps(data_payload)}

    response = requests.post(url, json=payload, headers=headers, timeout=60)

    if response.status_code == 200:
      logging.info("Workflow triggered successfully!")
      logging.info(response.json())
    else:
      logging.error(
          f"Failed to trigger workflow. Status: {response.status_code}")
      logging.error(response.text)

  except Exception as e:
    logging.error(f"Error triggering workflow via API: {e}")
