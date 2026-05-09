# Copyright 2024 Google Inc.
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
This is a utility function to trigger the Data Commons ingestion workflow.
It is used by the Stats import runner to trigger the ingestion workflow after the Stats import is complete.
"""

import json
import logging
import os
import requests
import google.auth
import google.auth.transport.requests

def trigger_ingestion_workflow(gcs_path: str, import_name: str = "default_import_name"):
  """Triggers the Data Commons ingestion workflow via Google Cloud Workflows API."""
  logging.info("Attempting to auto-trigger ingestion workflow via API...")
  required_env_vars = [
      "GCP_SPANNER_INSTANCE_ID",
      "GCP_SPANNER_DATABASE_NAME",
      "PROJECT_ID",
      "WORKFLOW_LOCATION",
      "TEMP_LOCATION",
      "REGION",
      "INGESTION_WORKFLOW_NAME"
  ]
  
  missing_vars = [var for var in required_env_vars if not os.getenv(var)]
  
  if missing_vars:
    logging.error(f"Cannot trigger ingestion workflow. Missing environment variables: {', '.join(missing_vars)}")
    logging.warning("Skipping auto-trigger of ingestion workflow.")
    return
  
  spanner_instance = os.getenv("GCP_SPANNER_INSTANCE_ID")
  spanner_database = os.getenv("GCP_SPANNER_DATABASE_NAME")
  workflow_name = os.getenv("INGESTION_WORKFLOW_NAME")
  project_id = os.getenv("PROJECT_ID")
  location = os.getenv("WORKFLOW_LOCATION")
  temp_location = os.getenv("TEMP_LOCATION")
  region = os.getenv("REGION")
  
  data_payload = {
      "spannerInstanceId": spanner_instance,
      "spannerDatabaseId": spanner_database,
      "importName": import_name,
      "importList": json.dumps([{
          "importName": import_name,
          "graphPath": gcs_path
      }]),
      "tempLocation": temp_location,
      "region": region
  }
  
  try:
    # Get credentials from the environment (Service Account attached to Cloud Run)
    credentials, _ = google.auth.default()
    auth_request = google.auth.transport.requests.Request()
    credentials.refresh(auth_request)
    
    url = f"https://workflowexecutions.googleapis.com/v1/projects/{project_id}/locations/{location}/workflows/{workflow_name}/executions"
    
    headers = {
        "Authorization": f"Bearer {credentials.token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "argument": json.dumps(data_payload)
    }
    
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code == 200:
      logging.info("Workflow triggered successfully!")
      logging.info(response.json())
    else:
      logging.error(f"Failed to trigger workflow. Status: {response.status_code}")
      logging.error(response.text)
        
  except Exception as e:
    logging.error(f"Error triggering workflow via API: {e}")
