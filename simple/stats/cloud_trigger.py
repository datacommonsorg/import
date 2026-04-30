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

import json
import logging
import os
import requests
import google.auth
import google.auth.transport.requests

# Defaults for Cloud Workflow triggering
DEFAULT_SPANNER_INSTANCE = "gabe-test-dcp-instance"
DEFAULT_SPANNER_DATABASE = "gabe-test-dcp-db-v2"
DEFAULT_WORKFLOW_NAME = "gabe-test-ingestion-orchestrator"
DEFAULT_PROJECT_ID = "datcom-website-dev"
DEFAULT_LOCATION = "us-central1"
DEFAULT_TEMP_LOCATION = "gs://gabe-test-ingestion-bucket-datcom-website-dev/temp"
DEFAULT_REGION = "us-central1"

def trigger_ingestion_workflow(gcs_path: str):
  """Triggers the Data Commons ingestion workflow via Google Cloud Workflows API."""
  logging.info("Attempting to auto-trigger ingestion workflow via API...")
  
  # Read from environment variables with fallbacks
  spanner_instance = os.getenv("GCP_SPANNER_INSTANCE_ID", DEFAULT_SPANNER_INSTANCE)
  spanner_database = os.getenv("GCP_SPANNER_DATABASE_NAME", DEFAULT_SPANNER_DATABASE)
  workflow_name = os.getenv("WORKFLOW_NAME", DEFAULT_WORKFLOW_NAME)
  project_id = os.getenv("PROJECT_ID", DEFAULT_PROJECT_ID)
  location = os.getenv("WORKFLOW_LOCATION", DEFAULT_LOCATION)
  temp_location = os.getenv("TEMP_LOCATION", DEFAULT_TEMP_LOCATION)
  region = os.getenv("REGION", DEFAULT_REGION)
  
  data_payload = {
      "spannerInstanceId": spanner_instance,
      "spannerDatabaseId": spanner_database,
      "importName": "dcp_bridge_jsonld_sharded_test",
      "importList": json.dumps([{
          "importName": "JSONLD_Sharded_Import",
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
