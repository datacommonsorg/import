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
"""Utility functions for ingestion helper."""

from datetime import datetime
import logging
import re
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


def get_ingestion_metrics(project_id: str, location: str, job_id: str) -> dict:
    """Fetches graph metrics (nodes, edges, observations) and execution time from a Dataflow job.

    Args:
        project_id: The GCP project ID.
        location: The location of the Dataflow job.
        job_id: The Dataflow job ID.

    Returns:
        A dictionary containing 'obs_count', 'node_count', 'edge_count', 'ts_count', 'execution_time',
        and 'import_metrics'.
    """
    dataflow = build('dataflow', 'v1b3', cache_discovery=False)
    node_count = 0
    edge_count = 0
    obs_count = 0
    ts_count = 0
    execution_time = 0
    import_metrics = {}
    if project_id and job_id:
        try:
            job = dataflow.projects().locations().jobs().get(
                projectId=project_id, location=location,
                jobId=job_id).execute()

            start_time_str = job.get('startTime')
            current_state_time_str = job.get('currentStateTime')

            if start_time_str and current_state_time_str:
                start_time = datetime.fromisoformat(
                    start_time_str.replace('Z', '+00:00'))
                end_time = datetime.fromisoformat(
                    current_state_time_str.replace('Z', '+00:00'))
                execution_time = int((end_time - start_time).total_seconds())

            metrics = dataflow.projects().locations().jobs().getMetrics(
                projectId=project_id, location=location,
                jobId=job_id).execute()
            for metric in metrics.get('metrics', []):
                name = metric['name']['name']
                scalar = int(metric.get('scalar', 0))
                match = re.match(r"^([^:]+)(?::(.+))?$", name)
                metric_type = match.group(1) if match else name
                imp_name = match.group(2).split(':')[-1] if match and match.group(2) else None

                if imp_name and imp_name not in import_metrics:
                    import_metrics[imp_name] = {
                        'node_count': 0,
                        'edge_count': 0,
                        'obs_count': 0,
                        'ts_count': 0,
                    }

                if metric_type == 'node_count':
                    node_count += scalar
                    if imp_name:
                        import_metrics[imp_name]['node_count'] += scalar
                elif metric_type == 'edge_count':
                    edge_count += scalar
                    if imp_name:
                        import_metrics[imp_name]['edge_count'] += scalar
                elif metric_type == 'observation_count':
                    obs_count += scalar
                    if imp_name:
                        import_metrics[imp_name]['obs_count'] += scalar
                elif metric_type == 'timeseries_count':
                    ts_count += scalar
                    if imp_name:
                        import_metrics[imp_name]['ts_count'] += scalar
        except HttpError as e:
            logging.error(f"Error fetching dataflow metrics for job {job_id}: {e}")
    return {
        'obs_count': obs_count,
        'node_count': node_count,
        'edge_count': edge_count,
        'ts_count': ts_count,
        'execution_time': execution_time,
        'import_metrics': import_metrics,
    }
