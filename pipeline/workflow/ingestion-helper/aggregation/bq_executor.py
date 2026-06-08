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
import time
from typing import Any, Dict, List, Optional

from google.cloud import bigquery


class BigQueryExecutor:
    """Handles BigQuery client initialization and query execution."""

    def __init__(self,
                 connection_id: str,
                 project_id: str,
                 instance_id: str,
                 database_id: str,
                 location: Optional[str] = None,
                 run_sequential: bool = True) -> None:
        """Initializes the BigQueryExecutor with connection and destination details."""
        self.connection_id = connection_id
        self.project_id = project_id
        self.instance_id = instance_id
        self.database_id = database_id
        self.location = location
        self.run_sequential = run_sequential
        self.client = bigquery.Client(project=self.project_id,
                                      location=self.location)

    def get_spanner_destination_uri(self) -> str:
        """Returns the Spanner destination URI for EXPORT DATA."""
        return f"https://spanner.googleapis.com/projects/{self.project_id}/instances/{self.instance_id}/databases/{self.database_id}"

    def execute(
        self,
        query: str,
        job_config: Optional[bigquery.QueryJobConfig] = None
    ) -> bigquery.job.QueryJob:
        """
        Submits a query and returns the QueryJob.

        If run_sequential is True, it blocks and waits for the query to complete
        before returning.
        """

        logging.info(
            f"Submitting query (first 100 chars): {query.strip()[:100]}...")

        start_time = time.time()
        try:
            query_job = self.client.query(query, job_config=job_config)
            logging.info(f"Query submitted. Job ID: {query_job.job_id}")

            if self.run_sequential:
                logging.info(
                    f"Waiting for Query Job {query_job.job_id} to complete (sequential mode)..."
                )
                query_job.result()
                duration = time.time() - start_time
                logging.info(
                    f"Query completed in {duration:.2f}s. Job ID: {query_job.job_id}"
                )

            return query_job
        except Exception as e:
            logging.error(f"Failed to submit or execute query: {e}")
            raise

    def get_jobs_status(self, job_ids: List[str]) -> Dict[str, Any]:
        """Returns the overall status of a list of BigQuery jobs."""

        overall_status = "DONE"
        failed_jobs = []
        error_message = ""

        for job_id in job_ids:
            try:
                job = self.client.get_job(job_id, location=self.location)
                if job.error_result:
                    overall_status = "FAILED"
                    failed_jobs.append(job_id)
                    error_message += f"Job {job_id} failed: {job.error_result}. "
                elif job.state != "DONE" and overall_status != "FAILED":
                    overall_status = "RUNNING"
            except Exception as e:
                logging.error(f"Failed to get job status for {job_id}: {e}")
                overall_status = "FAILED"
                failed_jobs.append(job_id)
                error_message += f"Failed to get job {job_id}: {e}. "

        if overall_status == "FAILED":
            return {
                "status": overall_status,
                "error": error_message,
                "failedJobs": failed_jobs
            }
        else:
            return {"status": overall_status}
