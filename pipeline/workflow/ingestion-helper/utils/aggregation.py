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
from typing import Any, Dict, List, Optional

from aggregation import BigQueryExecutor
from aggregation import LinkedEdgeGenerator
from aggregation import ProvenanceSummaryGenerator
from aggregation import StatVarAggregator
from google.cloud import bigquery

logging.getLogger().setLevel(logging.INFO)


class AggregationUtils:
    """Orchestrates the overall aggregation workflow."""

    def __init__(self,
                 connection_id: str,
                 project_id: str,
                 instance_id: str,
                 database_id: str,
                 location: Optional[str] = None,
                 is_base_dc: bool = True) -> None:
        # TODO: remove sequential execution once DCP changes are made
        # Use sequential execution for DCP (backward compatibility)
        run_sequential = not is_base_dc
        self.executor = BigQueryExecutor(connection_id=connection_id,
                                         project_id=project_id,
                                         instance_id=instance_id,
                                         database_id=database_id,
                                         location=location,
                                         run_sequential=run_sequential)
        self.linked_edge_generator = LinkedEdgeGenerator(
            self.executor, is_base_dc)
        self.provenance_summary_generator = ProvenanceSummaryGenerator(
            self.executor, is_base_dc)
        self.stat_var_aggregator = StatVarAggregator(self.executor)

    def run_aggregation(self, import_list: List[Dict[str, Any]]) -> List[str]:
        """
        Orchestrates standard per-import aggregations and global aggregations.
        Returns a list of BigQuery job IDs for async polling.
        """
        logging.info(f"Received request for importList: {import_list}")

        try:
            import_names = []
            # 1. Run standard per-import aggregations
            for import_item in import_list:
                import_name = import_item.get('importName')
                if import_name:
                    import_names.append(import_name)
                    query = "SELECT @import_name as import_name, CURRENT_TIMESTAMP() as execution_time"
                    job_config = bigquery.QueryJobConfig(query_parameters=[
                        bigquery.ScalarQueryParameter("import_name", "STRING",
                                                      import_name),
                    ])
                    self.executor.execute(query, job_config=job_config)
                else:
                    logging.info(
                        'Skipping aggregation logic for empty importName')

            # 2. Run global aggregations asynchronously
            jobs = []
            jobs.extend(self.linked_edge_generator.run_all(import_names))
            jobs.extend(self.provenance_summary_generator.run_all(import_names))

            # 3. Run StatVar aggregations (Example: Health Insurance rollup)
            # In production, this would be driven by parsing the config manifest.
            # This aggregates uninsured populations (with/without disability) into a total.
            jobs.extend(self.stat_var_aggregator.aggregate_stat_vars(
                ancestor_sv="Count_Person_NoHealthInsurance",
                source_svs=[
                    "dc/y0dvhk0sggzef", "dc/kdg05h55y45y6", "dc/9drszqwd2nef7", # No Disability
                    "dc/bew8kj6l7tv93", "dc/96dqj47csvmy8", "dc/qr4s77egv27q2"  # With Disability
                ],
                import_names=import_names
            ))

            job_ids = [job.job_id for job in jobs if job]
            logging.info(f"Submitted async aggregation jobs: {job_ids}")

            return job_ids
        except Exception as e:
            logging.error(f"Aggregation failed: {e}")
            raise e

    def check_aggregation_status(self, job_ids: List[str]) -> Dict[str, Any]:
        """
        Checks the status of the provided BigQuery job IDs.
        """
        logging.info(f"Checking status for jobs: {job_ids}")
        try:
            return self.executor.get_jobs_status(job_ids)
        except Exception as e:
            logging.error(f"Failed to check aggregation status: {e}")
            raise e
