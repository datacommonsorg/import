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
from typing import Any, Dict, List, Optional

from .bq_executor import BigQueryExecutor
from .linked_edge_generator import LinkedEdgeGenerator
from .provenance_summary_generator import ProvenanceSummaryGenerator
from .stat_var_aggregator import StatVarAggregator
from .place_aggregation_generator import PlaceAggregationGenerator
from .stat_var_group_generator import StatVarGroupGenerator
from .validator import validate_config


class AggregationOrchestrator:
    """Orchestrates the overall aggregation workflow."""

    def __init__(self,
                 connection_id: str,
                 project_id: str,
                 instance_id: str,
                 database_id: str,
                 location: Optional[str] = None,
                 is_base_dc: bool = True,
                 config_file_path: Optional[str] = None) -> None:
        """Initializes the orchestrator and loads/validates the configuration.

        Args:
            connection_id: BigQuery connection ID to Spanner.
            project_id: GCP Project ID.
            instance_id: Spanner Instance ID.
            database_id: Spanner Database ID.
            location: BigQuery location.
            is_base_dc: Whether this is running in the base Data Commons environment.
            config_file_path: Optional custom path to the aggregation.yaml file.
                If not specified, defaults to the aggregation.yaml in the parent directory.
        """
        # Always run asynchronously at the executor level for stages to run in parallel
        # We handle sequential blocking between stages at the workflow/router level
        self.executor = BigQueryExecutor(connection_id=connection_id,
                                         project_id=project_id,
                                         instance_id=instance_id,
                                         database_id=database_id,
                                         location=location,
                                         run_sequential=False)

        # Initialize all generators
        self.place_generator = PlaceAggregationGenerator(self.executor, is_base_dc)
        self.stat_var_aggregator = StatVarAggregator(self.executor, is_base_dc)
        self.linked_edge_generator = LinkedEdgeGenerator(self.executor, is_base_dc)
        self.provenance_summary_generator = ProvenanceSummaryGenerator(self.executor, is_base_dc)
        self.stat_var_group_generator = StatVarGroupGenerator(self.executor, is_base_dc)

        # Resolve paths for default config and schema
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        if not config_file_path:
            config_file_path = os.path.join(curr_dir, "..", "aggregation.yaml")
        schema_file_path = os.path.join(curr_dir, "schema.json")

        # Load and validate configuration
        self.aggregations = validate_config(config_file_path, schema_file_path)



    def execute_stage(self, stage_num: int, active_imports: List[str]) -> List[str]:
        """Executes all enabled aggregations in the specified stage in parallel.

        Args:
            stage_num: The stage number to execute.
            active_imports: The list of active import names in this run.

        Returns:
            A list of BigQuery job IDs submitted for this stage.
        """
        logging.info(f"Executing Aggregation Stage {stage_num} for active imports: {active_imports}")
        jobs = []

        for config in self.aggregations:
            # 1. Skip if disabled
            if config.get("disabled", False):
                continue

            # 2. Filter by stage
            if config.get("stage", 1) != stage_num:
                continue

            # 3. Filter by active imports
            applicable_imports = self._get_applicable_imports(config, active_imports)
            if not applicable_imports:
                logging.info(f"Skipping step '{config['type']}' in Stage {stage_num}: no matching active imports.")
                continue

            # 4. Route to correct generator
            step_type = config["type"]
            logging.info(f"Submitting step '{step_type}' in Stage {stage_num} for imports: {applicable_imports}")
            
            step_jobs = []
            if step_type == "place":
                job = self.place_generator.aggregate_places(
                    import_names=applicable_imports,
                    source_type=config["source_type"],
                    destination_type=config["destination_type"],
                    allow_multiple_to_places=config.get("allow_multiple_to_places", False)
                )
                if job: step_jobs.append(job)

            elif step_type == "stat_var":
                sv_jobs = self.stat_var_aggregator.aggregate_stat_vars(
                    ancestor_sv=config["ancestor_sv_id"],
                    source_svs=config["source_sv_ids"],
                    import_names=applicable_imports,
                    output_import_name=config.get("output_import_name"),
                    skip_all_sources_present_check=config.get("skip_all_sources_present_check", False)
                )
                step_jobs.extend(sv_jobs)

            elif step_type == "linked_edges":
                step_jobs.extend(self.linked_edge_generator.run_all(applicable_imports))

            elif step_type == "provenance_summary":
                step_jobs.extend(self.provenance_summary_generator.run_all(applicable_imports))

            elif step_type == "stat_var_groups":
                step_jobs.extend(self.stat_var_group_generator.run_all(applicable_imports))
            else:
                raise ValueError(f"Unsupported or unimplemented aggregation step type: {step_type}")

            # Collect BQ jobs
            for job in step_jobs:
                if job and job.job_id:
                    jobs.append(job.job_id)

        logging.info(f"Submitted {len(jobs)} jobs in Stage {stage_num}: {jobs}")
        return jobs

    def has_stage(self, stage_num: int, active_imports: List[str]) -> bool:
        """Checks if there are any active, enabled aggregations configured for the stage.

        Args:
            stage_num: The stage number to check.
            active_imports: The list of active import names.

        Returns:
            True if the stage has at least one aggregation that will run, False otherwise.
        """
        for config in self.aggregations:
            if config.get("disabled", False):
                continue
            if config.get("stage", 1) != stage_num:
                continue
            
            # Check if it applies to any active imports
            if self._get_applicable_imports(config, active_imports):
                return True

        return False

    def get_active_stages(self, active_imports: List[str]) -> List[int]:
        """Returns a sorted list of unique, active, and enabled stage numbers.

        Args:
            active_imports: The list of active import names.

        Returns:
            A sorted list of unique active stage numbers.
        """
        stages = set()
        for config in self.aggregations:
            if config.get("disabled", False):
                continue
            # Check if it applies to any active imports
            if self._get_applicable_imports(config, active_imports):
                stages.add(config.get("stage", 1))
        return sorted(list(stages))

    def check_jobs_status(self, job_ids: List[str]) -> Dict[str, Any]:
        """Checks the status of the specified BigQuery job IDs.

        Delegates to the BigQueryExecutor's get_jobs_status.
        """
        try:
            return self.executor.get_jobs_status(job_ids)
        except Exception as e:
            logging.error(f"Failed to check jobs status: {e}")
            raise e

    def _get_applicable_imports(self, config: Dict[str, Any], active_imports: List[str]) -> List[str]:
        """Determines which active imports apply to this aggregation config."""
        configured_imports = config["imports"]
        
        # Explicit wildcard check
        if "*" in configured_imports:
            return active_imports
            
        # Intersection of configured and active imports
        return list(set(configured_imports).intersection(active_imports))
