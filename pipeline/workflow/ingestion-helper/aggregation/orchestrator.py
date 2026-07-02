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

"""Aggregation orchestrator for Data Commons ingestion workflow."""

import logging
import os
import time
from typing import Any, Dict, List, Optional

from .bq_executor import BigQueryExecutor
from .linked_edge_generator import LinkedEdgeGenerator
from .place_aggregation_generator import PlaceAggregationGenerator
from .provenance_summary_generator import ProvenanceSummaryGenerator
from .stat_var_aggregator import StatVarAggregator
from .stat_var_group_generator import StatVarGroupGenerator
from .validator import validate_config


class AggregationOrchestrator:
    """Orchestrates the overall aggregation workflow across multi-stage execution."""

    def __init__(
        self,
        connection_id: str,
        project_id: str,
        instance_id: str,
        database_id: str,
        location: Optional[str] = None,
        is_base_dc: bool = True,
        config_dir: Optional[str] = None,
        config_file_path: Optional[str] = None
    ) -> None:
        """Initializes the orchestrator and loads/validates configuration files.

        Args:
            connection_id: BigQuery connection ID to Spanner.
            project_id: GCP Project ID.
            instance_id: Spanner Instance ID.
            database_id: Spanner Database ID.
            location: BigQuery location.
            is_base_dc: Whether this is running in the base Data Commons environment.
            config_dir: Directory containing aggregation YAML configs (default: configs/).
            config_file_path: Optional path to single config file or directory.
        """
        self.executor = BigQueryExecutor(
            connection_id=connection_id,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            location=location,
            run_sequential=False
        )
        self.is_base_dc = is_base_dc

        # Resolve paths for config directory and schema
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        target_config = config_dir or config_file_path or os.path.join(curr_dir, "configs")
        schema_file_path = os.path.join(curr_dir, "schema.json")

        # Load and validate configuration
        self.calculations = validate_config(target_config, schema_file_path)

    def run(self, active_imports: List[str]) -> None:
        """Executes aggregations independently for each active import.

        Blocks and synchronizes stage progression for each import:
        Stage 1 -> Wait -> Stage 2 -> Wait -> Stage 3 -> Wait.

        Args:
            active_imports: List of active import dataset names to process.
        """
        logging.info(f"Starting Aggregation Orchestrator run for active imports: {active_imports}")

        for single_import in active_imports:
            logging.info(f"=== Starting Aggregation Pipeline for Import: '{single_import}' ===")
            active_stages = self.get_active_stages_for_import(single_import)

            if not active_stages:
                logging.info(f"No aggregation steps configured for import '{single_import}'. Skipping.")
                continue

            for stage_num in active_stages:
                logging.info(f"--- Triggering Stage {stage_num} for import '{single_import}' ---")
                self._execute_and_synchronize_stage(single_import, stage_num)

            logging.info(f"=== Successfully completed all aggregation stages for Import: '{single_import}' ===")

    def get_active_stages_for_import(self, single_import: str) -> List[int]:
        """Returns a sorted list of unique active stage numbers for a single import.

        Args:
            single_import: The active import dataset name.

        Returns:
            Sorted list of stage numbers (e.g., [1, 2, 3]).
        """
        stages = set()
        for calc in self.calculations:
            if self._calc_applies_to_import(calc, single_import):
                stages.add(calc.get("stage", 1))
        return sorted(list(stages))

    def get_active_stages(self, active_imports: List[str]) -> List[int]:
        """Returns a sorted list of unique active stage numbers across active imports."""
        stages = set()
        for single_import in active_imports:
            stages.update(self.get_active_stages_for_import(single_import))
        return sorted(list(stages))

    def execute_stage(self, stage_num: int, active_imports: List[str]) -> List[str]:
        """Executes and collects BigQuery job IDs for a given stage.

        Args:
            stage_num: The stage number to execute.
            active_imports: List of active import dataset names.

        Returns:
            List of BigQuery job IDs submitted.
        """
        job_ids = []
        for single_import in active_imports:
            jobs = self._dispatch_stage_steps(single_import, stage_num)
            for job in jobs:
                if job and getattr(job, "job_id", None):
                    job_ids.append(job.job_id)
        return job_ids

    def _execute_and_synchronize_stage(self, single_import: str, stage_num: int) -> None:
        """Triggers stage steps for a single import and blocks until completion."""
        jobs = self._dispatch_stage_steps(single_import, stage_num)
        job_ids = [job.job_id for job in jobs if job and getattr(job, "job_id", None)]

        if not job_ids:
            logging.info(f"No BigQuery jobs submitted for Stage {stage_num} (import: '{single_import}').")
            return

        logging.info(f"Submitted {len(job_ids)} job(s) for Stage {stage_num} (import: '{single_import}'): {job_ids}")
        self._wait_for_jobs(job_ids)

    def _dispatch_stage_steps(self, single_import: str, stage_num: int) -> List[Any]:
        """Dispatches matching calculation steps for an import and stage number."""
        jobs = []
        for calc in self.calculations:
            if calc.get("stage", 1) != stage_num:
                continue
            if not self._calc_applies_to_import(calc, single_import):
                continue

            step_type = calc["type"]
            logging.info(f"Triggering '{step_type}' (Stage {stage_num}) for import '{single_import}'...")

            step_jobs = []
            if step_type == "PLACE_AGGREGATION":
                step_jobs = self._trigger_place(calc, [single_import])
            elif step_type == "STAT_VAR_AGGREGATION":
                step_jobs = self._trigger_stat_var(calc, [single_import])
            elif step_type == "LINKED_EDGES":
                step_jobs = self._trigger_linked_edges(calc, [single_import])
            elif step_type == "PROVENANCE_SUMMARY":
                step_jobs = self._trigger_provenance_summary(calc, [single_import])
            elif step_type == "STAT_VAR_GROUPS":
                step_jobs = self._trigger_stat_var_groups(calc, [single_import])
            else:
                logging.warning(
                    f"Calculation type '{step_type}' configured for import '{single_import}' has no active generator handler."
                )

            jobs.extend(step_jobs)
        return jobs

    def _wait_for_jobs(self, job_ids: List[str], poll_interval: int = 5) -> None:
        """Blocks until all specified BigQuery job IDs complete successfully.

        Args:
            job_ids: List of BigQuery job IDs to wait for.
            poll_interval: Seconds between polling checks.

        Raises:
            RuntimeError: If any job fails.
        """
        if not job_ids:
            return

        logging.info(f"Waiting for {len(job_ids)} BigQuery job(s) to complete: {job_ids}")
        while True:
            status_info = self.executor.get_jobs_status(job_ids)
            status = status_info.get("status")

            if status == "DONE":
                logging.info("All BigQuery jobs in stage completed successfully.")
                return
            elif status == "FAILED":
                error_msg = status_info.get("error", "One or more BigQuery jobs failed.")
                logging.error(f"Stage execution failed: {error_msg}")
                raise RuntimeError(f"Aggregation execution failed: {error_msg}")

            time.sleep(poll_interval)

    def _trigger_place(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers place-level rollup aggregations."""
        place_cfg = config.get("place_aggregation", {})
        from_type = place_cfg.get("from_place_types") or config.get("source_type")
        to_type = place_cfg.get("to_place_types") or config.get("destination_type")

        logging.info(f"  -> Place Rollup: {from_type} -> {to_type} for imports {applicable_imports}")
        generator = PlaceAggregationGenerator(self.executor, self.is_base_dc)
        job = generator.aggregate_places(
            import_names=applicable_imports,
            source_type=from_type,
            destination_type=to_type,
            allow_multiple_to_places=place_cfg.get("allow_multiple_to_places", False)
        )
        return [job] if job else []

    def _trigger_stat_var(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers statistical variable aggregations."""
        stat_cfg = config.get("stat_var_aggregation", {})
        aggregations = stat_cfg.get("aggregations", [])
        output_import_name = config.get("output_import") or config.get("output_import_name")

        # Backwards compatibility fallback for single item config
        if not aggregations and "ancestor_sv_id" in config:
            aggregations = [{
                "ancestor_sv_id": config["ancestor_sv_id"],
                "source_sv_ids": config["source_sv_ids"],
                "skip_all_sources_present_check": config.get("skip_all_sources_present_check", False)
            }]

        generator = StatVarAggregator(self.executor, self.is_base_dc)
        jobs = []

        for item in aggregations:
            ancestor_sv = item["ancestor_sv_id"]
            source_svs = item["source_sv_ids"]
            logging.info(
                f"  -> Stat Var Aggregation: ancestor '{ancestor_sv}' (sources: {source_svs}) for imports {applicable_imports}"
            )
            item_jobs = generator.aggregate_stat_vars(
                ancestor_sv=ancestor_sv,
                source_svs=source_svs,
                import_names=applicable_imports,
                output_import_name=output_import_name,
                skip_all_sources_present_check=item.get("skip_all_sources_present_check", False)
            )
            jobs.extend(item_jobs)

        return jobs

    def _trigger_linked_edges(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers linked edge aggregations."""
        logging.info(f"  -> Linked Edges Aggregation for imports {applicable_imports}")
        generator = LinkedEdgeGenerator(self.executor, self.is_base_dc)
        return generator.run_all(applicable_imports)

    def _trigger_provenance_summary(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers provenance summary aggregations."""
        logging.info(f"  -> Provenance Summary Aggregation for imports {applicable_imports}")
        generator = ProvenanceSummaryGenerator(self.executor, self.is_base_dc)
        return generator.run_all(applicable_imports)

    def _trigger_stat_var_groups(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers statistical variable group aggregations."""
        logging.info(f"  -> Stat Var Groups Aggregation for imports {applicable_imports}")
        generator = StatVarGroupGenerator(self.executor, self.is_base_dc)
        return generator.run_all(applicable_imports)

    def _calc_applies_to_import(self, calc: Dict[str, Any], single_import: str) -> bool:
        """Determines if a calculation step applies to a single import."""
        if calc.get("disabled", False):
            return False

        configured_imports = calc.get("input_imports") or calc.get("imports", [])
        if "*" in configured_imports or single_import in configured_imports:
            return True

        return False
