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

import glob
import logging
import os
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional

from .bq_executor import BigQueryExecutor
from .linked_edge_generator import LinkedEdgeGenerator
from .place_aggregation_generator import PlaceAggregationGenerator
from .provenance_summary_generator import ProvenanceSummaryGenerator
from .stat_var_aggregator import StatVarAggregator
from .stat_var_calculation_generator import StatVarCalculationGenerator
from .stat_var_group_generator import StatVarGroupGenerator
from .stat_var_series_aggregator import StatVarSeriesAggregator
from .entity_aggregation_generator import EntityAggregationGenerator, EntityAggregationConfig
from .super_enum_aggregation_generator import SuperEnumAggregationGenerator
from .validator import validate_config


@dataclass
class ImportExecutionResult:
    """Execution status and metadata for a single import dataset."""
    import_name: str
    success: bool
    stages_executed: List[int] = field(default_factory=list)
    error_message: Optional[str] = None


@dataclass
class AggregationRunResult:
    """Summary result of an overall AggregationOrchestrator run across imports."""
    import_results: Dict[str, ImportExecutionResult] = field(default_factory=dict)

    @property
    def success(self) -> bool:
        """Returns True if all processed imports succeeded."""
        return all(res.success for res in self.import_results.values())

    @property
    def failed_imports(self) -> List[str]:
        """Returns the list of import names that failed."""
        return [name for name, res in self.import_results.items() if not res.success]


class CalculationType(str, Enum):
    """Supported aggregation calculation step types."""
    PLACE_AGGREGATION = "PLACE_AGGREGATION"
    STAT_VAR_AGGREGATION = "STAT_VAR_AGGREGATION"
    ENTITY_AGGREGATION = "ENTITY_AGGREGATION"
    STAT_VAR_CALCULATION = "STAT_VAR_CALCULATION"
    LINKED_EDGES = "LINKED_EDGES"
    PROVENANCE_SUMMARY = "PROVENANCE_SUMMARY"
    STAT_VAR_GROUPS = "STAT_VAR_GROUPS"
    STAT_VAR_SERIES_AGGREGATION = "STAT_VAR_SERIES_AGGREGATION"
    SUPER_ENUM_AGGREGATION = "SUPER_ENUM_AGGREGATION"


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
        config_file_path: Optional[str] = None,
        run_sequential: bool = False,
        poll_interval: int = 15
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
            run_sequential: Whether to run queries sequentially (default: False).
            poll_interval: Polling interval in seconds when waiting for jobs (default: 15).
        """
        self.executor = BigQueryExecutor(
            connection_id=connection_id,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            location=location,
            run_sequential=run_sequential
        )
        self.is_base_dc = is_base_dc
        self.poll_interval = poll_interval

        # Resolve paths for config directory and schema
        curr_dir = os.path.dirname(os.path.abspath(__file__))
        target_config = config_dir or config_file_path or os.path.join(curr_dir, "configs")
        schema_file_path = os.path.join(curr_dir, "schema.json")

        # self.calculations holds the complete list of validated calculation step
        # specification dictionaries loaded from YAML configuration files (e.g. place.yaml).
        self.calculations: List[Dict[str, Any]] = []
        if os.path.isdir(target_config):
            yaml_files = sorted(glob.glob(os.path.join(target_config, "*.yaml")))
            for file_path in yaml_files:
                self.calculations.extend(validate_config(file_path, schema_file_path))
        else:
            self.calculations = validate_config(target_config, schema_file_path)

    def run(self, active_imports: List[str], dry_run: bool = True) -> AggregationRunResult:
        """Executes aggregations independently for each active import.

        Blocks and synchronizes stage progression for each import:
        Stage 1 -> Wait -> Stage 2 -> Wait -> Stage 3 -> Wait.

        Args:
            active_imports: List of active import dataset names to process.
            dry_run: If True, logs imports and active stages without executing BigQuery jobs.

        Returns:
            AggregationRunResult containing status per import.
        """
        expanded_imports = self._expand_active_imports(active_imports)
        logging.info(
            f"Starting Aggregation Orchestrator run (dry_run={dry_run}) for active imports: {active_imports} (expanded: {expanded_imports})"
        )
        run_result = AggregationRunResult()

        for single_import in expanded_imports:
            logging.info(f"=== Starting Aggregation Pipeline for Import: '{single_import}' ===")
            active_stages = self._get_active_stages_for_import(single_import)

            if not active_stages:
                logging.info(f"No aggregation steps configured for import '{single_import}'. Skipping.")
                run_result.import_results[single_import] = ImportExecutionResult(
                    import_name=single_import,
                    success=True,
                    stages_executed=[]
                )
                continue

            if dry_run:
                logging.info(
                    f"Detected active stage(s) {active_stages} for import '{single_import}'. Skipping execution because dry_run=True."
                )
                run_result.import_results[single_import] = ImportExecutionResult(
                    import_name=single_import,
                    success=True,
                    stages_executed=active_stages
                )
                continue

            try:
                for stage_num in active_stages:
                    logging.info(f"--- Triggering Stage {stage_num} for import '{single_import}' ---")
                    self._execute_and_synchronize_stage(single_import, stage_num)

                logging.info(f"=== Successfully completed all aggregation stages for Import: '{single_import}' ===")
                run_result.import_results[single_import] = ImportExecutionResult(
                    import_name=single_import,
                    success=True,
                    stages_executed=active_stages
                )
            except Exception as e:
                logging.error(f"Aggregation pipeline failed for import '{single_import}': {e}")
                run_result.import_results[single_import] = ImportExecutionResult(
                    import_name=single_import,
                    success=False,
                    stages_executed=active_stages,
                    error_message=str(e)
                )

        return run_result

    def _get_active_stages_for_import(self, single_import: str) -> List[int]:
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

    def _expand_active_imports(self, active_imports: List[str]) -> List[str]:
        """Expands the list of active imports to include chained aggregated imports.

        For example, if 'A' is active, and there is a calculation:
          input_imports: ['A']
          output_import: 'B'
        Then 'B' should also be added to the active imports.
        This process is repeated transitively.
        """
        expanded = list(active_imports)
        queue = list(active_imports)

        while queue:
            current_import = queue.pop(0)
            for calc in self.calculations:
                if self._calc_applies_to_import(calc, current_import):
                    output = calc.get("output_import")
                    if output and output not in expanded:
                        queue.append(output)
                        expanded.append(output)
                        
        return expanded


    def get_active_stages(self, active_imports: List[str]) -> List[int]:
        """Returns a sorted list of unique active stage numbers across active imports."""
        expanded_imports = self._expand_active_imports(active_imports)
        stages = set()
        for single_import in expanded_imports:
            stages.update(self._get_active_stages_for_import(single_import))
        return sorted(list(stages))

    def execute_stage(self, stage_num: int, active_imports: List[str]) -> List[str]:
        """Executes a single stage for all active imports asynchronously."""
        expanded_imports = self._expand_active_imports(active_imports)
        stage_jobs = []
        for calc in self.calculations:
            calc_stage = calc.get("stage", 1)
            if calc_stage != stage_num:
                continue

            applicable_imports = [imp for imp in expanded_imports if self._calc_applies_to_import(calc, imp)]
            if not applicable_imports:
                continue

            step_type = calc.get("type")
            logging.info(
                f"Triggering step: '{step_type}' (Stage {stage_num}) for imports {applicable_imports}..."
            )
            step_jobs = self._dispatch_stage_steps(calc, applicable_imports)
            stage_jobs.extend(step_jobs)

        return stage_jobs

    def _execute_and_synchronize_stage(self, single_import: str, stage_num: int) -> None:
        """Executes a single stage for a single import and blocks until all jobs complete."""
        stage_jobs = []
        for calc in self.calculations:
            calc_stage = calc.get("stage", 1)
            if calc_stage != stage_num:
                continue

            if not self._calc_applies_to_import(calc, single_import):
                continue

            step_type = calc.get("type")
            logging.info(
                f"Triggering step: '{step_type}' (Stage {stage_num}) for import '{single_import}'..."
            )
            step_jobs = self._dispatch_stage_steps(calc, [single_import])

            if step_jobs:
                job_ids = [job.job_id for job in step_jobs if hasattr(job, "job_id")]
                logging.info(f"Submitted {len(job_ids)} job(s) for Stage {stage_num} (import: '{single_import}'): {job_ids}")
                self._wait_for_jobs(
                    job_ids=job_ids,
                    poll_interval=self.poll_interval,
                    step_name=f"{step_type} (Stage {stage_num})",
                    single_import=single_import
                )

    def _dispatch_stage_steps(self, calc: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Dispatches job execution based on step calculation type."""
        step_type = calc.get("type")

        if step_type == CalculationType.PLACE_AGGREGATION:
            return self._trigger_place(calc, applicable_imports)
        elif step_type == CalculationType.STAT_VAR_AGGREGATION:
            return self._trigger_stat_var(calc, applicable_imports)
        elif step_type == CalculationType.ENTITY_AGGREGATION:
            return self._trigger_entity(calc, applicable_imports)
        elif step_type == CalculationType.STAT_VAR_CALCULATION:
            return self._trigger_stat_var_calculation(calc, applicable_imports)
        elif step_type == CalculationType.LINKED_EDGES:
            return self._trigger_linked_edges(calc, applicable_imports)
        elif step_type == CalculationType.PROVENANCE_SUMMARY:
            return self._trigger_provenance_summary(calc, applicable_imports)
        elif step_type == CalculationType.STAT_VAR_GROUPS:
            return self._trigger_stat_var_groups(calc, applicable_imports)
        elif step_type == CalculationType.STAT_VAR_SERIES_AGGREGATION:
            return self._trigger_stat_var_series_aggregation(calc, applicable_imports)
        elif step_type == CalculationType.SUPER_ENUM_AGGREGATION:
            return self._trigger_super_enum_aggregation(calc, applicable_imports)
        else:
            logging.warning(
                f"Calculation type '{step_type}' configured for imports '{applicable_imports}' has no active generator handler."
            )
            return []

    def _wait_for_jobs(
        self,
        job_ids: List[str],
        poll_interval: int = 60,
        step_name: str = "Aggregation",
        single_import: str = ""
    ) -> None:
        """Blocks until all specified BigQuery job IDs complete successfully.

        Args:
            job_ids: List of BigQuery job IDs to wait for.
            poll_interval: Seconds between polling checks (default: 60s).
            step_name: Name of the step for debug logging.
            single_import: Name of the import dataset for debug logging.

        Raises:
            RuntimeError: If any job fails.
        """
        if not job_ids:
            return

        logging.info(
            f"Waiting for {len(job_ids)} BigQuery job(s) for step '{step_name}' (import: '{single_import}'): {job_ids}"
        )
        elapsed = 0
        while True:
            status_info = self.executor.get_jobs_status(job_ids)
            status = status_info.get("status")

            if status == "DONE":
                logging.info(
                    f"All BigQuery jobs for step '{step_name}' (import: '{single_import}') completed successfully in {elapsed}s."
                )
                return
            elif status == "FAILED":
                error_msg = status_info.get("error", "One or more BigQuery jobs failed.")
                logging.error(f"Step '{step_name}' (import: '{single_import}') execution failed: {error_msg}")
                raise RuntimeError(f"Aggregation execution failed for step '{step_name}': {error_msg}")

            time.sleep(poll_interval)
            elapsed += poll_interval
            logging.info(
                f"Waiting for {len(job_ids)} BigQuery job(s) for step '{step_name}' (import: '{single_import}') - elapsed: {elapsed}s..."
            )

    def _trigger_place(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers place-level rollup aggregations."""
        place_cfg = config.get("place_aggregation", {})
        from_type = place_cfg["from_place_types"]
        to_type = place_cfg["to_place_types"]

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
        output_import_name = config.get("output_import")

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

    def _trigger_stat_var_calculation(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers statistical variable calculations."""
        calc_cfg = config.get("stat_var_calculation", {})
        calculations = calc_cfg.get("calculations", [])
        output_import_name = config.get("output_import")

        logging.info(f"  -> Stat Var Calculation for imports {applicable_imports}")
        generator = StatVarCalculationGenerator(self.executor, self.is_base_dc)
        return generator.calculate_stat_vars(
            calculations=calculations,
            import_names=applicable_imports,
            output_import_name=output_import_name
        )

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

    def _trigger_stat_var_series_aggregation(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers statistical variable series aggregations."""
        logging.info(f"  -> Stat Var Series Aggregation for imports {applicable_imports}")
        calc = config.copy()
        calc["input_imports"] = applicable_imports
        generator = StatVarSeriesAggregator(self.executor, self.is_base_dc)
        return generator.aggregate_series([calc])

    def _trigger_entity(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers entity aggregations."""
        entity_cfg = config.get("entity_aggregation", {})
        output_import = config.get("output_import", "")

        cfg = EntityAggregationConfig(
            entity_types=entity_cfg.get("entity_types", []),
            location_props=entity_cfg.get("location_props", []),
            date_prop=entity_cfg.get("date_prop", ""),
            agg_date_formats=entity_cfg.get("agg_date_formats", []),
            constraints=entity_cfg.get("constraints", []),
            output_import=output_import,
            input_imports=applicable_imports
        )

        logging.info(f"  -> Entity Aggregation for imports {applicable_imports}")
        generator = EntityAggregationGenerator(self.executor, self.is_base_dc)
        return generator.aggregate_entities([cfg])

    def _trigger_super_enum_aggregation(self, config: Dict[str, Any], applicable_imports: List[str]) -> List[Any]:
        """Triggers super enum aggregations."""
        logging.info(f"  -> Super Enum Aggregation for imports {applicable_imports}")
        generator = SuperEnumAggregationGenerator(self.executor, self.is_base_dc)
        return generator.run(applicable_imports)

    def _calc_applies_to_import(self, calc: Dict[str, Any], single_import: str) -> bool:
        """Determines if a calculation step applies to a single import."""
        if calc.get("disabled", False):
            return False

        configured_imports = calc.get("input_imports") or calc.get("imports", [])
        if "*" in configured_imports or single_import in configured_imports:
            return True

        return False
