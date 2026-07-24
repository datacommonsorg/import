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

"""Aggregation Helper Cloud Run Job execution entry point."""

import argparse
import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import List, Optional

from aggregation import AggregationOrchestrator, OrchestratorConfig


def parse_import_list(import_list_str: Optional[str]) -> List[str]:
    """Parses and validates the JSON string representing active imports."""
    if not import_list_str:
        logging.info("No --import_list provided. Proceeding with global import-independent calculations only.")
        return []

    parsed = json.loads(import_list_str)
    if not isinstance(parsed, list):
        raise ValueError("Parsed import_list is not a list")
    logging.info(f"Received active imports to process: {parsed}")
    return parsed


def create_orchestrator_config(
    args: argparse.Namespace, env: os._Environ = os.environ
) -> OrchestratorConfig:
    """Creates an OrchestratorConfig from CLI arguments and environment variables."""
    connection_id = env.get("BQ_SPANNER_CONN_ID")
    project_id = env.get("PROJECT_ID")
    instance_id = env.get("SPANNER_INSTANCE_ID")
    database_id = env.get("SPANNER_GRAPH_DATABASE_ID")
    location = env.get("LOCATION")

    if not connection_id or not project_id or not instance_id or not database_id:
        raise ValueError(
            f"Missing required environment variables. connection_id={connection_id}, "
            f"project_id={project_id}, instance_id={instance_id}, database_id (SPANNER_GRAPH_DATABASE_ID)={database_id}"
        )

    config_path = args.config_path or env.get("CONFIG_PATH")
    enable_embeddings = env.get("ENABLE_EMBEDDINGS", "false").lower() == "true"
    bq_dataset_id = env.get("BQ_DATASET_ID", "datacommons")

    return OrchestratorConfig(
        connection_id=connection_id,
        project_id=project_id,
        instance_id=instance_id,
        database_id=database_id,
        location=location,
        is_base_dc=args.is_base_dc,
        config_file_path=config_path,
        enable_embeddings=enable_embeddings,
        bq_dataset_id=bq_dataset_id,
        generate_stat_var_groups=args.generate_stat_var_groups,
    )


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Aggregation Helper Cloud Run Job...")

    parser = argparse.ArgumentParser(description="Run aggregation helper job.")
    parser.add_argument(
        "--import_list",
        help="JSON string representing the list of imports to process."
    )
    parser.add_argument(
        "--config_path",
        help="Optional path to a specific YAML config file or directory (e.g., aggregation/configs/embedding.yaml)."
    )
    parser.add_argument(
        "--generate_stat_var_groups",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether to auto-generate StatVarGroup hierarchy tree (default: True, use --no-generate_stat_var_groups to disable)."
    )
    parser.add_argument(
        "--dry_run",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run in dry-run mode without executing jobs (use --no-dry_run to execute)."
    )
    parser.add_argument(
        "--skip_deletions",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Skip deleting existing aggregated data before running new aggregations."
    )
    parser.add_argument(
        "--is_base_dc",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Whether running in base Data Commons environment (default: True, use --no-is_base_dc for DCP)."
    )

    args = parser.parse_args()

    try:
        import_list = parse_import_list(args.import_list)
        config = create_orchestrator_config(args)
    except (ValueError, json.JSONDecodeError) as e:
        logging.error(f"Failed to load job configuration: {e}")
        sys.exit(1)

    orchestrator = AggregationOrchestrator(config=config)

    logging.info(
        f"Executing AggregationOrchestrator pipeline (dry_run={args.dry_run}, skip_deletions={args.skip_deletions}) for imports: {import_list}"
    )
    run_result = orchestrator.run(
        active_imports=import_list,
        dry_run=args.dry_run,
        skip_deletions=args.skip_deletions
    )

    if not run_result.success:
        logging.error(
            f"Aggregation Helper Cloud Run Job finished with failures for import(s): {run_result.failed_imports}"
        )
        sys.exit(1)

    logging.info("Aggregation Helper Cloud Run Job completed successfully.")


if __name__ == "__main__":
    main()
