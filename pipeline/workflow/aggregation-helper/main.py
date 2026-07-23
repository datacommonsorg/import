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


@dataclass
class CloudRunJobConfig:
    """Configuration for the Aggregation Helper Cloud Run job invocation."""
    import_list: List[str]
    config_path: Optional[str]
    dry_run: bool
    skip_deletions: bool
    is_base_dc: bool
    connection_id: str
    project_id: str
    instance_id: str
    database_id: str
    location: Optional[str]
    enable_embeddings: bool
    bq_dataset_id: str

    @classmethod
    def from_args_and_env(cls, args: argparse.Namespace, env: os._Environ = os.environ) -> "CloudRunJobConfig":
        """Constructs a CloudRunJobConfig from parsed command line arguments and environment variables."""
        import_list = []
        if args.import_list:
            import_list = json.loads(args.import_list)
            if not isinstance(import_list, list):
                raise ValueError("Parsed import_list is not a list")
            logging.info(f"Received active imports to process: {import_list}")
        else:
            logging.info("No --import_list provided. Proceeding with global import-independent calculations only.")

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

        return cls(
            import_list=import_list,
            config_path=config_path,
            dry_run=args.dry_run,
            skip_deletions=args.skip_deletions,
            is_base_dc=args.is_base_dc,
            connection_id=connection_id,
            project_id=project_id,
            instance_id=instance_id,
            database_id=database_id,
            location=location,
            enable_embeddings=enable_embeddings,
            bq_dataset_id=bq_dataset_id,
        )

    def to_orchestrator_config(self) -> OrchestratorConfig:
        """Converts this run configuration into an OrchestratorConfig."""
        return OrchestratorConfig(
            connection_id=self.connection_id,
            project_id=self.project_id,
            instance_id=self.instance_id,
            database_id=self.database_id,
            location=self.location,
            is_base_dc=self.is_base_dc,
            config_file_path=self.config_path,
            enable_embeddings=self.enable_embeddings,
            bq_dataset_id=self.bq_dataset_id,
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
        job_config = CloudRunJobConfig.from_args_and_env(args)
    except (ValueError, json.JSONDecodeError) as e:
        logging.error(f"Failed to parse job configuration: {e}")
        sys.exit(1)

    orchestrator = AggregationOrchestrator(config=job_config.to_orchestrator_config())

    logging.info(
        f"Executing AggregationOrchestrator pipeline (dry_run={job_config.dry_run}, skip_deletions={job_config.skip_deletions}) for imports: {job_config.import_list}"
    )
    run_result = orchestrator.run(
        active_imports=job_config.import_list,
        dry_run=job_config.dry_run,
        skip_deletions=job_config.skip_deletions
    )

    if not run_result.success:
        logging.error(
            f"Aggregation Helper Cloud Run Job finished with failures for import(s): {run_result.failed_imports}"
        )
        sys.exit(1)

    logging.info("Aggregation Helper Cloud Run Job completed successfully.")


if __name__ == "__main__":
    main()
