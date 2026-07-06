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

from aggregation import AggregationOrchestrator


def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Aggregation Helper Cloud Run Job...")

    parser = argparse.ArgumentParser(description="Run aggregation helper job.")
    parser.add_argument(
        "--import_list",
        help="JSON string representing the list of imports to process."
    )
    parser.add_argument(
        "--dry_run",
        action="store_true",
        default=True,
        help="Run in dry-run mode without executing jobs (default: True)."
    )
    parser.add_argument(
        "--execute",
        action="store_false",
        dest="dry_run",
        help="Disable dry-run mode and execute BigQuery aggregation jobs."
    )

    args = parser.parse_args()

    if not args.import_list:
        logging.error("Missing required argument: --import_list")
        sys.exit(1)

    try:
        import_list = json.loads(args.import_list)
        logging.info(f"Received active imports to process: {import_list}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse import_list JSON: {e}")
        sys.exit(1)

    if not isinstance(import_list, list):
        logging.error("Parsed import_list is not a list")
        sys.exit(1)

    connection_id = os.environ.get("BQ_SPANNER_CONN_ID")
    project_id = os.environ.get("PROJECT_ID")
    instance_id = os.environ.get("SPANNER_INSTANCE_ID")
    database_id = os.environ.get("SPANNER_GRAPH_DATABASE_ID") or os.environ.get("SPANNER_DATABASE_ID")
    location = os.environ.get("LOCATION")

    if not all([connection_id, project_id, instance_id, database_id]):
        logging.error(
            f"Missing required environment variables. connection_id={connection_id}, "
            f"project_id={project_id}, instance_id={instance_id}, database_id={database_id}"
        )
        sys.exit(1)

    orchestrator = AggregationOrchestrator(
        connection_id=connection_id,
        project_id=project_id,
        instance_id=instance_id,
        database_id=database_id,
        location=location,
    )

    logging.info(f"Executing AggregationOrchestrator pipeline (dry_run={args.dry_run}) for imports: {import_list}")
    orchestrator.run(active_imports=import_list, dry_run=args.dry_run)
    logging.info("Aggregation Helper Cloud Run Job completed successfully.")

if __name__ == "__main__":
    main()
