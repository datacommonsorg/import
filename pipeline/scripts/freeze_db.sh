#!/bin/bash
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

# Freezes, unfreezes, or gets the current Mixer Spanner stale read timestamp by:
#   1. Acquiring / releasing global_ingestion_lock in IngestionLock
#   2. Inserting / removing a dummy row in IngestionHistory
#   3. Querying the effective StalenessTimestamp from IngestionHistory
#
# Usage:
#   ./pipeline/scripts/freeze_db.sh freeze <env: staging|prod> <timestamp>
#   ./pipeline/scripts/freeze_db.sh unfreeze <env: staging|prod>
#   ./pipeline/scripts/freeze_db.sh get <env: staging|prod>

set -euo pipefail

ACTION="${1:-}"
ENV="${2:-}"
RAW_TIMESTAMP="${3:-}"

usage() {
  cat <<EOF
Usage:
  $0 freeze <env: staging|prod> <timestamp>
  $0 unfreeze <env: staging|prod>
  $0 get <env: staging|prod>

Examples:
  $0 freeze prod "2026-09-23T08:30:00Z"
  $0 freeze prod "2026-09-23 14:00:00 IST"
  $0 unfreeze prod
  $0 get prod
EOF
  exit 1
}

if [[ -z "${ACTION}" ]] || [[ -z "${ENV}" ]]; then
  usage
fi

PROJECT="datcom-store"
DATABASE="dc_graph"
FREEZE_ID="manual-read-freeze"

case "${ENV,,}" in
  staging)
    INSTANCE="dc-graph-staging"
    ;;
  prod|production)
    INSTANCE="dc-graph-prod"
    ;;
  *)
    echo "Error: Unknown environment '${ENV}'. Supported: staging, prod"
    exit 1
    ;;
esac

run_sql() {
  local sql="$1"
  gcloud spanner databases execute-sql "${DATABASE}" \
    --instance="${INSTANCE}" \
    --project="${PROJECT}" \
    --sql="${sql}"
}

get_staleness_timestamp() {
  run_sql "SELECT MIN(CreationTimestamp) AS StalenessTimestamp FROM IngestionHistory WHERE (SELECT MAX(CompletionTimestamp) FROM IngestionHistory WHERE Status = 'SUCCESS') IS NULL OR CreationTimestamp > (SELECT MAX(CompletionTimestamp) FROM IngestionHistory WHERE Status = 'SUCCESS');"
}

case "${ACTION,,}" in
  get)
    get_staleness_timestamp
    ;;

  freeze)
    if [[ -z "${RAW_TIMESTAMP}" ]]; then
      echo "Error: <timestamp> is required for 'freeze'."
      usage
    fi

    if ! UTC_TIMESTAMP=$(date -u -d "${RAW_TIMESTAMP}" +"%Y-%m-%dT%H:%M:%SZ" 2>/dev/null); then
      echo "Error: Failed to parse timestamp '${RAW_TIMESTAMP}'."
      exit 1
    fi

    echo "Acquiring global_ingestion_lock in IngestionLock..."
    run_sql "INSERT OR UPDATE INTO IngestionLock (LockID, LockOwner, AcquiredTimestamp) VALUES ('global_ingestion_lock', '${FREEZE_ID}', PENDING_COMMIT_TIMESTAMP());"

    echo "Inserting dummy row into IngestionHistory at ${UTC_TIMESTAMP}..."
    run_sql "INSERT OR UPDATE INTO IngestionHistory (WorkflowExecutionID, CreationTimestamp, CompletionTimestamp, IngestionFailure, Status, Stage) VALUES ('${FREEZE_ID}', TIMESTAMP '${UTC_TIMESTAMP}', NULL, FALSE, 'RUNNING', 'manual_freeze');"

    echo "Done. Current stale read timestamp:"
    get_staleness_timestamp
    ;;

  unfreeze)
    echo "Removing dummy row from IngestionHistory..."
    run_sql "DELETE FROM IngestionHistory WHERE WorkflowExecutionID LIKE '${FREEZE_ID}%' OR Stage = 'manual_freeze';"

    echo "Releasing global_ingestion_lock in IngestionLock..."
    run_sql "UPDATE IngestionLock SET LockOwner = NULL, AcquiredTimestamp = NULL WHERE LockID = 'global_ingestion_lock' AND LockOwner LIKE '${FREEZE_ID}%';"

    echo "Done. Current stale read timestamp:"
    get_staleness_timestamp
    ;;

  *)
    echo "Error: Unknown action '${ACTION}'. Expected: freeze, unfreeze, or get."
    usage
    ;;
esac
