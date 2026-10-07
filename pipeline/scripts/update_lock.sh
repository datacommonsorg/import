#!/bin/bash
#
# Updates the global Spanner ingestion lock via the ingestion-helper service.
#
# Modes:
#   release  Releases the lock held by <workflowId>
#            (POST /database/lock/release).
#   acquire  Force-assigns the lock to <workflowId>, even if another workflow
#            holds it (POST /database/lock/acquire with force=true). Use this
#            to hand the lock to a rerun workflow (e.g. after a previous
#            workflow failed or was cancelled); the waiting workflow picks it
#            up on its next poll.
#
# Usage:
#   ./pipeline/scripts/update_lock.sh <release|acquire> <workflowId> <env: staging|prod>
#
# Examples:
#   ./pipeline/scripts/update_lock.sh release 12345678-1234-1234-1234-123456789abc staging
#   ./pipeline/scripts/update_lock.sh acquire 12345678-1234-1234-1234-123456789abc prod
#

set -e

MODE="$1"
ARG2="$2"
ARG3="$3"

usage() {
  echo "Usage: $0 <release|acquire> <workflowId> <env: staging|prod>"
  echo "Example: $0 release 12345678-1234-1234-1234-123456789abc staging"
  exit 1
}

if [ -z "$MODE" ] || [ -z "$ARG2" ] || [ -z "$ARG3" ]; then
  usage
fi

# Support passing <workflowId> <env> (default) or <env> <workflowId>
case "${ARG2,,}" in
  staging|prod|production)
    ENV="$ARG2"
    WORKFLOW_ID="$ARG3"
    ;;
  *)
    WORKFLOW_ID="$ARG2"
    ENV="$ARG3"
    ;;
esac

case "${MODE,,}" in
  release)
    ENDPOINT="/database/lock/release"
    PAYLOAD="{\"workflowId\": \"${WORKFLOW_ID}\"}"
    ACTION="release"
    ;;
  acquire)
    ENDPOINT="/database/lock/acquire"
    PAYLOAD="{\"workflowId\": \"${WORKFLOW_ID}\", \"force\": true}"
    ACTION="acquire"
    ;;
  *)
    echo "Unknown mode: '${MODE}'. Supported modes: release, acquire"
    usage
    ;;
esac

LOCATION="us-central1"
PROJECT_NUMBER="965988403328"

case "${ENV,,}" in
  staging)
    HELPER_SERVICE="ingestion-helper-service-staging"
    ;;
  prod|production)
    HELPER_SERVICE="ingestion-helper-service"
    ;;
  *)
    echo "Unknown environment: '${ENV}'. Supported environments: staging, prod"
    exit 1
    ;;
esac

HELPER_URL="https://${HELPER_SERVICE}-${PROJECT_NUMBER}.${LOCATION}.run.app"

echo "Requesting lock ${ACTION} for workflow '${WORKFLOW_ID}' in '${ENV}' via ${HELPER_URL}..."

HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -X POST "${HELPER_URL}${ENDPOINT}" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d "${PAYLOAD}" || true)

HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed '$d')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tail -n 1)

echo "Response (${HTTP_STATUS}):"
echo "${HTTP_BODY}"

if [ -n "${HTTP_STATUS}" ] && [ "${HTTP_STATUS}" -ge 200 ] && [ "${HTTP_STATUS}" -lt 300 ]; then
  echo "Successfully completed lock ${ACTION} for workflow '${WORKFLOW_ID}' in ${ENV}."
else
  echo "Error: Failed to ${ACTION} lock (HTTP ${HTTP_STATUS:-unknown})."
  exit 1
fi
