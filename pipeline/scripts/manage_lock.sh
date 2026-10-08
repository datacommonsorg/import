#!/bin/bash
#
# Queries or updates the global Spanner ingestion lock via the ingestion-helper service.
#
# Modes:
#   status   Returns the current lock status and owner workflow ID
#            (GET /database/lock/status).
#   release  Releases the lock held by <workflowId>
#            (POST /database/lock/release).
#   acquire  Force-assigns the lock to <workflowId>, even if another workflow
#            holds it (POST /database/lock/acquire with force=true). Use this
#            to hand the lock to a rerun workflow (e.g. after a previous
#            workflow failed or was cancelled); the waiting workflow picks it
#            up on its next poll.
#
# Usage:
#   ./pipeline/scripts/manage_lock.sh status <env: staging|prod>
#   ./pipeline/scripts/manage_lock.sh <release|acquire> <env: staging|prod> <workflowId>
#
# Examples:
#   ./pipeline/scripts/manage_lock.sh status staging
#   ./pipeline/scripts/manage_lock.sh release staging 12345678-1234-1234-1234-123456789abc
#   ./pipeline/scripts/manage_lock.sh acquire prod 12345678-1234-1234-1234-123456789abc
#

set -e

MODE="$1"
ENV="$2"
WORKFLOW_ID="$3"

usage() {
  echo "Usage:"
  echo "  $0 status <env: staging|prod>"
  echo "  $0 <release|acquire> <env: staging|prod> <workflowId>"
  echo "Examples:"
  echo "  $0 status staging"
  echo "  $0 release staging 12345678-1234-1234-1234-123456789abc"
  exit 1
}

if [ -z "$MODE" ] || [ -z "$ENV" ]; then
  usage
fi

if [ "${MODE,,}" != "status" ] && [ -z "$WORKFLOW_ID" ]; then
  usage
fi

case "${MODE,,}" in
  status)
    ENDPOINT="/database/lock/status"
    HTTP_METHOD="GET"
    ACTION="status"
    ;;
  release)
    ENDPOINT="/database/lock/release"
    HTTP_METHOD="POST"
    PAYLOAD="{\"workflowId\": \"${WORKFLOW_ID}\"}"
    ACTION="release"
    ;;
  acquire)
    ENDPOINT="/database/lock/acquire"
    HTTP_METHOD="POST"
    PAYLOAD="{\"workflowId\": \"${WORKFLOW_ID}\", \"force\": true}"
    ACTION="acquire"
    ;;
  *)
    echo "Unknown mode: '${MODE}'. Supported modes: status, release, acquire"
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

if [ "${ACTION}" = "status" ]; then
  echo "Fetching lock status in '${ENV}' via ${HELPER_URL}${ENDPOINT}..."
  HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -X GET "${HELPER_URL}${ENDPOINT}" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" || true)
else
  echo "Requesting lock ${ACTION} for workflow '${WORKFLOW_ID}' in '${ENV}' via ${HELPER_URL}..."
  HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -X "${HTTP_METHOD}" "${HELPER_URL}${ENDPOINT}" \
    -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
    -H "Content-Type: application/json" \
    -d "${PAYLOAD}" || true)
fi

HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed '$d')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tail -n 1)

echo "Response (${HTTP_STATUS}):"
echo "${HTTP_BODY}"

if [ -n "${HTTP_STATUS}" ] && [ "${HTTP_STATUS}" -ge 200 ] && [ "${HTTP_STATUS}" -lt 300 ]; then
  if [ "${ACTION}" != "status" ]; then
    echo "Successfully completed lock ${ACTION} for workflow '${WORKFLOW_ID}' in ${ENV}."
  fi
else
  echo "Error: Failed to ${ACTION} lock (HTTP ${HTTP_STATUS:-unknown})."
  exit 1
fi
