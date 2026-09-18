#!/bin/bash
#
# Releases the global Spanner ingestion lock held by a specific workflow execution
# via the ingestion-helper service.
#
# Usage:
#   ./pipeline/scripts/release_lock.sh <workflowId> <env: staging|prod>
#
# Example:
#   ./pipeline/scripts/release_lock.sh 12345678-1234-1234-1234-123456789abc staging
#

set -e

ARG1="$1"
ARG2="$2"

if [ -z "$ARG1" ] || [ -z "$ARG2" ]; then
  echo "Usage: $0 <workflowId> <env: staging|prod>"
  echo "Example: $0 12345678-1234-1234-1234-123456789abc staging"
  exit 1
fi

# Support passing <workflowId> <env> (default) or <env> <workflowId>
case "${ARG1,,}" in
  staging|prod|production)
    ENV="$ARG1"
    WORKFLOW_ID="$ARG2"
    ;;
  *)
    WORKFLOW_ID="$ARG1"
    ENV="$ARG2"
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

echo "Releasing lock for workflow '${WORKFLOW_ID}' in '${ENV}' via ${HELPER_URL}..."

HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -X POST "${HELPER_URL}/database/lock/release" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d "{\"workflowId\": \"${WORKFLOW_ID}\"}" || true)

HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed '$d')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tail -n 1)

echo "Response (${HTTP_STATUS}):"
echo "${HTTP_BODY}"

if [ -n "${HTTP_STATUS}" ] && [ "${HTTP_STATUS}" -ge 200 ] && [ "${HTTP_STATUS}" -lt 300 ]; then
  echo "Successfully released lock for workflow '${WORKFLOW_ID}' in ${ENV}."
else
  echo "Error: Failed to release lock (HTTP ${HTTP_STATUS:-unknown})."
  exit 1
fi
