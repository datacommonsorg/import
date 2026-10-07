#!/bin/bash
#
# Fetches the last SUCCESSFUL version for a specific import via the
# ingestion-helper service (GET /imports/version).
#
# Usage:
#   ./pipeline/scripts/get_version.sh <importName> <env>
#
# Example:
#   ./pipeline/scripts/get_version.sh \
#     USFed_ConstantMaturityRates_Test staging

set -e

IMPORT_NAME="$(echo "$1" | xargs)"
ENV="$(echo "$2" | xargs)"

if [ -z "$IMPORT_NAME" ] || [ -z "$ENV" ]; then
  echo "Usage: $0 <importName> <env: staging|prod>"
  exit 1
fi

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

# Clean import name if passed with script path prefix (e.g. scripts/foo:Bar -> Bar)
CLEAN_IMPORT_NAME="${IMPORT_NAME##*:}"

echo "Fetching last successful version for '${CLEAN_IMPORT_NAME}' in ${ENV} via ${HELPER_URL}/imports/version..."

HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -G "${HELPER_URL}/imports/version" \
  --data-urlencode "importName=${CLEAN_IMPORT_NAME}" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" || true)

HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed '$d')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tail -n 1)

echo "Response (${HTTP_STATUS}):"
echo "${HTTP_BODY}"

if [ -z "${HTTP_STATUS}" ] || [ "${HTTP_STATUS}" -lt 200 ] || [ "${HTTP_STATUS}" -ge 300 ]; then
  echo "Error: Failed to fetch import version (HTTP ${HTTP_STATUS:-unknown})."
  exit 1
fi
