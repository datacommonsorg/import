#!/bin/bash
#
# Triggers the Spanner ingestion workflow for a specific import via the
# ingestion-helper service (POST /imports/ingest).
#
# Usage:
#   ./pipeline/scripts/run_ingestion.sh <importName> <env> <latestVersion: full GCS path with wildcard> [--dry-run]
#
# With --dry-run, the helper resolves the import list but does not start the
# workflow.
#
# Example:
#   ./pipeline/scripts/run_ingestion.sh \
#     scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test staging \
#     'gs://datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates_Test/2025_12_17T02_30_27_233484_08_00/**/*.mcf*' \
#     --dry-run

set -e

IMPORT_NAME="$(echo "$1" | xargs)"
ENV="$(echo "$2" | xargs)"
LATEST_VERSION="$(echo "$3" | xargs)"
DRY_RUN=false

case "$4" in
  "") ;;
  --dry-run) DRY_RUN=true ;;
  *)
    echo "Unknown option: '$4'"
    exit 1
    ;;
esac

if [ -z "$IMPORT_NAME" ] || [ -z "$ENV" ] || [ -z "$LATEST_VERSION" ]; then
  echo "Usage: $0 <importName> <env: staging|prod> <latestVersion: full GCS path with wildcard> [--dry-run]"
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

DATA="{\"importList\":[{\"importName\":\"${CLEAN_IMPORT_NAME}\",\"latestVersion\":\"${LATEST_VERSION}\"}],\"forceIngestion\":true,\"dryRun\":${DRY_RUN}}"

echo "Requesting ingestion in ${ENV} via ${HELPER_URL}/imports/ingest with payload: ${DATA}"

HTTP_RESPONSE=$(curl -sS -w "\n%{http_code}" -X POST "${HELPER_URL}/imports/ingest" \
  -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d "${DATA}" || true)

HTTP_BODY=$(echo "${HTTP_RESPONSE}" | sed '$d')
HTTP_STATUS=$(echo "${HTTP_RESPONSE}" | tail -n 1)

echo "Response (${HTTP_STATUS}):"
echo "${HTTP_BODY}"

if [ -z "${HTTP_STATUS}" ] || [ "${HTTP_STATUS}" -lt 200 ] || [ "${HTTP_STATUS}" -ge 300 ]; then
  echo "Error: Failed to request ingestion (HTTP ${HTTP_STATUS:-unknown})."
  exit 1
fi
