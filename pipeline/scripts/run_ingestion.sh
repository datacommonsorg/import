#!/bin/bash
#
# Triggers the Spanner ingestion workflow for a specific import.
# Optionally updates the import version and version history via ingestion-helper.
#
# Usage:
#   ./pipeline/scripts/run_ingestion.sh <importName> <env> [latestVersion]
#
# Example:
#   ./pipeline/scripts/run_ingestion.sh \
#     scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates_Test staging

set -e

IMPORT_NAME="$(echo "$1" | xargs)"
ENV="$(echo "$2" | xargs)"
LATEST_VERSION="$(echo "$3" | xargs)"
LATEST_VERSION="${LATEST_VERSION:-STAGING}"

if [ -z "$IMPORT_NAME" ] || [ -z "$ENV" ]; then
  echo "Usage: $0 <importName> <env: staging|prod> [latestVersion: default STAGING]"
  exit 1
fi

LOCATION="us-central1"
PROJECT_NUMBER="965988403328"

case "${ENV,,}" in
  staging)
    PROJECT="datcom-import-automation-prod"
    WORKFLOW="spanner-ingestion-workflow-staging"
    HELPER_SERVICE="ingestion-helper-service-staging"
    ;;
  prod|production)
    PROJECT="datcom-import-automation-prod"
    WORKFLOW="spanner-ingestion-workflow"
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

# Extract version directory name if a full GCS path or glob pattern was passed
IMPORT_PATH="${IMPORT_NAME//://}"
if [ "$LATEST_VERSION" = "STAGING" ]; then
  VERSION_NAME="STAGING"
elif [[ "$LATEST_VERSION" =~ $IMPORT_PATH/([^/]+) ]]; then
  VERSION_NAME="${BASH_REMATCH[1]}"
else
  CLEAN_VERSION="${LATEST_VERSION%%\**}"
  CLEAN_VERSION="${CLEAN_VERSION%/}"
  VERSION_NAME="$(basename "${CLEAN_VERSION}")"
fi

# Update import version and version history in Spanner/GCS
echo "Updating version history for ${IMPORT_NAME} to version ${VERSION_NAME} via ${HELPER_URL}..."

curl -X POST "${HELPER_URL}/imports/version" \
  -H "Authorization: bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d "{\"imports\": [\"${IMPORT_NAME}\"], \"version\": \"${VERSION_NAME}\", \"override\": true, \"comment\": \"Manual trigger via run_ingestion.sh\"}"
echo ""

# Build JSON payload
IMPORT_ITEM="{\"importName\":\"${CLEAN_IMPORT_NAME}\""
if [ "$LATEST_VERSION" != "STAGING" ]; then
  IMPORT_ITEM="${IMPORT_ITEM},\"latestVersion\":\"${LATEST_VERSION}\""
fi
IMPORT_ITEM="${IMPORT_ITEM}}"

DATA="{\"importList\":[${IMPORT_ITEM}]}"

echo "Triggering ${WORKFLOW} in ${ENV} (${PROJECT}/${LOCATION}) with payload: ${DATA}"

gcloud workflows run "${WORKFLOW}" \
  --project="${PROJECT}" \
  --location="${LOCATION}" \
  --data="${DATA}"

