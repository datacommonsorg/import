#!/bin/bash

OPERATION=$1

if [ "$OPERATION" == "deploy" ]; then
    echo "Deploying Dataflow Flex Template..."
    # Find the JAR file using wildcard
    JAR_FILE=$(ls target/ingestion-bundled-*.jar | head -n 1)
    echo "Found JAR: ${JAR_FILE}"

    gcloud dataflow flex-template build \
        "gs://datcom-templates/templates/flex/ingestion.json" \
        --image-gcr-path "gcr.io/datcom-ci/dataflow-templates/ingestion:latest" \
        --sdk-language "JAVA" \
        --flex-template-base-image JAVA17 \
        --metadata-file "metadata.json" \
        --jar "${JAR_FILE}" \
        --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.datacommons.ingestion.pipeline.ImportGroupPipeline"
elif [ "$OPERATION" == "run" ]; then
    echo "Running Dataflow Flex Template..."
    gcloud dataflow flex-template run "ingestion-job" \
        --template-file-gcs-location "gs://datcom-templates/templates/flex/ingestion.json" \
        --parameters ^~^importList='[{"importName":"scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates","latestVersion":"datcom-prod-imports/scripts/us_fed/treasury_constant_maturity_rates/USFed_ConstantMaturityRates/2025_09_09T20_18_16_090752_07_00/*/validation"}]' \
        --project=datcom-store \
        --region "us-central1"
else
    echo "Usage: $0 [deploy|run]"
    exit 1
fi
