#!/bin/bash

OPERATION=$1

if [ "$OPERATION" == "deploy" ]; then
    echo "Deploying Dataflow Flex Template..."
    gcloud dataflow flex-template build \
        "gs://datcom-templates/templates/flex/ingestion.json" \
        --image-gcr-path "gcr.io/datcom-ci/dataflow-templates/ingestion:latest" \
        --sdk-language "JAVA" \
        --flex-template-base-image JAVA17 \
        --metadata-file "metadata.json" \
        --jar "target/ingestion-bundled-0.1-SNAPSHOT.jar" \
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
