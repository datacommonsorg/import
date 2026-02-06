#!/bin/bash

OPERATION=$1

if [ "$OPERATION" == "deploy" ]; then
    echo "Deploying Dataflow Flex Template..."
    gcloud dataflow flex-template build \
        "gs://datcom-templates/templates/flex/differ.json" \
        --image-gcr-path "gcr.io/datcom-ci/dataflow-templates/differ:latest" \
        --sdk-language "JAVA" \
        --flex-template-base-image JAVA17 \
        --metadata-file "metadata.json" \
        --jar "target/differ-bundled-0.1-SNAPSHOT.jar" \
        --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.datacommons.ingestion.differ.DifferPipeline"
elif [ "$OPERATION" == "run" ]; then
    echo "Running Dataflow Flex Template..."
    gcloud dataflow flex-template run "differ-job" \
        --template-file-gcs-location "gs://datcom-templates/templates/flex/differ.json" \
        --parameters currentData="gs://datcom-prod-imports/scripts/import-path/*.mcf" \
        --parameters previousData="gs://datcom-prod-imports/scripts/import-path/*.mcf" \
        --parameters outputLocation="gs://datcom-dataflow/differ/output" \
        --project=datcom-store \
        --region "us-central1"
else
    echo "Usage: $0 [deploy|run]"
    exit 1
fi
