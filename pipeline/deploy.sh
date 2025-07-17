#!/bin/bash

OPERATION=$1

if [ "$OPERATION" == "deploy" ]; then
    echo "Deploying Dataflow Flex Template..."
    gcloud dataflow flex-template build \
        "gs://vishg-dataflow/templates/flex/ingestion.json" \
        --image-gcr-path "gcr.io/datcom-ci/dataflow-templates/ingestion:latest" \
        --sdk-language "JAVA" \
        --flex-template-base-image JAVA17 \
        --metadata-file "ingestion/metadata.json" \
        --jar "ingestion/target/ingestion-0.1-SNAPSHOT-jar-with-dependencies.jar" \
        --project=datcom-ci \
        --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.datacommons.ingestion.pipeline.ImportGroupPipeline"
elif [ "$OPERATION" == "run" ]; then
    echo "Running Dataflow Flex Template..."
    gcloud dataflow flex-template run "template-test" \
        --template-file-gcs-location "gs://vishg-dataflow/templates/flex/ingestion.json" \
        --parameters importList="gs://vishg-dataflow/temp" \
        --parameters spannerDatabaseId="dc_graph_5" \
        --project=datcom-store \
        --region "us-central1"
else
    echo "Usage: $0 [deploy|run]"
    exit 1
fi
