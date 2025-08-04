#!/bin/bash

OPERATION=$1

if [ "$OPERATION" == "deploy" ]; then
    echo "Deploying Dataflow Flex Template..."
    gcloud dataflow flex-template build \
        "gs://vishg-dataflow/templates/flex/differ.json" \
        --image-gcr-path "gcr.io/datcom-ci/dataflow-templates/differ:latest" \
        --sdk-language "JAVA" \
        --flex-template-base-image JAVA17 \
        --metadata-file "metadata.json" \
        --jar "target/differ-0.1-SNAPSHOT-jar-with-dependencies.jar" \
        --env FLEX_TEMPLATE_JAVA_MAIN_CLASS="org.datacommons.pipeline.differ.DifferPipeline"
elif [ "$OPERATION" == "run" ]; then
    echo "Running Dataflow Flex Template..."
    gcloud dataflow flex-template run "differ-job" \
        --template-file-gcs-location "gs://datcom-dataflow/templates/flex/differ.json" \
        --parameters importList="import_a,import_b" \
        --parameters spannerDatabaseId="dc_graph_db" \
        --project=datcom-store \
        --region "us-central1"
else
    echo "Usage: $0 [deploy|run]"
    exit 1
fi
