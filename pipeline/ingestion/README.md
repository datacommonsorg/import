# Spanner Graph Data Ingestion Pipeline

## org.datacommons.ingestion.pipeline.IngestionPipeline

This module loads all tables (observations, nodes and edges) for all or specified import groups into Spanner.
The import group version to load from are fetched from the DC API's version endpoint.

Example usages:

### Import all import groups

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.ingestion.pipeline.IngestionPipeline \
-Dexec.args="--project=datcom-store --gcpTempLocation=gs://keyurs-dataflow/temp --runner=DataflowRunner --region=us-central1  --numWorkers=60 --maxNumWorkers=60 --dataflowServiceOptions=enable_google_cloud_profiler --workerMachineType=n2-custom-16-262144-ext"
```

### Import all import groups while skipping observations

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.ingestion.pipeline.IngestionPipeline \
-Dexec.args="--skipProcessing=SKIP_OBS --project=datcom-store --gcpTempLocation=gs://keyurs-dataflow/temp --runner=DataflowRunner --region=us-central1  --numWorkers=30 --maxNumWorkers=40 --dataflowServiceOptions=enable_google_cloud_profiler --workerMachineType=e2-highmem-16"
```

### Import specific import group

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.ingestion.pipeline.IngestionPipeline \
-Dexec.args="--importGroupVersion=ipcc_2025_04_04_03_46_23 --project=datcom-store --gcpTempLocation=gs://keyurs-dataflow/temp --runner=DataflowRunner --region=us-central1  --numWorkers=20 --maxNumWorkers=30 \
 --dataflowServiceOptions=enable_google_cloud_profiler --workerMachineType=e2-highmem-16"
```

### Import specific import group while skipping graph

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.ingestion.pipeline.IngestionPipeline \
-Dexec.args="--importGroupVersion=ipcc_2025_04_04_03_46_23 --skipProcessing=SKIP_GRAPH --project=datcom-store --gcpTempLocation=gs://keyurs-dataflow/temp --runner=DataflowRunner --region=us-central1  --numWorkers=20 --maxNumWorkers=20 --dataflowServiceOptions=enable_google_cloud_profiler --workerMachineType=e2-highmem-16"
```

## org.datacommons.IngestionPipeline

This module implements a Dataflow pipeline that loads Spanner DB with StatVar observations and graph reading from GCS cache. The Pipeline can be run in local mode for testing (DirectRunner) or in the cloud (DataflowRunner).

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.IngestionPipeline -Dexec.args="--project=<project-id> --gcpTempLocation=<gs://path> --runner=DataflowRunner --region=<region>  --projectId=<project-id> --spannerInstanceId=<instance-id> --spannerDatabaseId=<database-id> --cacheType=<observation/graph> --importGroupList=auto1d,auto1w,auto2w --storageBucketId=<bucket-id>"
```

## Debug options

When running any pipeline, various debug options are possible. Below are some that can be useful for the ingestion pipeline:

```shell
# Log hot keys
--hotKeyLoggingEnabled=true

# Enable profiler
--dataflowServiceOptions=enable_google_cloud_profiler

# Capture output samples for display in the cloud console
--experiments=enable_data_sampling
```
