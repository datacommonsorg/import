# Spanner Graph Data Ingestion Pipeline

## org.datacommons.ingestion.pipeline.IngestionPipeline

This module loads all tables (observations, nodes and edges) for all or
specified import groups into Spanner.
The import group version to load from are fetched from the DC API's version
endpoint.

### Worker configuration

- Memory requirement
    - Pipeline is more memory intensive than CPU.
    - Memory hungry steps are
        - Writing to spanner
            - The Spanner sink sorts and batches mutations in memory. To improve
              throughput, the pipeline uses larger batch sizes, which
              consequently requires more memory.
        - Duplicate nodes and edges are eliminated within the pipeline using
          bundle-level caches. These caches track seen items and are cleared
          only after a bundle is processed.
    - Pipeline works best with ram/cpu ratio greater than
        - 8 - when importing individual import groups.
        - 16 - when importing all import groups.
- Worker machine type
    - For a RAM/CPU ratio of 8, it's advisable to use 16-core workers (such as
      n2/e2-highmem-16) instead of 8-core alternatives (like n2/e2-highmem-8).
      This approach helps prevent excessive garbage collection and GC thrashing,
      which can otherwise lead to Dataflow terminating workers and causing
      workflow failures.
    - For a RAM/CPU ratio of 16, prefer 8-core workers (e.g.,
      n2-custom-8-131072-ext with 8 cores and 128GiB). Using 16-core workers
      would necessitate 256GiB of ram, leading to a larger Java heap. This
      larger heap can cause extended GC pauses, during which worker threads are
      unresponsive. The Dataflow service might interpret this unresponsiveness
      as a failed worker and terminate it.
- Worker count
    - Dataflow autoscaling has the tendency to increase worker
      counts for spanner write step.
    - Cap Dataflow's total worker threads to 6x (big
      rows like observations) - 15x (small rows like edges/nodes) your Spanner
      node count. This prevents Spanner overload (for spanner step, workers
      counts are increased by dataflow autoscaling ) and helps maintain write
      throughput.
        - This can be done using pipeline option `--maxNumWorkers`.
        - Please note that `maxNumWorkers` takes count of virtual machines(vm)
          and not worker threads. Hence,
          `maxNumWorkers = Total worker threads / number of cores per vm`.
    - Setting the initial worker count (`numWorkers`) to match the maximum
      worker count (`maxNumWorkers`) saves several minutes by eliminating the
      Dataflow autoscaling typical ramp-up time to reach the maximum number of
      workers.

### Example usages:

### Import all import groups

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.ingestion.pipeline.IngestionPipeline \
-Dexec.args="--project=datcom-store --gcpTempLocation=gs://keyurs-dataflow/temp --runner=DataflowRunner --region=us-central1  --numWorkers=120 --maxNumWorkers=120 --dataflowServiceOptions=enable_google_cloud_profiler --workerMachineType=n2-custom-8-131072-ext"
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

This module implements a Dataflow pipeline that loads Spanner DB with StatVar
observations and graph reading from GCS cache. The Pipeline can be run in local
mode for testing (DirectRunner) or in the cloud (DataflowRunner).

```shell
mvn -Pdataflow-runner compile exec:java -pl ingestion -am -Dexec.mainClass=org.datacommons.IngestionPipeline -Dexec.args="--project=<project-id> --gcpTempLocation=<gs://path> --runner=DataflowRunner --region=<region>  --projectId=<project-id> --spannerInstanceId=<instance-id> --spannerDatabaseId=<database-id> --cacheType=<observation/graph> --importGroupList=auto1d,auto1w,auto2w --storageBucketId=<bucket-id>"
```

## Debug options

When running any pipeline, various debug options are possible. Below are some
that can be useful for the ingestion pipeline:

```shell
# Log hot keys
--hotKeyLoggingEnabled=true

# Enable profiler
--dataflowServiceOptions=enable_google_cloud_profiler

# Capture output samples for display in the cloud console
--experiments=enable_data_sampling
```
