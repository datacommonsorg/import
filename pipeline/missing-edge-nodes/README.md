# Missing Edge Nodes Pipeline

Migration-only Dataflow pipeline to find distinct values from `Edge.subject_id`,
`Edge.predicate`, `Edge.object_id`, and `Edge.provenance` that are missing from
`Node.subject_id`.

The Spanner queries are plain scans; Beam deduplicates the combined typed Edge
candidate stream.

Run from `import/pipeline`:

```bash
mvn compile exec:java -pl missing-edge-nodes -am \
  -Dexec.mainClass=org.datacommons.ingestion.missingnodes.MissingEdgeNodesPipeline \
  -Dexec.args="\
    --runner=DataflowRunner \
    --project=<dataflow-project> \
    --region=<region> \
    --tempLocation=gs://<bucket>/tmp \
    --stagingLocation=gs://<bucket>/staging \
    --jobName=missing-edge-node-dcids \
    --workerMachineType=e2-highmem-16 \
    --numberOfWorkerHarnessThreads=8 \
    --spannerProjectId=<spanner-project> \
    --spannerInstanceId=<spanner-instance> \
    --spannerDatabaseId=<spanner-database> \
    --writeDedupedInputs=true \
    --outputLocation=gs://<bucket>/edge-node-audit"
```

Staging Spanner command:

```bash
mvn compile exec:java -pl missing-edge-nodes -am -Dexec.mainClass=org.datacommons.ingestion.missingnodes.MissingEdgeNodesPipeline -Dexec.args="--runner=DataflowRunner --project=datcom-store --region=us-central1 --tempLocation=gs://rohitrkumar-dataflow/temp/tmp --stagingLocation=gs://rohitrkumar-dataflow/temp/staging --jobName=missing-edge-node-dcids --workerMachineType=e2-highmem-16 --numberOfWorkerHarnessThreads=8 --spannerProjectId=datcom-store --spannerInstanceId=dc-graph-staging --spannerDatabaseId=dc_graph --writeDedupedInputs=true --outputLocation=gs://rohitrkumar-dataflow/edge-node-audit"
```

For large runs, start with `e2-highmem-16`. If workers still OOM, retry with
`--numberOfWorkerHarnessThreads=4` to reduce per-worker concurrency. Avoid
`e2-standard-32` for this job; it has the same total memory as `e2-highmem-16`
but more worker concurrency and less memory per vCPU.

Output:

```text
gs://<bucket>/edge-node-audit/missing-edge-node-dcids/part-*.csv
dcid,type
```

When `writeDedupedInputs` is enabled, the job also writes headerless files:

```text
gs://<bucket>/edge-node-audit/distinct-node-subject-ids/part-*.csv
gs://<bucket>/edge-node-audit/distinct-edge-subject-ids/part-*.csv
gs://<bucket>/edge-node-audit/distinct-predicates/part-*.csv
gs://<bucket>/edge-node-audit/distinct-object-ids/part-*.csv
gs://<bucket>/edge-node-audit/distinct-provenances/part-*.csv
```
