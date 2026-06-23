# Missing Edge Nodes Pipeline

Migration-only Dataflow pipeline to find distinct values from `Edge.subject_id`,
`Edge.predicate`, `Edge.object_id`, and `Edge.provenance` that are missing from
`Node.subject_id`.

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
    --spannerProjectId=<spanner-project> \
    --spannerInstanceId=<spanner-instance> \
    --spannerDatabaseId=<spanner-database> \
    --outputLocation=gs://<bucket>/missing-edge-node-dcids"
```

Output:

```text
gs://<bucket>/missing-edge-node-dcids/missing-edge-node-dcids-*.csv
dcid,type
```
