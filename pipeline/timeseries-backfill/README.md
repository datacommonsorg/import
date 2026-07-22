# Timeseries Backfill

This module backfills the normalized timeseries tables from the legacy `Observation` table in Spanner.

It supports two execution paths:
- **Spanner to Spanner**: Reads directly from the live `Observation` table in Spanner. This approach is direct and doesn't require an intermediate export, but it queries the live database which might impact performance or require stable snapshots for consistency. **Note:** Complete migration using this path failed as queries timed out in Spanner after 2 hours. It is recommended only for targeted small batches (e.g., scoped by variable measured + geo combination).
- **Avro to Spanner**: Reads from exported `Observation` Avro files on GCS. This approach is decoupled from the live database for reading, making it suitable for large-scale backfills or when reading from a specific historical export. **Note:** For large backfills, this path is recommended. However, filtering in Avro reads all files in the selected directory/list. It supports 2 modes: all files in a directory or a single file. **Crucially, the Spanner table backup needs to be taken separately for this path to work (see One-Time Prep).**

By default it writes:

- `TimeSeries_<SUFFIX>`
- `TimeSeriesAttribute_<SUFFIX>`
- `StatVarObservation_<SUFFIX>`

It does not populate `ObservationAttribute` in v1.

## Source And Destination Shape

Source table:

- `Observation` from [import/pipeline/spanner/src/main/resources/spanner_schema.sql](../spanner/src/main/resources/spanner_schema.sql)

Destination schema:

- [timeseries_schema.sql](<PATH_TO_SCHEMA>/timeseries_schema.sql)

Important assumptions used by this module:

- `TimeSeries.id` reuses the existing `dc/os/<variable>_<observationAbout>_<facetId>` series id shape.
- `TimeSeries.provenance` is derived from `import_name` as `dc/base/<import_name>`.
- `TimeSeriesAttribute` stores the current normalized-read fields:
  `observationAbout`, `facetId`, `importName`, `provenanceUrl`, `observationPeriod`,
  `measurementMethod`, `unit`, `scalingFactor`, `isDcAggregate`.
- The backfill should run against a fixed source snapshot via `--readTimestamp` when correctness matters.

## Entry Points

- `org.datacommons.ingestion.timeseries.TimeseriesBackfillPipeline`
  Beam/Dataflow batch job for the full backfill.
- `org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline`
  Beam/Dataflow batch job that reads exported `Observation` Avro files and writes the same destination tables.
- `org.datacommons.ingestion.timeseries.TimeseriesBackfillValidator`
  Small direct runner for bounded validation on one shard or range.

The Spanner and Avro Beam entrypoints reuse the same downstream mutation and Spanner write path.

## Flag Reference

Core flags:

- `--project`: GCP project that owns the Spanner instance and database. Prefer this for all runs, especially `DataflowRunner`.
- `--projectId`: Legacy fallback for the same GCP project value. Keep only for compatibility with the older import-pipeline style.
- `--spannerInstanceId`: Spanner instance to read from and write to.
- `--spannerDatabaseId`: Spanner database containing both the source `Observation` table and the destination normalized tables.
- `--sourceObservationTableName`: Source table name. Normally `Observation`.
- `--inputExportDir`: Avro-only. Spanner export directory containing `Observation-manifest.json`.
- `--inputFiles`: Avro-only. Comma-separated exact Avro file paths.
- `--destinationTimeSeriesTableName`: Destination parent series table. Normally `TimeSeries_<SUFFIX>`.
- `--destinationTimeSeriesAttributeTableName`: Destination series-attribute table. Normally `TimeSeriesAttribute_<SUFFIX>`.
- `--destinationStatVarObservationTableName`: Destination point table. Normally `StatVarObservation_<SUFFIX>`.
- `--readTimestamp`: Source read snapshot in RFC3339 format, for example `2026-04-22T00:00:00Z`. When set, the job reads from that exact Spanner snapshot for consistent backfill semantics. When empty, the code uses a Spanner strong read instead.
- `--variableMeasured`: Fixed `variable_measured` filter. Use one stat var or a comma-separated list such as `Count_Person,Min_Temperature`.
- `--startObservationAbout`: Inclusive lower bound on `observation_about`. Useful for sharding by place range.
- `--endObservationAboutExclusive`: Exclusive upper bound on `observation_about`. Use together with `--startObservationAbout` to define one shard.
- `--spannerEmulatorHost`: Optional emulator host such as `localhost:9010`. Leave empty for real Cloud Spanner.

For the Avro entrypoint, exactly one of `--inputExportDir` or `--inputFiles` is required.

If `--startObservationAbout` or `--endObservationAboutExclusive` is set, `--variableMeasured` is required. It can still be a comma-separated list.

Beam pipeline row-limit flags:

- `--maxSeriesRows`: Currently unsupported for Beam/Dataflow because partitioned Spanner queries cannot use the generated bounded SQL shape. Leave unset for Beam runs.
- `--maxPointRows`: Currently unsupported for Beam/Dataflow because partitioned Spanner queries cannot use the generated bounded SQL shape. Leave unset for Beam runs.

Local progress flags:

- `--progressEverySourceRows`: Local progress log interval for validator and `DirectRunner`. Default `1000`. Non-positive disables row-progress logs.
- `--heartbeatSeconds`: Local heartbeat log interval for validator and `DirectRunner`. Default `30`. Non-positive disables heartbeat logs.

Optional Beam/Dataflow Spanner sink flags:

- `--batchSizeBytes`: Optional `SpannerIO.Write` batch size in bytes. When unset, Beam uses its default.
- `--maxNumRows`: Optional `SpannerIO.Write` max rows per batch. When unset, Beam uses its default.
- `--maxNumMutations`: Optional `SpannerIO.Write` max mutations per batch. When unset, Beam uses its default.
- `--groupingFactor`: Optional `SpannerIO.Write` grouping factor. When unset, Beam uses its default.
- `--commitDeadlineSeconds`: Optional `SpannerIO.Write` commit deadline in seconds. When unset, Beam uses its default.

Validator-only flags:

- `--validatorMaxSeriesRows`: Cap on source series rows for `TimeseriesBackfillValidator`. Non-positive means no limit.

Beam/Dataflow runner flags used in the examples:

- `--runner`: Beam runner to use. `DirectRunner` runs locally. `DataflowRunner` submits the Beam job to Dataflow.
- `--region`: Dataflow region. Only needed with `DataflowRunner`.
- `--tempLocation`: GCS path used by Dataflow for temporary files.
- `--stagingLocation`: GCS path used by Dataflow for staged job artifacts.
- `--numWorkers`: Initial Dataflow worker count.
- `--maxNumWorkers`: Maximum Dataflow worker count when autoscaling is enabled.
- `--workerMachineType`: Worker VM shape, for example `n2-custom-4-32768`.
- `--numberOfWorkerHarnessThreads`: Number of SDK harness threads per worker, for example `2`.

## Flag Guidance

- Always set `--project`, `--spannerInstanceId`, and `--spannerDatabaseId`.
- Set `--readTimestamp` for any real backfill where you want a stable source snapshot.
- Use `TimeseriesBackfillPipeline` when the source is live Spanner and `TimeseriesBackfillAvroPipeline` when the source is a Spanner Avro export.
- Use `--variableMeasured` for every local or early Dataflow run. It accepts one stat var or a comma-separated list.
- Add `--startObservationAbout` and `--endObservationAboutExclusive` only when you want to shard one stat-var slice by place range. If either range flag is set, `--variableMeasured` is required.
- For the Avro entrypoint, use `--inputExportDir` for a full exported snapshot and `--inputFiles` for targeted reruns or debugging.
- For the Avro entrypoint, `--inputExportDir` reads only `Observation-manifest.json` and only the `Observation.avro-*` files listed there. Other exported table files in the same export directory are ignored.
- `--inputExportDir` always resolves the full `Observation` file list from the manifest. It is not a partial-file selector.
- Use `--inputFiles` when you want to process only a few exact Avro files for a targeted rerun or debugging.
- Avro local runs are supported only through `TimeseriesBackfillAvroPipeline` with `DirectRunner`. There is no separate standalone Avro validator.
- The Avro entrypoint writes directly to Spanner. It does not create new intermediate output files.
- `--variableMeasured`, `--startObservationAbout`, and `--endObservationAboutExclusive` are row-level filters after Avro rows are read. They narrow which rows are written, but they do not prune the Avro file list chosen from `--inputExportDir`.
- Use `TimeseriesBackfillValidator` first when you want a small bounded write. `--validatorMaxSeriesRows` is the only bounded-row flag that is supported today.
- Do not use `--maxSeriesRows` or `--maxPointRows` with Beam/Dataflow. They are intentionally blocked for partitioned Spanner reads.
- For local `DirectRunner`, tune `--progressEverySourceRows` and `--heartbeatSeconds` if you want more visible liveness logs.
- For Dataflow memory experiments, test worker settings separately from backfill query settings. The main ones are `--workerMachineType` and `--numberOfWorkerHarnessThreads`.
- Use the optional Spanner sink flags only when you want to override Beam defaults for the Beam/Dataflow write path.
- The standalone validator accepts those sink flags because it shares the options interface, but its direct `DatabaseClient.writeAtLeastOnce(...)` path does not use them.

## Spanner Sink Tuning Order

Suggested order to test if the sink is too memory-heavy or too serialized:

- `groupingFactor`
- `maxNumRows`
- `maxNumMutations`
- `batchSizeBytes`
- `commitDeadlineSeconds`

## One-Time Prep

### Exporting Observation Table (for Avro path)
If you plan to use the Avro execution path, you must first export the `Observation` table from Spanner to GCS as Avro files. Run this command to start the Dataflow export job:

```bash
gcloud dataflow jobs run observation-export-$(date +%Y%m%d-%H%M%S) \
    --project=datcom-store \
    --region=us-central1 \
    --gcs-location=gs://dataflow-templates-us-central1/latest/Cloud_Spanner_to_GCS_Avro \
    --max-workers=50 \
    --parameters=instanceId=dc-kg-test,databaseId=dc_graph_2026_01_27,spannerProjectId=datcom-store,tableNames=Observation,outputDir=gs://<USER_DATAFLOW_BUCKET>/spanner_obs_dump_2026_04_21,dataBoostEnabled=true,spannerPriority=LOW
```

If you see a missing artifact error for `org.datacommons:datacommons-import-util`, install that top-level module once from the `import/pipeline` directory:

```bash
mvn -f ../pom.xml -pl util -am install -Pgit-worktree -DskipTests
```

## Build

From the `import/pipeline` directory:

```bash
mvn -pl timeseries-backfill -am package -Pgit-worktree -DskipTests
```

## Validate Locally

Run the standalone validator from the `import/pipeline` directory:

```bash
mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillValidator \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --sourceObservationTableName=Observation --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk --variableMeasured=Count_Person --startObservationAbout=geoId/06 --endObservationAboutExclusive=geoId/07 --validatorMaxSeriesRows=1000"
```

The validator uses the same single-read row mapping logic as the Beam pipeline and writes bounded batches directly to Spanner.

During local validation, the default progress logs print:

- a heartbeat every `30` seconds
- a row-progress log every `1000` source rows
- a validator flush log on each write batch

Use the validator first when you want a small bounded write without running Beam.

## Run With DirectRunner

```bash
mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --sourceObservationTableName=Observation --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk --variableMeasured=Count_Person --startObservationAbout=geoId/06 --endObservationAboutExclusive=geoId/07 --runner=DirectRunner"
```

`DirectRunner` still runs the Beam job locally. For a small bounded run, use the validator instead of Beam row-cap flags, and pair any observation_about range with `--variableMeasured`.

The Beam/Dataflow path reads the raw `observations` proto value from `Observation` and expands it inside Beam. The validator uses that same source-read and expansion path.

For local `DirectRunner`, the default progress logs print:

- a heartbeat every `30` seconds
- a row-progress log every `1000` source rows

Set `--progressEverySourceRows` or `--heartbeatSeconds` if you want a different local signal. These flags are ignored for `DataflowRunner`.

## Run Avro Export With DirectRunner

```bash
mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputFiles=gs://rohitrkumar-dataflow/spanner_obs_dump_2026_04_21/dc-kg-test-dc_graph_2026_01_27-2026-04-23_05_47_24-8439747614048276587/Observation.avro-00005-of-00303 --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk  --runner=DirectRunner"


  mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputFiles=<PATH_TO_IMPORT_REPO>/import/pipeline/Observation.avro-00042-of-00303 --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk  --runner=DirectRunner"

```

The Avro entrypoint reads exported `Observation` Avro rows, recomputes `provenance` from `import_name`, parses `observations` from the serialized proto payload, and then reuses the same normalized write path as the Spanner entrypoint.

For Avro input selection:

- `--inputExportDir` should point to the exact export subdirectory that contains `Observation-manifest.json`.
- `--inputExportDir` processes all `Observation.avro-*` files listed in that manifest.
- Use `--inputFiles` instead if you want to process only a few exact Avro files.
- `--variableMeasured` and the observation-about range flags still apply, but only after the selected Avro files are opened and rows are read.

## Run On Dataflow

```bash
mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --sourceObservationTableName=Observation --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk --variableMeasured=Count_Person --startObservationAbout=geoId/06 --endObservationAboutExclusive=geoId/07 --runner=DataflowRunner --region=us-central1 --tempLocation=gs://keyurs-dataflow/temp --stagingLocation=gs://keyurs-dataflow/temp --numWorkers=20 --maxNumWorkers=100 --workerMachineType=n2-custom-4-32768 --numberOfWorkerHarnessThreads=2"
```

Use `--variableMeasured` together with `--startObservationAbout` and `--endObservationAboutExclusive` to shard Beam/Dataflow runs safely.

Do not force the `ObservationAboutVariableMeasured` index for the Beam/Dataflow queries in their current form. The backfill selects columns that are not stored in that index, so forcing it can introduce a back join and make the query non-root-partitionable.

## Run Avro Export On Dataflow

```bash
mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputExportDir=gs://rohitrkumar-dataflow/spanner_obs_dump_2026_04_21/<export_subdir> --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk --variableMeasured=Count_Person --runner=DataflowRunner --region=us-central1 --tempLocation=gs://keyurs-dataflow/temp --stagingLocation=gs://keyurs-dataflow/temp --numWorkers=20 --maxNumWorkers=100 --workerMachineType=n2-custom-4-32768 --numberOfWorkerHarnessThreads=2"

  mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputFiles=gs://rohitrkumar-dataflow/spanner_obs_dump_2026_04_21/dc-kg-test-dc_graph_2026_01_27-2026-04-23_05_47_24-8439747614048276587/Observation.avro-00042-of-00303 --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk  --runner=DataflowRunner --region=us-central1 --tempLocation=gs://keyurs-dataflow/temp --stagingLocation=gs://keyurs-dataflow/temp --numWorkers=20 --maxNumWorkers=100 --workerMachineType=n2-custom-4-32768 --numberOfWorkerHarnessThreads=2"

  mvn -Pgit-worktree compile exec:java \
  -pl timeseries-backfill -am \
  -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
  -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputExportDir=gs://rohitrkumar-dataflow/spanner_obs_dump_2026_04_21/dc-kg-test-dc_graph_2026_01_27-2026-04-23_05_47_24-8439747614048276587 --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk  --runner=DataflowRunner --region=us-central1 --tempLocation=gs://keyurs-dataflow/temp --stagingLocation=gs://keyurs-dataflow/temp --numWorkers=20 --maxNumWorkers=100 --workerMachineType=n2-custom-4-32768 --numberOfWorkerHarnessThreads=2"

  mvn -Pgit-worktree compile exec:java \
        -pl timeseries-backfill -am \
        -Dexec.mainClass=org.datacommons.ingestion.timeseries.TimeseriesBackfillAvroPipeline \
        -Dexec.args="--project=datcom-store --spannerInstanceId=dc-kg-test --spannerDatabaseId=dc_graph_2026_01_27 --inputExportDir=gs://rohitrkumar-dataflow/spanner_obs_dump_2026_04_21/dc-kg-test-dc_graph_2026_01_27-2026-04-23_05_47_24-8439747614048276587 --destinationTimeSeriesTableName=TimeSeries_rk --destinationTimeSeriesAttributeTableName=TimeSeriesAttribute_rk --destinationStatVarObservationTableName=StatVarObservation_rk  --runner=DataflowRunner --region=us-central1 --tempLocation=gs://keyurs-dataflow/temp --stagingLocation=gs://keyurs-dataflow/temp --numWorkers=20 --maxNumWorkers=100 --workerMachineType=n2-custom-4-32768 --numberOfWorkerHarnessThreads=2

```

## Recreate Destination Tables

Run this from the `import` repo root to drop any existing experimental normalized `_rk` tables and recreate them from the checked-in schema:

```bash
./pipeline/timeseries-backfill/recreate_timeseries_tables.sh datcom-store dc-kg-test dc_graph_5
```

The script reads the current database DDL, drops the normalized `_rk` tables and indexes if they already exist, and then reapplies a suffixed form of [timeseries_schema.sql](<PATH_TO_SCHEMA>/timeseries_schema.sql).

## Notes

- For the Avro export path, `Observation.provenance` should not be relied on as an exported populated value because it is a stored generated column in the deployed schema. This module recomputes it from `import_name`.
- The default experimental destination schema should include `TimeSeriesAttributePropertyValue_rk` and `TimeSeriesAttributeValue_rk` before using the normalized Mixer query path.
