package org.datacommons.ingestion.timeseries;

import com.google.cloud.spanner.Struct;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.Transaction;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Beam/Dataflow pipeline that backfills the normalized timeseries tables from Observation. */
public class TimeseriesBackfillPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesBackfillPipeline.class);
  private static volatile LocalProgressTracker localProgressTracker;

  public static void main(String[] args) {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TimeseriesBackfillOptions.class);
    validateOptions(options);
    LOGGER.info("Starting timeseries backfill with runner {}", runnerName(options));
    try (LocalProgressTracker progressTracker = createLocalProgressTracker(options)) {
      setLocalProgressTracker(progressTracker);
      Pipeline pipeline = Pipeline.create(options);
      buildPipeline(pipeline, options);
      PipelineResult result = pipeline.run();
      LOGGER.info("Timeseries backfill returned runner {}", result.getClass().getSimpleName());
    } finally {
      clearLocalProgressTracker();
    }
  }

  static void buildPipeline(Pipeline pipeline, TimeseriesBackfillOptions options) {
    TimeseriesBackfillQueries queries = new TimeseriesBackfillQueries(options);
    PCollectionView<Transaction> readTransaction =
        pipeline.apply("CreateReadTransaction", TimeseriesBackfillIO.buildReadTransaction(options));

    PCollection<SourceObservationRow> sourceRows =
        pipeline
            .apply(
                "ReadSourceRows",
                TimeseriesBackfillIO.buildRead(options)
                    .withTransaction(readTransaction)
                    .withQuery(queries.buildSourceQuery(options.getMaxSeriesRows()))
                    .withQueryName("ReadSourceRows"))
            .apply("NormalizeSourceRows", ParDo.of(new StructToSourceObservationRowsFn()));
    sourceRows.setCoder(SerializableCoder.of(SourceObservationRow.class));
    buildPipelineFromSourceRows(sourceRows, options);
  }

  static void buildPipelineFromSourceRows(
      PCollection<SourceObservationRow> sourceRows, TimeseriesBackfillOptions options) {
    PCollection<MutationGroup> mutationGroups =
        sourceRows.apply(
            "MapMutationGroups",
            ParDo.of(
                new SourceRowsToMutationGroupsFn(
                    options.getDestinationTimeSeriesTableName(),
                    options.getDestinationTimeSeriesAttributeTableName(),
                    options.getDestinationStatVarObservationTableName())));

    mutationGroups.apply("WriteMutationGroups", TimeseriesBackfillIO.buildGroupedWrite(options));
  }

  static void validateOptions(TimeseriesBackfillOptions options) {
    TimeseriesBackfillOptionValidator.validateCommonOptions(options);
    if (options.getMaxSeriesRows() > 0 || options.getMaxPointRows() > 0) {
      throw new IllegalArgumentException(
          "Beam row caps are not supported in the Beam/Dataflow entrypoints. "
              + "Use observation_about sharding or exact Avro file selection, or use the "
              + "validator for small bounded runs.");
    }
  }

  static boolean shouldEnableLocalProgress(TimeseriesBackfillOptions options) {
    return options.getRunner() == null || DirectRunner.class.equals(options.getRunner());
  }

  static LocalProgressTracker createLocalProgressTracker(TimeseriesBackfillOptions options) {
    if (!shouldEnableLocalProgress(options)) {
      return new LocalProgressTracker("DataflowRunner", 0, 0, LOGGER);
    }
    return new LocalProgressTracker(
        runnerName(options),
        options.getProgressEverySourceRows(),
        options.getHeartbeatSeconds(),
        LOGGER);
  }

  static String runnerName(TimeseriesBackfillOptions options) {
    if (options.getRunner() == null) {
      return DirectRunner.class.getSimpleName();
    }
    return options.getRunner().getSimpleName();
  }

  static void setLocalProgressTracker(LocalProgressTracker progressTracker) {
    localProgressTracker = progressTracker;
  }

  static void clearLocalProgressTracker() {
    localProgressTracker = null;
  }

  static LocalProgressTracker localProgressTracker() {
    return localProgressTracker;
  }

  static final class StructToSourceObservationRowsFn extends DoFn<Struct, SourceObservationRow> {
    @ProcessElement
    public void processElement(@Element Struct row, OutputReceiver<SourceObservationRow> out) {
      out.output(SourceObservationRows.toObservationRow(row));
    }
  }

  static final class SourceRowsToMutationGroupsFn
      extends DoFn<SourceObservationRow, MutationGroup> {
    private static final Counter SOURCE_ROWS =
        Metrics.counter(SourceRowsToMutationGroupsFn.class, "source_rows");
    private static final Counter TIMESERIES_ROWS =
        Metrics.counter(SourceRowsToMutationGroupsFn.class, "timeseries_rows_written");
    private static final Counter TIMESERIES_ATTRIBUTE_ROWS =
        Metrics.counter(SourceRowsToMutationGroupsFn.class, "timeseries_attribute_rows_written");
    private static final Counter SOURCE_POINT_ROWS =
        Metrics.counter(SourceRowsToMutationGroupsFn.class, "source_point_rows");
    private static final Counter STAT_VAR_OBSERVATION_ROWS =
        Metrics.counter(SourceRowsToMutationGroupsFn.class, "stat_var_observation_rows_written");
    private final String timeSeriesTableName;
    private final String timeSeriesAttributeTableName;
    private final String statVarObservationTableName;

    SourceRowsToMutationGroupsFn(
        String timeSeriesTableName,
        String timeSeriesAttributeTableName,
        String statVarObservationTableName) {
      this.timeSeriesTableName = timeSeriesTableName;
      this.timeSeriesAttributeTableName = timeSeriesAttributeTableName;
      this.statVarObservationTableName = statVarObservationTableName;
    }

    @ProcessElement
    public void processElement(
        @Element SourceObservationRow sourceRow, OutputReceiver<MutationGroup> out) {
      BackfillMutationGroups mutationGroups =
          TimeseriesMutationFactory.toMutationGroups(
              sourceRow,
              timeSeriesTableName,
              timeSeriesAttributeTableName,
              statVarObservationTableName);
      SOURCE_ROWS.inc();
      TIMESERIES_ROWS.inc();
      TIMESERIES_ATTRIBUTE_ROWS.inc(mutationGroups.timeSeriesAttributeRows());
      SOURCE_POINT_ROWS.inc(mutationGroups.statVarObservationRows());
      STAT_VAR_OBSERVATION_ROWS.inc(mutationGroups.statVarObservationRows());
      LocalProgressTracker progressTracker = localProgressTracker;
      if (progressTracker != null) {
        progressTracker.recordRow(
            mutationGroups.timeSeriesAttributeRows(), mutationGroups.statVarObservationRows());
      }

      for (MutationGroup mutationGroup : mutationGroups.groups()) {
        out.output(mutationGroup);
      }
    }
  }
}
