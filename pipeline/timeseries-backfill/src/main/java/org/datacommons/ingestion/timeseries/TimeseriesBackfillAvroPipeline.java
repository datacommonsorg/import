package org.datacommons.ingestion.timeseries;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Beam/Dataflow pipeline that backfills the normalized timeseries tables from Avro exports. */
public class TimeseriesBackfillAvroPipeline {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(TimeseriesBackfillAvroPipeline.class);

  public static void main(String[] args) {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TimeseriesBackfillOptions.class);
    validateOptions(options);

    List<String> inputFiles = ObservationExportFiles.resolveInputFiles(options);
    LOGGER.info(
        "Starting timeseries Avro backfill with runner {} over {} Avro files",
        TimeseriesBackfillPipeline.runnerName(options),
        inputFiles.size());

    try (LocalProgressTracker progressTracker =
        TimeseriesBackfillPipeline.createLocalProgressTracker(options)) {
      TimeseriesBackfillPipeline.setLocalProgressTracker(progressTracker);
      Pipeline pipeline = Pipeline.create(options);
      buildPipeline(pipeline, options, inputFiles);
      PipelineResult result = pipeline.run();
      LOGGER.info("Timeseries Avro backfill returned runner {}", result.getClass().getSimpleName());
    } finally {
      TimeseriesBackfillPipeline.clearLocalProgressTracker();
    }
  }

  static void validateOptions(TimeseriesBackfillOptions options) {
    TimeseriesBackfillPipeline.validateOptions(options);
    ObservationExportFiles.validateOptions(options);
  }

  static void buildPipeline(
      Pipeline pipeline, TimeseriesBackfillOptions options, List<String> inputFiles) {
    PCollection<CompactSourceObservationRow> sourceRows =
        pipeline
            .apply("CreateAvroFileSpecs", Create.of(inputFiles))
            .apply(
                "ReadAvroSourceRows",
                AvroIO.parseAllGenericRecords(SourceObservationRows::toCompactObservationRow)
                    .withCoder(SerializableCoder.of(CompactSourceObservationRow.class)))
            .apply(
                "FilterSourceRows",
                ParDo.of(
                    new FilterCompactSourceObservationRowsFn(
                        options.getStartObservationAbout(),
                        options.getEndObservationAboutExclusive(),
                        VariableMeasuredFilters.parse(options.getVariableMeasured()))));
    sourceRows.setCoder(SerializableCoder.of(CompactSourceObservationRow.class));

    PCollection<MutationGroup> mutationGroups =
        sourceRows.apply(
            "MapMutationGroups",
            ParDo.of(
                new CompactRowsToMutationGroupsFn(
                    options.getDestinationTimeSeriesTableName(),
                    options.getDestinationTimeSeriesAttributeTableName(),
                    options.getDestinationStatVarObservationTableName())));
    mutationGroups.apply("WriteMutationGroups", TimeseriesBackfillIO.buildGroupedWrite(options));
  }

  static boolean matchesFilters(
      SourceSeriesRow seriesRow,
      String startObservationAbout,
      String endObservationAboutExclusive,
      List<String> variableMeasuredFilters) {
    if (!startObservationAbout.isEmpty()
        && seriesRow.observationAbout().compareTo(startObservationAbout) < 0) {
      return false;
    }
    if (!endObservationAboutExclusive.isEmpty()
        && seriesRow.observationAbout().compareTo(endObservationAboutExclusive) >= 0) {
      return false;
    }
    return variableMeasuredFilters.isEmpty()
        || variableMeasuredFilters.contains(seriesRow.variableMeasured());
  }

  static final class FilterCompactSourceObservationRowsFn
      extends DoFn<CompactSourceObservationRow, CompactSourceObservationRow> {
    private final String startObservationAbout;
    private final String endObservationAboutExclusive;
    private final List<String> variableMeasuredFilters;

    FilterCompactSourceObservationRowsFn(
        String startObservationAbout,
        String endObservationAboutExclusive,
        List<String> variableMeasuredFilters) {
      this.startObservationAbout = startObservationAbout;
      this.endObservationAboutExclusive = endObservationAboutExclusive;
      this.variableMeasuredFilters = variableMeasuredFilters;
    }

    @ProcessElement
    public void processElement(
        @Element CompactSourceObservationRow sourceRow,
        OutputReceiver<CompactSourceObservationRow> out) {
      if (matchesFilters(
          sourceRow.seriesRow(),
          startObservationAbout,
          endObservationAboutExclusive,
          variableMeasuredFilters)) {
        out.output(sourceRow);
      }
    }
  }

  static final class CompactRowsToMutationGroupsFn
      extends DoFn<CompactSourceObservationRow, MutationGroup> {
    private static final Counter SOURCE_ROWS =
        Metrics.counter(CompactRowsToMutationGroupsFn.class, "source_rows");
    private static final Counter TIMESERIES_ROWS =
        Metrics.counter(CompactRowsToMutationGroupsFn.class, "timeseries_rows_written");
    private static final Counter TIMESERIES_ATTRIBUTE_ROWS =
        Metrics.counter(CompactRowsToMutationGroupsFn.class, "timeseries_attribute_rows_written");
    private static final Counter SOURCE_POINT_ROWS =
        Metrics.counter(CompactRowsToMutationGroupsFn.class, "source_point_rows");
    private static final Counter STAT_VAR_OBSERVATION_ROWS =
        Metrics.counter(CompactRowsToMutationGroupsFn.class, "stat_var_observation_rows_written");
    private final String timeSeriesTableName;
    private final String timeSeriesAttributeTableName;
    private final String statVarObservationTableName;

    CompactRowsToMutationGroupsFn(
        String timeSeriesTableName,
        String timeSeriesAttributeTableName,
        String statVarObservationTableName) {
      this.timeSeriesTableName = timeSeriesTableName;
      this.timeSeriesAttributeTableName = timeSeriesAttributeTableName;
      this.statVarObservationTableName = statVarObservationTableName;
    }

    @ProcessElement
    public void processElement(
        @Element CompactSourceObservationRow sourceRow, OutputReceiver<MutationGroup> out) {
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
      LocalProgressTracker progressTracker = TimeseriesBackfillPipeline.localProgressTracker();
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
