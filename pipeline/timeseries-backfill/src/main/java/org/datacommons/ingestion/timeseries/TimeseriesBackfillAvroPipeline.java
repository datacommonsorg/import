package org.datacommons.ingestion.timeseries;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.extensions.avro.io.AvroIO;
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
    PCollection<SourceObservationRow> sourceRows =
        pipeline
            .apply("CreateAvroFileSpecs", Create.of(inputFiles))
            .apply(
                "ReadAvroSourceRows",
                AvroIO.parseAllGenericRecords(SourceObservationRows::toObservationRow)
                    .withCoder(SerializableCoder.of(SourceObservationRow.class)))
            .apply(
                "FilterSourceRows",
                ParDo.of(
                    new FilterSourceObservationRowsFn(
                        options.getStartObservationAbout(),
                        options.getEndObservationAboutExclusive(),
                        VariableMeasuredFilters.parse(options.getVariableMeasured()))));
    sourceRows.setCoder(SerializableCoder.of(SourceObservationRow.class));
    TimeseriesBackfillPipeline.buildPipelineFromSourceRows(sourceRows, options);
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

  static final class FilterSourceObservationRowsFn
      extends DoFn<SourceObservationRow, SourceObservationRow> {
    private final String startObservationAbout;
    private final String endObservationAboutExclusive;
    private final List<String> variableMeasuredFilters;

    FilterSourceObservationRowsFn(
        String startObservationAbout,
        String endObservationAboutExclusive,
        List<String> variableMeasuredFilters) {
      this.startObservationAbout = startObservationAbout;
      this.endObservationAboutExclusive = endObservationAboutExclusive;
      this.variableMeasuredFilters = variableMeasuredFilters;
    }

    @ProcessElement
    public void processElement(
        @Element SourceObservationRow sourceRow, OutputReceiver<SourceObservationRow> out) {
      if (matchesFilters(
          sourceRow.seriesRow(),
          startObservationAbout,
          endObservationAboutExclusive,
          variableMeasuredFilters)) {
        out.output(sourceRow);
      }
    }
  }
}
