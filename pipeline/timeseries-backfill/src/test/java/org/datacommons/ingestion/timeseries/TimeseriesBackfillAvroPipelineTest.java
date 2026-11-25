package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class TimeseriesBackfillAvroPipelineTest {
  @Test
  public void validateOptions_acceptsExportDirAndInputFilesModes() {
    TimeseriesBackfillOptions exportDirOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    exportDirOptions.setInputExportDir("gs://bucket/export");

    TimeseriesBackfillOptions inputFilesOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    inputFilesOptions.setInputFiles("gs://bucket/export/Observation.avro-00000-of-00001");

    TimeseriesBackfillAvroPipeline.validateOptions(exportDirOptions);
    TimeseriesBackfillAvroPipeline.validateOptions(inputFilesOptions);
  }

  @Test
  public void validateOptions_rejectsBeamRowCapsForAvroPipeline() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setInputExportDir("gs://bucket/export");
    options.setMaxSeriesRows(10);

    try {
      TimeseriesBackfillAvroPipeline.validateOptions(options);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Beam row caps are not supported"));
    }
  }

  @Test
  public void matchesFilters_appliesObservationAboutAndVariableMeasuredBounds() {
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "123",
            "",
            "",
            "",
            "",
            "TestImport",
            "",
            false,
            "dc/base/TestImport");

    assertTrue(
        TimeseriesBackfillAvroPipeline.matchesFilters(
            seriesRow, "geoId/06", "geoId/07", List.of("Count_Person")));
    assertFalse(
        TimeseriesBackfillAvroPipeline.matchesFilters(
            seriesRow, "geoId/07", "", List.of("Count_Person")));
    assertFalse(
        TimeseriesBackfillAvroPipeline.matchesFilters(
            seriesRow, "", "", List.of("Min_Temperature")));
  }
}
