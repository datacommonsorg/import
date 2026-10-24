package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.gson.JsonParser;
import java.util.List;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class ObservationExportFilesTest {
  @Test
  public void validateOptions_rejectsMissingAndBothSourceModes() {
    TimeseriesBackfillOptions missingOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    assertInvalid(missingOptions);

    TimeseriesBackfillOptions bothOptions =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    bothOptions.setInputExportDir("gs://bucket/export");
    bothOptions.setInputFiles("gs://bucket/export/Observation.avro-00000-of-00001");
    assertInvalid(bothOptions);
  }

  @Test
  public void resolveInputFiles_parsesCsvFileList() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setInputFiles("gs://bucket/a.avro, gs://bucket/b.avro ");

    assertEquals(
        List.of("gs://bucket/a.avro", "gs://bucket/b.avro"),
        ObservationExportFiles.resolveInputFiles(options));
  }

  @Test
  public void parseManifest_resolvesRelativeAndAbsoluteObservationFiles() {
    List<String> files =
        ObservationExportFiles.parseManifest(
            JsonParser.parseString(
                """
                {
                  "files": [
                    "Observation.avro-00000-of-00002",
                    "nested/Observation.avro-00001-of-00002",
                    "gs://other/export/Observation.avro-00002-of-00002",
                    "Other.avro-00000-of-00001"
                  ]
                }
                """),
            "gs://bucket/export",
            "Observation");

    assertEquals(
        List.of(
            "gs://bucket/export/Observation.avro-00000-of-00002",
            "gs://bucket/export/nested/Observation.avro-00001-of-00002",
            "gs://other/export/Observation.avro-00002-of-00002"),
        files);
  }

  private static void assertInvalid(TimeseriesBackfillOptions options) {
    try {
      ObservationExportFiles.validateOptions(options);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException expected) {
      assertEquals(
          "Exactly one of inputExportDir or inputFiles must be provided for the Avro pipeline.",
          expected.getMessage());
    }
  }
}
