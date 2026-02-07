package org.datacommons.ingestion.differ;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.commons.io.FileUtils;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.junit.Rule;
import org.junit.Test;

public class DifferTest {

  PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void testDiffer() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    // Create an input PCollection.
    String currentFile = getClass().getClassLoader().getResource("current").getPath();
    String previousFile = getClass().getClassLoader().getResource("previous").getPath();

    // Process the input.
    PCollection<McfGraph> currentGraph =
        PipelineUtils.readMcfFiles(Paths.get(currentFile, "*.mcf").toString(), p);
    PCollection<McfGraph> previousGraph =
        PipelineUtils.readMcfFiles(Paths.get(previousFile, "*.mcf").toString(), p);
    PCollectionTuple currentNodesTuple = DifferUtils.processGraph(currentGraph);
    PCollectionTuple previousNodesTuple = DifferUtils.processGraph(previousGraph);

    PCollection<KV<String, String>> currentNodes =
        currentNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, String>> previousNodes =
        previousNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<String> obsDiff = DifferUtils.performDiff(currentNodes, previousNodes);

    currentNodes = currentNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    previousNodes = previousNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    PCollection<String> schemaDiff = DifferUtils.performDiff(currentNodes, previousNodes);

    // Assert on the results.
    String expectedObsDiffFile = getClass().getClassLoader().getResource("obs-diff.csv").getPath();
    String expectedSchemaDiffFile =
        getClass().getClassLoader().getResource("schema-diff.csv").getPath();

    try {
      PAssert.that(obsDiff)
          .containsInAnyOrder(
              FileUtils.readLines(new File(expectedObsDiffFile), StandardCharsets.UTF_8));
      PAssert.that(schemaDiff)
          .containsInAnyOrder(
              FileUtils.readLines(new File(expectedSchemaDiffFile), StandardCharsets.UTF_8));

    } catch (IOException e) {
      fail("Unable to read expected output files.");
    }
    // Run the pipeline.
    p.run();
  }
}
