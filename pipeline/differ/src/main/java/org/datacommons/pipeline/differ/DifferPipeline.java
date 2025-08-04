package org.datacommons.pipeline.differ;

import java.nio.file.Paths;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.datacommons.pipeline.util.GraphUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(DifferPipeline.class);
  private static final String OBSERVATION_DIFF_HEADER =
      "variableMeasured,observationAbout,observationDate,observationPeriod,measurementMethod,unit,scalingFactor,valueCurrent,valuePrevious,diff";
  private static final String SCHEMA_DIFF_HEADER = "dcid,valueCurrent,valuePrevious,diff";

  public static void main(String[] args) throws Exception {

    // Create the pipeline.
    DifferOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DifferOptions.class);
    Pipeline p = Pipeline.create(options);

    // Read input graph files and convert into PCollections.
    PCollection<McfGraph> previousNodes, currentNodes;
    if (options.getUseOptimizedGraphFormat()) {
      LOGGER.info("Using tfrecord file format");
      currentNodes = GraphUtils.readMcfGraph(options.getCurrentData(), p);
      previousNodes = GraphUtils.readMcfGraph(options.getPreviousData(), p);
    } else {
      LOGGER.info("Using mcf file format");
      previousNodes = GraphUtils.readMcfFile(options.getPreviousData(), p);
      currentNodes = GraphUtils.readMcfFile(options.getCurrentData(), p);
    }

    // Process the input and perform diff operation.
    PCollectionTuple currentNodesTuple = DifferUtils.processGraph(currentNodes);
    PCollectionTuple previousNodesTuple = DifferUtils.processGraph(previousNodes);
    PCollection<KV<String, String>> nCollection =
        currentNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, String>> pCollection =
        previousNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<String> obsDiff = DifferUtils.performDiff(nCollection, pCollection);

    nCollection = currentNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    pCollection = previousNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    PCollection<String> schemaDiff = DifferUtils.performDiff(nCollection, pCollection);

    obsDiff.apply(
        "Write observation diff output",
        TextIO.write()
            .to(Paths.get(options.getOutputLocation(), "observation-diff").toString())
            .withSuffix(".csv")
            .withHeader(OBSERVATION_DIFF_HEADER));
    schemaDiff.apply(
        "Write schema diff output",
        TextIO.write()
            .to(Paths.get(options.getOutputLocation(), "schema-diff").toString())
            .withSuffix(".csv")
            .withHeader(SCHEMA_DIFF_HEADER));
    p.run().waitUntilFinish();
  }
}
