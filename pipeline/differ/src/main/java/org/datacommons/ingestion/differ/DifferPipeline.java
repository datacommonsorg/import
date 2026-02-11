package org.datacommons.ingestion.differ;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(DifferPipeline.class);
  private static final String DIFF_HEADER =
      "key_combined,value_combined_current,value_combined_previous,diff_type";

  public static void main(String[] args) {

    // Create the pipeline.
    DifferOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DifferOptions.class);
    Pipeline p = Pipeline.create(options);

    // Read input graph files and convert into PCollections.
    PCollection<McfGraph> previousNodes;
    PCollection<McfGraph> currentNodes;
    if (options.getUseOptimizedGraphFormat()) {
      LOGGER.info("Using tfrecord file format");
      currentNodes = PipelineUtils.readMcfGraph(options.getCurrentData(), p);
      previousNodes = PipelineUtils.readMcfGraph(options.getPreviousData(), p);
    } else {
      LOGGER.info("Using mcf file format");
      previousNodes = PipelineUtils.readMcfFiles(options.getPreviousData(), p);
      currentNodes = PipelineUtils.readMcfFiles(options.getCurrentData(), p);
    }

    // Process the input and perform diff operation.
    PCollectionTuple currentNodesTuple = DifferUtils.processGraph(currentNodes);
    PCollectionTuple previousNodesTuple = DifferUtils.processGraph(previousNodes);
    PCollection<KV<String, String>> nCollection =
        currentNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, String>> pCollection =
        previousNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<String> obsDiff = DifferUtils.performDiff(nCollection, pCollection);

    nCollection = currentNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG).apply(Distinct.create());
    pCollection = previousNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG).apply(Distinct.create());
    PCollection<String> schemaDiff = DifferUtils.performDiff(nCollection, pCollection);

    obsDiff.apply(
        "Write observation diff output",
        TextIO.write()
            .to(options.getOutputLocation() + "/" + "obs-diff")
            .withSuffix(".csv")
            .withNumShards(1)
            .withHeader(DIFF_HEADER));
    schemaDiff.apply(
        "Write schema diff output",
        TextIO.write()
            .to(options.getOutputLocation() + "/" + "schema-diff")
            .withSuffix(".csv")
            .withNumShards(1)
            .withHeader(DIFF_HEADER));
    p.run();
  }
}
