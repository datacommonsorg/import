package org.datacommons.ingestion.differ;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DifferPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(DifferPipeline.class);

  public static void main(String[] args) {

    // Create the pipeline.
    DifferOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(DifferOptions.class);
    Pipeline p = Pipeline.create(options);

    buildPipeline(p, options);

    PipelineResult result = p.run();

    try {
      result.waitUntilFinish();
      DifferUtils.generateSummary(result, options.getOutputLocation());
    } catch (UnsupportedOperationException e) {
      LOGGER.info("Pipeline run in template mode. Not waiting or writing metrics.");
    }
  }

  public static PCollectionTuple buildPipeline(Pipeline p, DifferOptions options) {
    // Read input graph files and convert into PCollections.
    PCollection<McfGraph> previousNodes;
    PCollection<McfGraph> currentNodes;
    if (options.getUseOptimizedGraphFormat() != null && options.getUseOptimizedGraphFormat()) {
      LOGGER.info("Using tfrecord file format");
      currentNodes = PipelineUtils.readMcfGraph("differ", options.getCurrentData(), p);
      previousNodes = PipelineUtils.readMcfGraph("differ", options.getPreviousData(), p);
    } else {
      LOGGER.info("Using mcf file format");
      previousNodes = PipelineUtils.readMcfFiles("differ", options.getPreviousData(), p);
      currentNodes = PipelineUtils.readMcfFiles("differ", options.getCurrentData(), p);
    }

    // Process the input and perform diff operation.
    PCollectionTuple currentNodesTuple = DifferUtils.processGraph(currentNodes, true);
    PCollectionTuple previousNodesTuple = DifferUtils.processGraph(previousNodes, false);

    PCollection<KV<String, PropertyValues>> nCollectionObs =
        currentNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, PropertyValues>> pCollectionObs =
        previousNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, String>> obsDiff =
        DifferUtils.performDiff(nCollectionObs, pCollectionObs, true, true);

    PCollection<KV<String, PropertyValues>> nCollectionSchema =
        currentNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    PCollection<KV<String, PropertyValues>> pCollectionSchema =
        previousNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    PCollection<KV<String, String>> schemaDiff =
        DifferUtils.performDiff(nCollectionSchema, pCollectionSchema, false, true);

    if (options.getOutputLocation() != null) {
      DoFn<KV<String, String>, String> injectDiffTypeFn =
          new DoFn<KV<String, String>, String>() {
            @ProcessElement
            public void processElement(ProcessContext c) {
              KV<String, String> element = c.element();
              String diffType = element.getKey();
              String mcf = element.getValue();
              if (mcf == null || mcf.isEmpty()) return;
              for (String node : mcf.split("\n\n")) {
                String trimmed = node.trim();
                if (!trimmed.isEmpty()) {
                  c.output(trimmed + "\nDiffType: " + diffType + "\n");
                }
              }
            }
          };

      PCollection<String> obsDiffFormatted =
          obsDiff.apply("FormatObsDiff", ParDo.of(injectDiffTypeFn));
      PCollection<String> schemaDiffFormatted =
          schemaDiff.apply("FormatSchemaDiff", ParDo.of(injectDiffTypeFn));

      PCollectionList<String> combinedList =
          PCollectionList.of(obsDiffFormatted).and(schemaDiffFormatted);
      PCollection<String> combinedDiff = combinedList.apply("FlattenDiffs", Flatten.pCollections());
      combinedDiff.apply(
          "WriteCombinedDiff",
          TextIO.write().to(options.getOutputLocation() + "/diff").withSuffix(".mcf"));
    }
    return PCollectionTuple.of(new TupleTag<KV<String, String>>("obsDiff"), obsDiff)
        .and(new TupleTag<KV<String, String>>("schemaDiff"), schemaDiff);
  }
}
