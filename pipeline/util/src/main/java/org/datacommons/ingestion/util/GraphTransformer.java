package org.datacommons.ingestion.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.LogWrapper;
import org.datacommons.util.McfMutator;

/** Transformer for data to convert complex values into nodes. */
public class GraphTransformer extends DoFn<McfGraph, McfGraph> {
  private static final LogWrapper DUMMY_LOG_CTX = new LogWrapper(null);

  @ProcessElement
  public void processElement(@Element McfGraph inputGraph, OutputReceiver<McfGraph> receiver) {
    McfGraph.Builder graphBuilder = inputGraph.toBuilder();
    McfGraph mutated = McfMutator.mutate(graphBuilder, DUMMY_LOG_CTX);
    if (mutated.getNodesCount() > 0) {
      receiver.output(mutated);
    }
  }
}
