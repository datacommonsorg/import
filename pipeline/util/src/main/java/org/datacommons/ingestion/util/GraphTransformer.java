package org.datacommons.ingestion.util;

import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.LogWrapper;
import org.datacommons.util.McfMutator;

/** Transformer for data to convert complex values into nodes and enrich graph metadata. */
public class GraphTransformer extends DoFn<McfGraph, McfGraph> {
  private static final LogWrapper DUMMY_LOG_CTX = new LogWrapper(null);
  private final boolean isBaseDc;

  /**
   * Constructs a GraphTransformer with the specified Base DC flag.
   *
   * @param isBaseDc Whether the run is for Base DC. When false, generation of the synthetic
   *     "definition" field on StatisticalVariables is skipped to avoid creating artificial edges.
   */
  public GraphTransformer(boolean isBaseDc) {
    this.isBaseDc = isBaseDc;
  }

  @ProcessElement
  public void processElement(@Element McfGraph inputGraph, OutputReceiver<McfGraph> receiver) {
    McfGraph.Builder graphBuilder = inputGraph.toBuilder();
    McfGraph mutated = McfMutator.mutate(graphBuilder, DUMMY_LOG_CTX, isBaseDc);
    if (mutated.getNodesCount() > 0) {
      receiver.output(mutated);
    }
  }
}
