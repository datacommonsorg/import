package org.datacommons.util;

import org.datacommons.proto.Mcf;

public class McfChecker {
  private Mcf.McfGraph graph;
  private LogWrapper logCtx;

  public McfChecker(Mcf.McfGraph graph, LogWrapper logCtx) {
    this.graph = graph;
    this.logCtx = logCtx;
  }

  public void check() {
    for (String nodeId : graph.getNodesMap().keySet()) {
      Mcf.McfGraph.PropertyValues node = graph.toBuilder().getNodesOrThrow(nodeId);
      checkNode(nodeId, node);
    }
  }

  public void checkNode(String nodeId, Mcf.McfGraph.PropertyValues node) {
  }
}