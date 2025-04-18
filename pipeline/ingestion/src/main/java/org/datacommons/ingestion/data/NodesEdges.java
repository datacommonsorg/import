package org.datacommons.ingestion.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Models a collection of nodes and edges. Typically indvidual cache entries are parsed into objects
 * of this class.
 *
 * <p>Objects of this class will only be used in-memory and will not be transmitted in the pipeline.
 */
public class NodesEdges {

  private List<Node> nodes;
  private List<Edge> edges;

  public NodesEdges() {
    this.nodes = new ArrayList<>();
    this.edges = new ArrayList<>();
  }

  public List<Node> getNodes() {
    return nodes;
  }

  public List<Edge> getEdges() {
    return edges;
  }

  public NodesEdges addNode(Node node) {
    this.nodes.add(node);
    return this;
  }

  public NodesEdges addEdge(Edge edge) {
    this.edges.add(edge);
    return this;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodesEdges that = (NodesEdges) o;
    return Objects.equals(nodes, that.nodes) && Objects.equals(edges, that.edges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(nodes, edges);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("NodesEdges {\n");
    sb.append(String.format("\tNodes (%d): [\n", nodes.size()));
    for (Node node : nodes) {
      sb.append("\t\t").append(node.toString()).append("\n");
    }
    sb.append("\t],\n");
    sb.append(String.format("\tEdges (%d): [\n", edges.size()));
    for (Edge edge : edges) {
      sb.append("\t\t").append(edge.toString()).append("\n");
    }
    sb.append("\t]\n");
    sb.append("}");
    return sb.toString();
  }
}
