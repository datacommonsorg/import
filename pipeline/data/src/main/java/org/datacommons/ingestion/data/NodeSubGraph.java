package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Models a graph node paired with its outgoing edges. */
@DefaultCoder(AvroCoder.class)
public class NodeSubGraph implements Serializable {

  private Node node;
  private List<Edge> edges;

  // Default constructor for AvroCoder
  private NodeSubGraph() {}

  // Private constructor to enforce use of Builder
  private NodeSubGraph(Builder builder) {
    this.node = builder.node;
    this.edges = builder.edges != null ? new ArrayList<>(builder.edges) : new ArrayList<>();
  }

  public static Builder builder() {
    return new Builder();
  }

  public Node getNode() {
    return node;
  }

  public List<Edge> getEdges() {
    return edges;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    NodeSubGraph that = (NodeSubGraph) o;
    return Objects.equals(node, that.node) && Objects.equals(edges, that.edges);
  }

  @Override
  public int hashCode() {
    return Objects.hash(node, edges);
  }

  @Override
  public String toString() {
    return String.format("NodeSubGraph{node=%s, edges=%s}", node, edges);
  }

  public static class Builder {
    private Node node;
    private List<Edge> edges = List.of();

    private Builder() {}

    public Builder node(Node node) {
      this.node = node;
      return this;
    }

    public Builder edges(List<Edge> edges) {
      this.edges = edges;
      return this;
    }

    public NodeSubGraph build() {
      Objects.requireNonNull(node, "node cannot be null");
      return new NodeSubGraph(this);
    }
  }

  public static AvroCoder<NodeSubGraph> getCoder() {
    return AvroCoder.of(NodeSubGraph.class);
  }
}
