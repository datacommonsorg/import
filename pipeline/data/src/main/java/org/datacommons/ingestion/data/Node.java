package org.datacommons.ingestion.data;

import com.google.cloud.ByteArray;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * Models a graph node. Equality is based on the subjectId only. This is because the subjectId is
 * unique for each node in the graph.
 */
@DefaultCoder(AvroCoder.class)
public class Node implements Serializable {

  private String subjectId;
  private String value;
  @Nullable private byte[] bytes;
  private String name;
  private List<String> types;

  // Default constructor for AvroCoder
  private Node() {}

  // Private constructor to enforce use of Builder
  private Node(Builder builder) {
    this.subjectId = builder.subjectId;
    this.value = builder.value;
    this.bytes = builder.bytes != null ? builder.bytes.toByteArray() : null;
    this.name = builder.name;
    this.types = builder.types != null ? new ArrayList<>(builder.types) : new ArrayList<>();
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getSubjectId() {
    return subjectId;
  }

  public String getValue() {
    return value;
  }

  public ByteArray getBytes() {
    return bytes != null ? ByteArray.copyFrom(bytes) : null;
  }

  public String getName() {
    return name;
  }

  public List<String> getTypes() {
    return types;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Node node = (Node) o;
    return Objects.equals(subjectId, node.subjectId)
        && Objects.equals(value, node.value)
        && Arrays.equals(bytes, node.bytes)
        && Objects.equals(name, node.name)
        && Objects.equals(types, node.types);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subjectId);
  }

  @Override
  public String toString() {
    return String.format(
        "Node{subjectId='%s', value='%s', bytes='%s', name='%s', types=%s}",
        subjectId, value, getBytes(), name, types);
  }

  public static class Builder {
    private String subjectId = "";
    private String value = "";
    private ByteArray bytes = null;
    private String name = "";
    private List<String> types = List.of();

    private Builder() {}

    public Builder subjectId(String subjectId) {
      this.subjectId = subjectId;
      return this;
    }

    public Builder value(String value) {
      this.value = value;
      return this;
    }

    public Builder bytes(ByteArray bytes) {
      this.bytes = bytes;
      return this;
    }

    public Builder name(String name) {
      this.name = name;
      return this;
    }

    public Builder types(List<String> types) {
      this.types = types;
      return this;
    }

    public Node build() {
      if (subjectId == null || subjectId.isEmpty()) {
        throw new IllegalArgumentException("subjectId cannot be null or empty");
      }
      return new Node(this);
    }
  }

  public static AvroCoder<Node> getCoder() {
    return AvroCoder.of(Node.class);
  }
}
