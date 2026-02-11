package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Models a graph edge. */
@DefaultCoder(AvroCoder.class)
public class Edge implements Serializable {

  private String subjectId;
  private String predicate;
  private String objectId;
  private String provenance;

  // Private constructor to enforce use of Builder
  private Edge(Builder builder) {
    this.subjectId = builder.subjectId;
    this.predicate = builder.predicate;
    this.objectId = builder.objectId;
    this.provenance = builder.provenance;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getSubjectId() {
    return subjectId;
  }

  public String getPredicate() {
    return predicate;
  }

  public String getObjectId() {
    return objectId;
  }

  public String getProvenance() {
    return provenance;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Edge edge = (Edge) o;
    return subjectId.equals(edge.subjectId)
        && predicate.equals(edge.predicate)
        && objectId.equals(edge.objectId)
        && provenance.equals(edge.provenance);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subjectId, predicate, objectId, provenance);
  }

  @Override
  public String toString() {
    return String.format(
        "Edge{subjectId='%s', predicate='%s', objectId='%s', provenance='%s'}",
        subjectId, predicate, objectId, provenance);
  }

  public static class Builder {
    private String subjectId = "";
    private String predicate = "";
    private String objectId = "";
    private String provenance = "";

    private Builder() {}

    public Builder subjectId(String subjectId) {
      this.subjectId = subjectId;
      return this;
    }

    public Builder predicate(String predicate) {
      this.predicate = predicate;
      return this;
    }

    public Builder objectId(String objectId) {
      this.objectId = objectId;
      return this;
    }

    public Builder provenance(String provenance) {
      this.provenance = provenance;
      return this;
    }

    public Edge build() {
      if (subjectId == null || subjectId.isEmpty()) {
        throw new IllegalArgumentException("subjectId cannot be null or empty");
      }
      if (predicate == null || predicate.isEmpty()) {
        throw new IllegalArgumentException("predicate cannot be null or empty");
      }
      if (objectId == null || objectId.isEmpty()) {
        throw new IllegalArgumentException("objectId cannot be null or empty");
      }
      return new Edge(this);
    }
  }

  public static AvroCoder<Edge> getCoder() {
    return AvroCoder.of(Edge.class);
  }
}
