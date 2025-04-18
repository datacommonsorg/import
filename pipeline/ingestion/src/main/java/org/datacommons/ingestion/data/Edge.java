package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Objects;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

import com.google.common.hash.Hashing;

/** Models a graph edge. */
@DefaultCoder(AvroCoder.class)
public class Edge implements Serializable {

  private String subjectId;
  private String predicate;
  private String objectId;
  private String objectValue;
  private String provenance;
  private String objectHash;

  // Private constructor to enforce use of Builder
  private Edge(Builder builder) {
    this.subjectId = builder.subjectId;
    this.predicate = builder.predicate;
    this.objectId = builder.objectId;
    this.objectValue = builder.objectValue;
    this.provenance = builder.provenance;
    this.objectHash = builder.objectHash;
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

  public String getObjectValue() {
    return objectValue;
  }

  public String getProvenance() {
    return provenance;
  }

  public String getObjectHash() {
    return objectHash;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Edge edge = (Edge) o;
    return subjectId.equals(edge.subjectId)
        && predicate.equals(edge.predicate)
        && objectId.equals(edge.objectId)
        && Objects.equals(objectHash, edge.objectHash);
  }

  @Override
  public int hashCode() {
    return Objects.hash(subjectId, predicate, objectId, objectHash);
  }

  @Override
  public String toString() {
    return String.format(
        "Edge{subjectId='%s', predicate='%s', objectId='%s', objectValue='%s', provenance='%s', objectHash='%s'}",
        subjectId, predicate, objectId, objectValue, provenance, objectHash);
  }

  public static class Builder {
    private String subjectId = "";
    private String predicate = "";
    private String objectId = "";
    private String objectValue = "";
    private String provenance = "";
    private String objectHash = "";

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

    public Builder objectValue(String objectValue) {
      this.objectValue = objectValue;
      this.objectHash = generateSha256(objectValue);
      return this;
    }

    public Builder provenance(String provenance) {
      this.provenance = provenance;
      return this;
    }

    public Builder objectHash(String objectHash) {
      this.objectHash = objectHash;
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

    private String generateSha256(String input) {
      if (input == null || input.isEmpty()) {
        return "";
      }
      return Base64.getEncoder()
          .encodeToString(Hashing.sha256().hashString(input, StandardCharsets.UTF_8).asBytes());
    }
  }

  public static AvroCoder<Edge> getCoder() {
    return AvroCoder.of(Edge.class);
  }
}
