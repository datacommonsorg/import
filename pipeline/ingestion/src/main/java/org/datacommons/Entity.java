package org.datacommons;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Models a graph entity (node/edge). */
@DefaultCoder(AvroCoder.class)
public class Entity {
  // Id of the generated Edge.
  String id;
  // SubjectId of the generated Edge.
  String subjectId;
  // Predicate of the generated Edge.
  String predicate;
  // ObjectId of the generated Edge.
  String objectId;
  // ObjectValue of the generatedEdge.
  @Nullable
  String objectValue;
  // Provenance of the generated Edge.
  @Nullable
  String provenance;
  // SubjectId of the generated Node.
  String nodeId;
  // Name of the generated Node.
  @Nullable
  String name;
  // Types of the generated Node.
  @Nullable
  List<String> types;

  public Entity(
      String id,
      String subjectId,
      String predicate,
      String objectId,
      String objectValue,
      String provenance,
      String nodeId,
      String name,
      List<String> types) {
    this.id = id;
    this.subjectId = subjectId;
    this.predicate = predicate;
    this.objectId = objectId;
    this.objectValue = objectValue;
    this.provenance = provenance;
    this.nodeId = nodeId;
    this.name = name;
    this.types = types;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((id == null) ? 0 : id.hashCode());
    result = prime * result + ((subjectId == null) ? 0 : subjectId.hashCode());
    result = prime * result + ((predicate == null) ? 0 : predicate.hashCode());
    result = prime * result + ((objectId == null) ? 0 : objectId.hashCode());
    result = prime * result + ((objectValue == null) ? 0 : objectValue.hashCode());
    result = prime * result + ((provenance == null) ? 0 : provenance.hashCode());
    result = prime * result + ((nodeId == null) ? 0 : nodeId.hashCode());
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    result = prime * result + ((types == null) ? 0 : types.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Entity other = (Entity) obj;
    if (id == null) {
      if (other.id != null)
        return false;
    } else if (!id.equals(other.id))
      return false;
    if (subjectId == null) {
      if (other.subjectId != null)
        return false;
    } else if (!subjectId.equals(other.subjectId))
      return false;
    if (predicate == null) {
      if (other.predicate != null)
        return false;
    } else if (!predicate.equals(other.predicate))
      return false;
    if (objectId == null) {
      if (other.objectId != null)
        return false;
    } else if (!objectId.equals(other.objectId))
      return false;
    if (objectValue == null) {
      if (other.objectValue != null)
        return false;
    } else if (!objectValue.equals(other.objectValue))
      return false;
    if (provenance == null) {
      if (other.provenance != null)
        return false;
    } else if (!provenance.equals(other.provenance))
      return false;
    if (nodeId == null) {
      if (other.nodeId != null)
        return false;
    } else if (!nodeId.equals(other.nodeId))
      return false;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    if (types == null) {
      if (other.types != null)
        return false;
    } else if (!types.equals(other.types))
      return false;
    return true;
  }

  @Override
  public String toString() {
        return String.format("{" +
          "\n\tid: %s" +
          "\n\tsubjectId: %s" +
          "\n\tpredicate: %s" + 
          "\n\tobjectId: %s" +
          "\n\tobjectValue: %s" +
          "\n\tprovenance: %s" +
          "\n\tnodeId: %s" +
          "\n\tname: %s" +
          "\n\ttypes: %s" +
          "\n}", id, subjectId, predicate, objectId, objectValue, provenance, nodeId, name, String.join(", ", types));
  }

  public Mutation toNode(String nodeTableName) {
    return Mutation.newInsertOrUpdateBuilder(nodeTableName)
        .set("subject_id")
        .to(nodeId)
        .set("name")
        .to(name)
        .set("types")
        .to(Value.stringArray(types))
        .build();
  }

  public Mutation toEdge(String edgeTableName) {
    return Mutation.newInsertOrUpdateBuilder(edgeTableName)
        .set("id")
        .to(id)
        .set("subject_id")
        .to(subjectId)
        .set("predicate")
        .to(predicate)
        .set("object_id")
        .to(objectId)
        .set("object_value")
        .to(objectValue)
        .set("provenance")
        .to(provenance)
        .build();
  }

  Entity() {
  }
}