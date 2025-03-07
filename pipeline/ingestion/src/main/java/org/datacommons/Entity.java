package org.datacommons;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

// Models a graph entity (node/edge).
@DefaultCoder(AvroCoder.class)
public class Entity {
  String id;
  String subjectId;
  String predicate;
  String objectId;
  @Nullable String objectValue;
  @Nullable String provenance;
  @Nullable String name;
  @Nullable List<String> types;

  public Entity(
      String id,
      String subjectId,
      String predicate,
      String objectId,
      String objectValue,
      String provenance,
      String name,
      List<String> types) {
    this.id = id;
    this.subjectId = subjectId;
    this.predicate = predicate;
    this.objectId = objectId;
    this.objectValue = objectValue;
    this.provenance = provenance;
    this.name = name;
    this.types = types;
  }

  public Mutation toNode() {
    return Mutation.newInsertOrUpdateBuilder("Node")
        .set("subject_id")
        .to(subjectId)
        .set("name")
        .to(name)
        .set("types")
        .to(Value.stringArray(types))
        .build();
  }

  public Mutation toEdge() {
    return Mutation.newInsertOrUpdateBuilder("Edge")
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

  Entity() {}
  }