package org.datacommons;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Mutation;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

@DefaultCoder(AvroCoder.class)
public class Triple {
  String id;
  @Nullable String provId;
  @Nullable String subjectId;
  @Nullable String predicate;
  @Nullable String objectId;

  public Triple(
      String id,
      String provId,
      String subjectId,
      String predicate,
      String objectId,
      String objectValue) {
    this.id = id;
    this.provId = provId;
    this.subjectId = subjectId;
    this.predicate = predicate;
    this.objectId = objectId;
  }

  Triple() {}

  static Triple fromTableRow(TableRow row) {
    Triple triple = new Triple();
    if (row.get("id") != null) {
      triple.id = (String) row.get("id");
    }
    if (row.get("prov_id") != null) {
      triple.provId = (String) row.get("prov_id");
    }
    if (row.get("subject_id") != null) {
      triple.subjectId = (String) row.get("subject_id");
    }
    if (row.get("predicate") != null) {
      triple.predicate = (String) row.get("predicate");
    }
    if (row.get("object_id") != null) {
      triple.objectId = (String) row.get("object_id");
    }
    return triple;
  }

  public Mutation toMutation() {
    return Mutation.newInsertOrUpdateBuilder("Triples")
        .set("Id")
        .to(id)
        .set("ProvId")
        .to(provId)
        .set("SubjecTId")
        .to(subjectId)
        .set("Predicate")
        .to(predicate)
        .set("ObjectId")
        .to(objectId)
        .build();
  }
  }