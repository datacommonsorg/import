// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.io.Serializable;

/**
 * Utility functions for mapping historical Spanner {@link Struct} records to safe restore {@link
 * Mutation}s.
 *
 * <p>Encapsulates all column projection rules, null-safety guards, and Spanner datatype mappings
 * (e.g. omitting STORED generated columns in TimeSeries, serializing JSON, and handling byte
 * arrays).
 */
public final class RestoreMutationMappers implements Serializable {
  private RestoreMutationMappers() {}

  public static Mutation toNodeRestoreMutation(Struct struct, String nodeTableName) {
    return Mutation.newInsertOrUpdateBuilder(nodeTableName)
        .set("subject_id")
        .to(struct.getString("subject_id"))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .set("value")
        .to(struct.isNull("value") ? null : struct.getString("value"))
        .set("bytes")
        .to(struct.isNull("bytes") ? null : struct.getBytes("bytes"))
        .set("name")
        .to(struct.isNull("name") ? null : struct.getString("name"))
        .set("types")
        .toStringArray(struct.isNull("types") ? null : struct.getStringList("types"))
        .build();
  }

  public static Mutation toEdgeRestoreMutation(Struct struct, String edgeTableName) {
    return Mutation.newInsertOrUpdateBuilder(edgeTableName)
        .set("subject_id")
        .to(struct.getString("subject_id"))
        .set("predicate")
        .to(struct.getString("predicate"))
        .set("object_id")
        .to(struct.getString("object_id"))
        .set("provenance")
        .to(struct.getString("provenance"))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation toTimeSeriesRestoreMutation(Struct struct, String timeSeriesTableName) {
    return Mutation.newInsertOrUpdateBuilder(timeSeriesTableName)
        .set("variable_measured")
        .to(struct.getString("variable_measured"))
        // entity1 is a STORED generated column; DO NOT write to it directly!
        .set("extra_entities_id")
        .to(struct.getString("extra_entities_id"))
        .set("facet_id")
        .to(struct.getString("facet_id"))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .set("entities")
        .to(struct.isNull("entities") ? Value.json(null) : Value.json(struct.getJson("entities")))
        .set("facet")
        .to(struct.isNull("facet") ? Value.json(null) : Value.json(struct.getJson("facet")))
        .build();
  }

  public static Mutation toObservationRestoreMutation(Struct struct, String observationTableName) {
    return Mutation.newInsertOrUpdateBuilder(observationTableName)
        .set("variable_measured")
        .to(struct.getString("variable_measured"))
        .set("entity1")
        .to(struct.getString("entity1"))
        .set("extra_entities_id")
        .to(struct.getString("extra_entities_id"))
        .set("facet_id")
        .to(struct.getString("facet_id"))
        .set("date")
        .to(struct.getString("date"))
        .set("value")
        .to(struct.getString("value"))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation toKeyValueStoreRestoreMutation(
      Struct struct, String keyValueStoreTableName) {
    return Mutation.newInsertOrUpdateBuilder(keyValueStoreTableName)
        .set("type")
        .to(struct.getString("type"))
        .set("key")
        .to(struct.getString("key"))
        .set("provenance")
        .to(struct.getString("provenance"))
        .set("value")
        .to(struct.isNull("value") ? Value.json(null) : Value.json(struct.getJson("value")))
        .build();
  }

  public static Mutation toNodeEmbeddingRestoreMutation(
      Struct struct, String nodeEmbeddingTableName) {
    return Mutation.newInsertOrUpdateBuilder(nodeEmbeddingTableName)
        .set("subject_id")
        .to(struct.getString("subject_id"))
        .set("embedding_label")
        .to(struct.getString("embedding_label"))
        .set("embedding_content_key")
        .to(struct.getString("embedding_content_key"))
        .set("embedding_content")
        .to(
            struct.isNull("embedding_content")
                ? Value.json(null)
                : Value.json(struct.getJson("embedding_content")))
        .set("node_types")
        .toStringArray(struct.isNull("node_types") ? null : struct.getStringList("node_types"))
        .set("embeddings")
        .toFloat64Array(struct.isNull("embeddings") ? null : struct.getDoubleList("embeddings"))
        .build();
  }
}
