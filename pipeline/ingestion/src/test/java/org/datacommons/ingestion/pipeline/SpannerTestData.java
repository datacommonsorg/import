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
import com.google.cloud.spanner.Value;
import java.util.List;

/** Reusable sample data fixtures and mutation builders for Spanner integration tests. */
public final class SpannerTestData {

  private SpannerTestData() {}

  // Common Test Identifiers
  public static final String SUBJECT_ID_CA = "geoId/06";
  public static final String SUBJECT_ID_DIRTY = "geoId/999";
  public static final String PROVENANCE = "dc/base/TestImport";
  public static final String STAT_VAR = "Count_Person";
  public static final String FACET_ID = "F1";
  public static final String DATE_2020 = "2020";
  public static final String DATE_2021 = "2021";
  public static final String VALUE_2020 = "39000000";
  public static final String VALUE_2021 = "40000000";
  public static final String KV_VALUE_BASELINE = "{\"obs_count\": 1}";
  public static final String KV_VALUE_CORRUPTED = "{\"obs_count\": 999}";
  public static final String EMBEDDING_LABEL = "test-label";
  public static final String EMBEDDING_KEY = "test-key";
  public static final String EMBEDDING_CONTENT_V1 = "{\"text\":\"California\"}";
  public static final String EMBEDDING_CONTENT_V2 = "{\"text\":\"Corrupted California\"}";

  // Other Import Identifiers (For Provenance Isolation Testing)
  public static final String OTHER_PROVENANCE = "dc/base/OtherImport";
  public static final String SUBJECT_ID_OTHER = "geoId/02";
  public static final String STAT_VAR_OTHER = "UnemploymentRate";
  public static final String OTHER_OBS_VALUE = "5.2";
  public static final String OTHER_KV_VALUE = "{\"obs_count\": 50}";

  // Mutation Factory Methods
  public static Mutation nodeMutation(String subjectId, String name, List<String> types) {
    return Mutation.newInsertOrUpdateBuilder("Node")
        .set("subject_id")
        .to(subjectId)
        .set("name")
        .to(name)
        .set("types")
        .toStringArray(types)
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation nodeEmbeddingMutation(
      String subjectId,
      String embeddingLabel,
      String embeddingContentKey,
      String embeddingContent,
      List<String> nodeTypes,
      List<Double> embeddings) {
    var builder =
        Mutation.newInsertOrUpdateBuilder("NodeEmbedding")
            .set("subject_id")
            .to(subjectId)
            .set("embedding_label")
            .to(embeddingLabel)
            .set("embedding_content_key")
            .to(embeddingContentKey)
            .set("embedding_content")
            .to(Value.json(embeddingContent))
            .set("node_types")
            .toStringArray(nodeTypes);
    if (embeddings != null) {
      builder.set("embeddings").toFloat64Array(embeddings);
    }
    return builder.build();
  }

  public static Mutation edgeMutation(
      String subjectId, String predicate, String objectId, String provenance) {
    return Mutation.newInsertOrUpdateBuilder("Edge")
        .set("subject_id")
        .to(subjectId)
        .set("predicate")
        .to(predicate)
        .set("object_id")
        .to(objectId)
        .set("provenance")
        .to(provenance)
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation timeSeriesMutation(
      String variableMeasured, String entity1, String facetId, String provenance) {
    return Mutation.newInsertOrUpdateBuilder("TimeSeries")
        .set("variable_measured")
        .to(variableMeasured)
        .set("extra_entities_id")
        .to("")
        .set("facet_id")
        .to(facetId)
        .set("entities")
        .to(Value.json(String.format("{\"entity1\":\"%s\"}", entity1)))
        .set("facet")
        .to(Value.json(String.format("{\"provenance\":\"%s\"}", provenance)))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation observationMutation(
      String variableMeasured, String entity1, String facetId, String date, String value) {
    return Mutation.newInsertOrUpdateBuilder("Observation")
        .set("variable_measured")
        .to(variableMeasured)
        .set("entity1")
        .to(entity1)
        .set("extra_entities_id")
        .to("")
        .set("facet_id")
        .to(facetId)
        .set("date")
        .to(date)
        .set("value")
        .to(value)
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  public static Mutation keyValueMutation(String key, String provenance, String jsonValue) {
    return Mutation.newInsertOrUpdateBuilder("KeyValueStore")
        .set("type")
        .to("ProvenanceSummary")
        .set("key")
        .to(key)
        .set("provenance")
        .to(provenance)
        .set("value")
        .to(Value.json(jsonValue))
        .build();
  }

  // Pre-assembled Mutation Sets
  public static final List<Mutation> V1_BASELINE_MUTATIONS =
      List.of(
          nodeMutation(SUBJECT_ID_CA, "California", List.of("State", "Place")),
          nodeEmbeddingMutation(
              SUBJECT_ID_CA,
              EMBEDDING_LABEL,
              EMBEDDING_KEY,
              EMBEDDING_CONTENT_V1,
              List.of("State"),
              java.util.Collections.nCopies(768, 0.1)),
          edgeMutation(SUBJECT_ID_CA, "typeOf", "Place", PROVENANCE),
          timeSeriesMutation(STAT_VAR, SUBJECT_ID_CA, FACET_ID, PROVENANCE),
          observationMutation(STAT_VAR, SUBJECT_ID_CA, FACET_ID, DATE_2020, VALUE_2020),
          keyValueMutation(STAT_VAR, PROVENANCE, KV_VALUE_BASELINE));

  public static final List<Mutation> V2_CORRUPTED_MUTATIONS =
      List.of(
          nodeMutation(SUBJECT_ID_CA, "Corrupted California", List.of("CorruptedType")),
          nodeEmbeddingMutation(
              SUBJECT_ID_CA,
              EMBEDDING_LABEL,
              EMBEDDING_KEY,
              EMBEDDING_CONTENT_V2,
              List.of("CorruptedType"),
              java.util.Collections.nCopies(768, 0.9)),
          nodeMutation(SUBJECT_ID_DIRTY, "Dirty Node", List.of("Place")),
          nodeEmbeddingMutation(
              SUBJECT_ID_DIRTY,
              EMBEDDING_LABEL,
              EMBEDDING_KEY,
              "{\"text\":\"Dirty\"}",
              List.of("Place"),
              java.util.Collections.nCopies(768, 0.5)),
          edgeMutation(SUBJECT_ID_DIRTY, "typeOf", "Place", PROVENANCE),
          observationMutation(STAT_VAR, SUBJECT_ID_CA, FACET_ID, DATE_2021, VALUE_2021),
          keyValueMutation(STAT_VAR, PROVENANCE, KV_VALUE_CORRUPTED));

  public static final List<Mutation> OTHER_IMPORT_BASELINE_MUTATIONS =
      List.of(nodeMutation(SUBJECT_ID_OTHER, "Alaska", List.of("State", "Place")));

  public static final List<Mutation> OTHER_IMPORT_CONCURRENT_MUTATIONS =
      List.of(
          edgeMutation(SUBJECT_ID_OTHER, "typeOf", "Place", OTHER_PROVENANCE),
          timeSeriesMutation(STAT_VAR_OTHER, SUBJECT_ID_OTHER, FACET_ID, OTHER_PROVENANCE),
          observationMutation(
              STAT_VAR_OTHER, SUBJECT_ID_OTHER, FACET_ID, DATE_2020, OTHER_OBS_VALUE),
          keyValueMutation(STAT_VAR_OTHER, OTHER_PROVENANCE, OTHER_KV_VALUE));
}
