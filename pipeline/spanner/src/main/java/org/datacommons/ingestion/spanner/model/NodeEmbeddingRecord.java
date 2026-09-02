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

package org.datacommons.ingestion.spanner.model;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.List;
import java.util.Set;

/** Immutable canonical record representing a single row in the Spanner NodeEmbedding table. */
public record NodeEmbeddingRecord(
    String subjectId,
    String embeddingLabel,
    String embeddingContentKey,
    String embeddingContent,
    List<String> nodeTypes,
    List<Double> embeddings)
    implements Serializable {

  public static final String COL_SUBJECT_ID = "subject_id";
  public static final String COL_EMBEDDING_LABEL = "embedding_label";
  public static final String COL_EMBEDDING_CONTENT_KEY = "embedding_content_key";
  public static final String COL_EMBEDDING_CONTENT = "embedding_content";
  public static final String COL_NODE_TYPES = "node_types";
  public static final String COL_EMBEDDINGS = "embeddings";

  public static final List<String> READ_COLUMNS =
      List.of(
          COL_SUBJECT_ID,
          COL_EMBEDDING_LABEL,
          COL_EMBEDDING_CONTENT_KEY,
          COL_EMBEDDING_CONTENT,
          COL_NODE_TYPES,
          COL_EMBEDDINGS);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(
          COL_SUBJECT_ID,
          COL_EMBEDDING_LABEL,
          COL_EMBEDDING_CONTENT_KEY,
          COL_EMBEDDING_CONTENT,
          COL_NODE_TYPES,
          COL_EMBEDDINGS);

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static NodeEmbeddingRecord from(Struct struct) {
    return new NodeEmbeddingRecord(
        struct.getString(COL_SUBJECT_ID),
        struct.getString(COL_EMBEDDING_LABEL),
        struct.getString(COL_EMBEDDING_CONTENT_KEY),
        struct.isNull(COL_EMBEDDING_CONTENT) ? null : struct.getJson(COL_EMBEDDING_CONTENT),
        struct.isNull(COL_NODE_TYPES) ? null : struct.getStringList(COL_NODE_TYPES),
        struct.isNull(COL_EMBEDDINGS) ? null : struct.getDoubleList(COL_EMBEDDINGS));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set(COL_SUBJECT_ID)
            .to(subjectId)
            .set(COL_EMBEDDING_LABEL)
            .to(embeddingLabel)
            .set(COL_EMBEDDING_CONTENT_KEY)
            .to(embeddingContentKey)
            .set(COL_EMBEDDING_CONTENT)
            .to(embeddingContent != null ? Value.json(embeddingContent) : Value.json(null))
            .set(COL_NODE_TYPES)
            .toStringArray(nodeTypes)
            .set(COL_EMBEDDINGS)
            .toFloat64Array(embeddings)
            .build();
    if (!WRITABLE_COLUMNS.equals(mutation.asMap().keySet())) {
      throw new IllegalStateException(
          "Mutation columns "
              + mutation.asMap().keySet()
              + " do not match WRITABLE_COLUMNS "
              + WRITABLE_COLUMNS);
    }
    return mutation;
  }
}
