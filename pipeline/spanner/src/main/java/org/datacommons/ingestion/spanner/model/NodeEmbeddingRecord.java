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
import org.datacommons.ingestion.spanner.SpannerClient;

/** Immutable canonical record representing a single row in the Spanner NodeEmbedding table. */
public record NodeEmbeddingRecord(
    String subjectId,
    String embeddingLabel,
    String embeddingContentKey,
    String embeddingContent,
    List<String> nodeTypes,
    List<Double> embeddings)
    implements Serializable {

  public static final List<String> READ_COLUMNS =
      List.of(
          SpannerClient.COL_SUBJECT_ID,
          SpannerClient.COL_EMBEDDING_LABEL,
          SpannerClient.COL_EMBEDDING_CONTENT_KEY,
          SpannerClient.COL_EMBEDDING_CONTENT,
          SpannerClient.COL_NODE_TYPES,
          SpannerClient.COL_EMBEDDINGS);

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static NodeEmbeddingRecord from(Struct struct) {
    return new NodeEmbeddingRecord(
        struct.getString(SpannerClient.COL_SUBJECT_ID),
        struct.getString(SpannerClient.COL_EMBEDDING_LABEL),
        struct.getString(SpannerClient.COL_EMBEDDING_CONTENT_KEY),
        struct.isNull(SpannerClient.COL_EMBEDDING_CONTENT)
            ? null
            : struct.getJson(SpannerClient.COL_EMBEDDING_CONTENT),
        struct.isNull(SpannerClient.COL_NODE_TYPES)
            ? null
            : struct.getStringList(SpannerClient.COL_NODE_TYPES),
        struct.isNull(SpannerClient.COL_EMBEDDINGS)
            ? null
            : struct.getDoubleList(SpannerClient.COL_EMBEDDINGS));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set(SpannerClient.COL_SUBJECT_ID)
        .to(subjectId)
        .set(SpannerClient.COL_EMBEDDING_LABEL)
        .to(embeddingLabel)
        .set(SpannerClient.COL_EMBEDDING_CONTENT_KEY)
        .to(embeddingContentKey)
        .set(SpannerClient.COL_EMBEDDING_CONTENT)
        .to(embeddingContent != null ? Value.json(embeddingContent) : Value.json(null))
        .set(SpannerClient.COL_NODE_TYPES)
        .toStringArray(nodeTypes)
        .set(SpannerClient.COL_EMBEDDINGS)
        .toFloat64Array(embeddings)
        .build();
  }
}
