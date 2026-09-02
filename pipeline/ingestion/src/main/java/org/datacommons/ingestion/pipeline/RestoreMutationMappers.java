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
import java.io.Serializable;
import org.datacommons.ingestion.spanner.SpannerClient;

/**
 * Utility functions for mapping historical Spanner {@link Struct} records to safe restore {@link
 * Mutation}s. Delegates to the canonical implementations in {@link SpannerClient}.
 */
public final class RestoreMutationMappers implements Serializable {
  private RestoreMutationMappers() {}

  public static Mutation toNodeRestoreMutation(Struct struct, String nodeTableName) {
    return SpannerClient.toNodeRestoreMutation(struct, nodeTableName);
  }

  public static Mutation toEdgeRestoreMutation(Struct struct, String edgeTableName) {
    return SpannerClient.toEdgeRestoreMutation(struct, edgeTableName);
  }

  public static Mutation toTimeSeriesRestoreMutation(Struct struct, String timeSeriesTableName) {
    return SpannerClient.toTimeSeriesRestoreMutation(struct, timeSeriesTableName);
  }

  public static Mutation toObservationRestoreMutation(Struct struct, String observationTableName) {
    return SpannerClient.toObservationRestoreMutation(struct, observationTableName);
  }

  public static Mutation toKeyValueStoreRestoreMutation(
      Struct struct, String keyValueStoreTableName) {
    return SpannerClient.toKeyValueStoreRestoreMutation(struct, keyValueStoreTableName);
  }

  public static Mutation toNodeEmbeddingRestoreMutation(
      Struct struct, String nodeEmbeddingTableName) {
    return SpannerClient.toNodeEmbeddingRestoreMutation(struct, nodeEmbeddingTableName);
  }
}
