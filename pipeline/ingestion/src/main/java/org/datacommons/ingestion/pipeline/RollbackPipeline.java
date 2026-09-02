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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.datacommons.ingestion.pipeline.rollback.SpannerRollbackPipeline;
import org.datacommons.ingestion.spanner.SpannerClient;

/**
 * Entrypoint for Spanner time-travel rollback preserving backward compatibility.
 *
 * <p>Implementation logic and modular DoFns live in {@link
 * org.datacommons.ingestion.pipeline.rollback.SpannerRollbackPipeline}.
 */
public class RollbackPipeline implements Serializable {

  public static final String KEY_VALUE_STORE_TABLE = SpannerRollbackPipeline.KEY_VALUE_STORE_TABLE;
  public static final String NODE_EMBEDDING_TABLE = SpannerRollbackPipeline.NODE_EMBEDDING_TABLE;

  public static final TupleTag<Mutation> RESTORE_NODES_TAG =
      SpannerRollbackPipeline.RESTORE_NODES_TAG;
  public static final TupleTag<Mutation> DELETE_NODES_TAG =
      SpannerRollbackPipeline.DELETE_NODES_TAG;

  /** Builds the Beam execution graph for Spanner time-travel rollback. */
  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    SpannerRollbackPipeline.buildPipeline(pipeline, options, spannerClient);
  }

  public static List<String> resolveTargetProvenances(IngestionPipelineOptions options) {
    return SpannerRollbackPipeline.resolveTargetProvenances(options);
  }

  public static void validateRetentionWindow(Timestamp tPre) {
    SpannerRollbackPipeline.validateRetentionWindow(tPre);
  }

  public static PCollection<Void> deleteDataForProvenances(
      Pipeline pipeline,
      List<String> targetProvenances,
      String tableName,
      String columnName,
      SpannerClient spannerClient,
      String emulatorHost) {
    return SpannerRollbackPipeline.deleteDataForProvenances(
        pipeline, targetProvenances, tableName, columnName, spannerClient, emulatorHost);
  }
}
