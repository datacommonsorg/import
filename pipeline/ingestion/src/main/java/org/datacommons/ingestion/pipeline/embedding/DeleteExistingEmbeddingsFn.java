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

package org.datacommons.ingestion.pipeline.embedding;

import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;

/**
 * Deletes existing rows in {@code NodeEmbedding} by querying distinct {@code subject_id}s from
 * {@code Edge} for a given {@code provenance}, batching them in chunks of {@link
 * #DELETE_BATCH_SIZE}, and writing primary-key prefix delete {@link Mutation}s via {@link
 * SpannerClient#writeMutations}.
 *
 * <p>TODO: This class will help migrate the existing {@code NodeEmbedding} table to include {@code
 * provenance} (by deleting legacy embedding rows without {@code provenance} via {@code subject_id}s
 * from {@code Edge}). Once {@code NodeEmbedding} includes {@code provenance}, provenance-scoped
 * deletion will be handled at the top level via {@link SpannerClient#deleteDataForImport}.
 */
public class DeleteExistingEmbeddingsFn extends DoFn<List<String>, Mutation> {
  public static final int DELETE_BATCH_SIZE = 1000;
  private static final int DELETE_SHARDS = 32;

  private final String embeddingTable;
  private final String embeddingLabel;
  private final Counter deletedNodesCounter;

  public DeleteExistingEmbeddingsFn(String embeddingTable, String embeddingLabel) {
    this.embeddingTable = embeddingTable;
    this.embeddingLabel = embeddingLabel;
    this.deletedNodesCounter =
        Metrics.counter(
            DeleteExistingEmbeddingsFn.class, "embedding_deleted_nodes:" + embeddingLabel);
  }

  /**
   * Reads distinct {@code subject_id}s from {@code Edge} for {@code provenance}, builds prefix
   * delete {@link Mutation}s for {@code embeddingTable} and {@code embeddingLabel}, and writes them
   * via {@link SpannerClient#writeMutations}, returning a completion signal {@link PCollection} for
   * {@code Wait.on}.
   */
  public static PCollection<Void> deleteExistingEmbeddings(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String embeddingTable,
      String embeddingLabel,
      String provenance) {
    PCollection<String> subjectIds =
        readSubjectIdsByProvenance(pipeline, spannerClient, provenance, embeddingLabel);
    PCollection<Mutation> deleteMutations =
        buildDeleteMutations(subjectIds, embeddingTable, embeddingLabel);
    SpannerWriteResult writeResult =
        spannerClient.writeMutations(
            pipeline, "WriteDeletedEmbeddings_" + embeddingLabel, deleteMutations);
    return writeResult.getOutput();
  }

  /**
   * Reads distinct {@code subject_id}s from the {@code Edge} table constrained to {@code
   * provenance}.
   */
  public static PCollection<String> readSubjectIdsByProvenance(
      Pipeline pipeline, SpannerClient spannerClient, String provenance, String stageSuffix) {
    Statement stmt = buildSubjectIdsByProvenanceStatement(spannerClient, provenance);
    return pipeline
        .apply(
            "ReadEdgeSubjectIdsForDelete_" + stageSuffix,
            spannerClient.getReadTransform().withQuery(stmt))
        .apply(
            "ExtractEdgeSubjectIdForDelete_" + stageSuffix,
            MapElements.into(TypeDescriptors.strings())
                .via(struct -> struct.getString(EdgeRecord.COL_SUBJECT_ID)));
  }

  /**
   * Builds the partitioned Spanner SQL {@link Statement} to read distinct {@code subject_id}s from
   * {@code Edge} for {@code provenance}.
   */
  static Statement buildSubjectIdsByProvenanceStatement(
      SpannerClient spannerClient, String provenance) {
    String sql =
        spannerClient.formatPartitionQuery(
            "SELECT DISTINCT %s FROM %s WHERE %s = @provenance",
            EdgeRecord.COL_SUBJECT_ID, spannerClient.getEdgeTableName(), EdgeRecord.COL_PROVENANCE);
    return Statement.newBuilder(sql).bind(EdgeRecord.COL_PROVENANCE).to(provenance).build();
  }

  static PCollection<Mutation> buildDeleteMutations(
      PCollection<String> subjectIds, String embeddingTable, String embeddingLabel) {
    return subjectIds
        .apply(
            "KeyCandidateIdsForDelete_" + embeddingLabel,
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                .via(id -> KV.of(Math.floorMod(id.hashCode(), DELETE_SHARDS), id)))
        .apply(
            "BatchCandidateIdsForDelete_" + embeddingLabel,
            GroupIntoBatches.ofSize(DELETE_BATCH_SIZE))
        .apply(
            "ExtractCandidateIdBatches_" + embeddingLabel,
            MapElements.into(TypeDescriptors.lists(TypeDescriptors.strings()))
                .via(kv -> Lists.newArrayList(kv.getValue())))
        .apply(
            "BuildDeleteEmbeddingMutations_" + embeddingLabel,
            ParDo.of(new DeleteExistingEmbeddingsFn(embeddingTable, embeddingLabel)));
  }

  @ProcessElement
  public void processElement(@Element List<String> subjectIds, OutputReceiver<Mutation> receiver) {
    if (subjectIds == null) {
      return;
    }

    ImmutableSet<String> uniqueIds =
        subjectIds.stream()
            .filter(id -> id != null && !id.isEmpty())
            .collect(ImmutableSet.toImmutableSet());

    deletedNodesCounter.inc(uniqueIds.size());
    receiver.output(buildDeleteMutation(embeddingTable, embeddingLabel, uniqueIds));
  }

  /**
   * Builds a {@link Mutation#delete(String, KeySet)} targeting the composite primary key prefix
   * {@code (subject_id, embedding_label)} for each subject ID in {@code subjectIds}.
   */
  public static Mutation buildDeleteMutation(
      String embeddingTable, String embeddingLabel, Iterable<String> subjectIds) {
    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String id : subjectIds) {
      if (id != null && !id.isEmpty()) {
        keySetBuilder.addRange(
            KeyRange.prefix(com.google.cloud.spanner.Key.of(id, embeddingLabel)));
      }
    }
    return Mutation.delete(embeddingTable, keySetBuilder.build());
  }
}
