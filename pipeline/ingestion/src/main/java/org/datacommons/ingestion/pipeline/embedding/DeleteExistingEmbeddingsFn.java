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
import java.util.LinkedHashSet;
import java.util.Set;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * Builds primary-key prefix delete {@link Mutation}s in batches of {@link #DELETE_BATCH_SIZE} for a
 * target {@code embedding_label} and candidate {@code subject_id}s, and writes them to Spanner via
 * {@link SpannerClient#writeMutations}.
 */
public class DeleteExistingEmbeddingsFn extends DoFn<Iterable<String>, Mutation> {
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
   * Step 2 of {@link EmbeddingPipeline}: batches candidate {@code subject_id}s, builds prefix
   * delete {@link Mutation}s for {@code embeddingTable} and {@code embeddingLabel}, and writes them
   * via {@link SpannerClient#writeMutations}, returning a completion signal {@link PCollection} for
   * {@code Wait.on}.
   */
  public static PCollection<Void> deleteExistingEmbeddings(
      PCollection<NodeRecord> candidateNodes,
      SpannerClient spannerClient,
      String embeddingTable,
      String embeddingLabel) {
    PCollection<Mutation> deleteMutations =
        buildDeleteMutations(candidateNodes, embeddingTable, embeddingLabel);
    SpannerWriteResult writeResult =
        spannerClient.writeMutations(
            candidateNodes.getPipeline(),
            "WriteDeletedEmbeddings_" + embeddingLabel,
            deleteMutations);
    return writeResult.getOutput();
  }

  static PCollection<Mutation> buildDeleteMutations(
      PCollection<NodeRecord> candidateNodes, String embeddingTable, String embeddingLabel) {
    return candidateNodes
        .apply(
            "KeyCandidateIdsForDelete_" + embeddingLabel,
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.integers(), TypeDescriptors.strings()))
                .via(
                    n ->
                        KV.of(
                            Math.floorMod(n.subjectId().hashCode(), DELETE_SHARDS), n.subjectId())))
        .apply(
            "BatchCandidateIdsForDelete_" + embeddingLabel,
            GroupIntoBatches.ofSize(DELETE_BATCH_SIZE))
        .apply("ExtractCandidateIdBatches_" + embeddingLabel, Values.create())
        .apply(
            "BuildDeleteEmbeddingMutations_" + embeddingLabel,
            ParDo.of(new DeleteExistingEmbeddingsFn(embeddingTable, embeddingLabel)));
  }

  @ProcessElement
  public void processElement(
      @Element Iterable<String> subjectIds, OutputReceiver<Mutation> receiver) {
    if (subjectIds == null) {
      return;
    }

    Set<String> uniqueIds = new LinkedHashSet<>();
    for (String id : subjectIds) {
      if (id != null && !id.isEmpty()) {
        uniqueIds.add(id);
      }
    }

    if (uniqueIds.isEmpty()) {
      return;
    }

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
