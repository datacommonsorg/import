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

import com.google.cloud.spanner.Mutation;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Orchestrates the per-import embedding ingestion sub-DAG inside {@code GraphIngestionPipeline}:
 *
 * <ol>
 *   <li>Deletes existing {@code NodeEmbedding} rows for previously ingested {@code subject_id}s
 *       (when pre-deletion is enabled).
 *   <li>Waits for deletion completion before filtering candidate {@link NodeSubGraph} records and
 *       generating {@link EmbeddingContent} payloads.
 *   <li>Batches content and generates embedding vectors via {@link VertexAiEmbeddingClient}.
 *   <li>Writes resulting {@link Mutation}s to {@code NodeEmbedding} after parent {@code Node}
 *       writes finish (to satisfy Spanner interleaving constraints).
 * </ol>
 */
public final class SpannerEmbeddingPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerEmbeddingPipeline.class);

  private SpannerEmbeddingPipeline() {}

  /**
   * Attaches per-spec embedding deletion, content generation, vector prediction, and Spanner write
   * transforms for a single import. All Beam transform names are scoped by {@code embeddingLabel}
   * and {@code importName} so multiple imports can coexist inside the same {@link Pipeline}.
   */
  public static void attachTransforms(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      String provenance,
      EmbeddingConfig config,
      VertexAiEmbeddingClient embeddingClient,
      String embeddingTableName,
      int embeddingBatchSize,
      PCollection<NodeSubGraph> nodeSubGraphs,
      PCollection<Void> writtenNodesWait,
      boolean skipDelete) {
    LOGGER.info(
        "Configuring embedding ingestion for import '{}' with {} spec(s) targeting table '{}'"
            + " (skipDelete={})",
        importName,
        config.specs().size(),
        embeddingTableName,
        skipDelete);

    for (EmbeddingSpec spec : config.specs()) {
      String label = spec.embeddingLabel();
      String stageSuffix = label + "-" + importName;

      // 1. Delete existing NodeEmbedding rows for previously ingested subject_ids.
      PCollection<Void> deleteWaitSignal =
          skipDelete
              ? null
              : DeleteExistingEmbeddingsFn.deleteExistingEmbeddings(
                  pipeline, spannerClient, importName, embeddingTableName, label, provenance);

      // 2. Run embedding content processing strictly after deletion completes.
      PCollection<NodeSubGraph> gatedNodeSubGraphs =
          deleteWaitSignal != null
              ? nodeSubGraphs.apply(
                  "WaitForDeletedEmbeddingsBeforeContent_" + stageSuffix, Wait.on(deleteWaitSignal))
              : nodeSubGraphs;

      PCollection<NodeSubGraph> candidateNodes =
          ReadCandidateNodesFn.readCandidateNodes(importName, gatedNodeSubGraphs, spec);
      PCollection<EmbeddingContent> contents =
          GenerateEmbeddingContentFn.generateEmbeddingContent(importName, candidateNodes, spec);

      // 3. Call Vertex AI in batches to generate embedding vectors and Spanner mutations.
      PCollection<Mutation> mutations =
          GenerateEmbeddingsFn.buildEmbeddingMutations(
              importName, contents, embeddingClient, embeddingTableName, spec, embeddingBatchSize);

      // 4. Write NodeEmbedding mutations after parent Node writes finish (interleaved table).
      PCollection<Mutation> mutationsToWrite =
          writtenNodesWait != null
              ? mutations.apply(
                  "WaitForWrittenNodesForEmbeddings_" + stageSuffix, Wait.on(writtenNodesWait))
              : mutations;
      spannerClient.writeMutations(
          pipeline, "WriteNodeEmbeddings_" + stageSuffix, mutationsToWrite);
    }
  }
}
