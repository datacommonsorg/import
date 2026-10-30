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

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * High-level orchestrator for the node embedding content generation pipeline.
 *
 * <p>For each {@link EmbeddingSpec} in {@link EmbeddingConfig}, orchestrates three sequential
 * stages:
 *
 * <ol>
 *   <li><b>Read Candidate Nodes</b>: Reads and filters candidate {@link NodeRecord}s from Spanner
 *       by {@code node_types} and optional {@code ENTITY_TYPES} ({@link
 *       ReadCandidateNodesFn#readCandidateNodes}).
 *   <li><b>Delete Existing Embeddings</b>: Deletes existing rows in {@code NodeEmbedding} for the
 *       candidate {@code subject_id}s and {@code embedding_label} ({@link
 *       DeleteExistingEmbeddingsFn#deleteExistingEmbeddings}).
 *   <li><b>Generate Embedding Content</b>: Waits for pre-deletion to finish, resolves any
 *       configured predicate properties from Spanner, and generates {@link EmbeddingContent}
 *       payloads with FarmHash64 content keys ({@link
 *       GenerateEmbeddingContentFn#generateEmbeddingContent}).
 * </ol>
 */
public final class EmbeddingPipeline {

  private EmbeddingPipeline() {}

  /**
   * Builds the embedding content generation pipeline for all specs in {@link EmbeddingConfig}.
   *
   * <p>TODO: Temporarily returns {@link PCollection}s while {@link EmbeddingPipeline} is under
   * development; once vector generation and Spanner writes to {@code embeddingTable} are wired in,
   * change return type to {@code void}.
   *
   * @return a map from {@code embedding_label} to the generated {@link EmbeddingContent} {@link
   *     PCollection}
   */
  public static Map<String, PCollection<EmbeddingContent>> build(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String embeddingTable,
      EmbeddingConfig config) {
    Map<String, PCollection<EmbeddingContent>> contentByLabel = new LinkedHashMap<>();
    for (EmbeddingSpec spec : config.specs()) {
      contentByLabel.put(
          spec.embeddingLabel(), buildForSpec(pipeline, spannerClient, embeddingTable, spec));
    }
    return contentByLabel;
  }

  /**
   * Builds the pipeline for a single {@link EmbeddingSpec}:
   *
   * <pre>
   *   readCandidateNodes -> deleteExistingEmbeddings -> Wait.on -> generateEmbeddingContent
   * </pre>
   *
   * <p>TODO: Temporarily returns {@code PCollection<EmbeddingContent>} during development; end goal
   * is to write generated embeddings to {@code embeddingTable} and return {@code void}.
   */
  public static PCollection<EmbeddingContent> buildForSpec(
      Pipeline pipeline, SpannerClient spannerClient, String embeddingTable, EmbeddingSpec spec) {
    String label = spec.embeddingLabel();

    // 1. Read candidate nodes from Spanner
    PCollection<NodeRecord> candidateNodes =
        ReadCandidateNodesFn.readCandidateNodes(pipeline, spannerClient, spec);

    // 2. Delete existing embeddings for candidate nodes
    PCollection<Void> deleteDone =
        DeleteExistingEmbeddingsFn.deleteExistingEmbeddings(
            candidateNodes, spannerClient, embeddingTable, label);

    // 3. Wait for deletion to complete, then generate embedding content
    // TODO: Chain vector prediction and Spanner writes to embeddingTable instead of returning
    // PCollection.
    PCollection<NodeRecord> gatedCandidates =
        candidateNodes.apply("WaitOnDeleteExistingEmbeddings_" + label, Wait.on(deleteDone));
    return GenerateEmbeddingContentFn.generateEmbeddingContent(
        gatedCandidates, spannerClient, spec);
  }
}
