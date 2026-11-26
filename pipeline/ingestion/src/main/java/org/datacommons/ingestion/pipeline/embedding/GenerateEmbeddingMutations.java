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
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Helper building per-spec {@code NodeEmbedding} mutation {@link PCollection}s for an import inside
 * {@code GraphIngestionPipeline}:
 *
 * <ol>
 *   <li>Filters candidate {@link NodeSubGraph} records per {@link EmbeddingSpec}.
 *   <li>Generates {@link EmbeddingContent} payloads per spec.
 *   <li>Batches content and generates embedding vectors as Spanner {@link Mutation}s via {@link
 *       VertexAiEmbeddingClient}.
 * </ol>
 */
public final class GenerateEmbeddingMutations {
  private static final Logger LOGGER = LoggerFactory.getLogger(GenerateEmbeddingMutations.class);

  private GenerateEmbeddingMutations() {}

  /**
   * Builds per-spec {@code NodeEmbedding} {@link Mutation} collections for a single import. All
   * Beam transform names are scoped by {@code embeddingLabel} and {@code importName} so multiple
   * imports can coexist inside the same pipeline.
   */
  public static List<PCollection<Mutation>> buildEmbeddingMutations(
      String importName,
      EmbeddingConfig config,
      VertexAiEmbeddingClient embeddingClient,
      String embeddingTableName,
      int embeddingBatchSize,
      PCollection<NodeSubGraph> nodeSubGraphs) {
    LOGGER.info(
        "Configuring embedding generation for import '{}' with {} spec(s) targeting table '{}'",
        importName,
        config.specs().size(),
        embeddingTableName);

    List<PCollection<Mutation>> embeddingMutations = new ArrayList<>(config.specs().size());
    for (EmbeddingSpec spec : config.specs()) {
      PCollection<NodeSubGraph> candidateNodes =
          ReadCandidateNodesFn.readCandidateNodes(importName, nodeSubGraphs, spec);
      PCollection<EmbeddingContent> contents =
          GenerateEmbeddingContentFn.generateEmbeddingContent(importName, candidateNodes, spec);
      embeddingMutations.add(
          GenerateEmbeddingsFn.buildEmbeddingMutations(
              importName, contents, embeddingClient, embeddingTableName, spec, embeddingBatchSize));
    }
    return embeddingMutations;
  }
}
