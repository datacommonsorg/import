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

import java.io.Serializable;
import java.util.List;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;

/**
 * Generated embedding content for a node before vector prediction, carrying both the JSON payload
 * stored in Spanner ({@code embeddingContentJson}) and the text sent to the Vertex AI model ({@code
 * textToEmbed}).
 */
public record EmbeddingContent(
    String subjectId,
    String embeddingLabel,
    String embeddingContentKey,
    String embeddingContentJson,
    String textToEmbed,
    List<String> nodeTypes,
    String modelEndpoint,
    String taskType)
    implements Serializable {

  public EmbeddingContent {
    nodeTypes = nodeTypes == null ? List.of() : List.copyOf(nodeTypes);
  }

  /**
   * Converts this content and its generated vector into a canonical {@link NodeEmbeddingRecord}.
   */
  public NodeEmbeddingRecord toNodeEmbeddingRecord(List<Double> embeddings) {
    return new NodeEmbeddingRecord(
        subjectId,
        embeddingLabel,
        embeddingContentKey,
        embeddingContentJson,
        nodeTypes,
        embeddings);
  }
}
