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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Specification for a single embedding generation setup.
 *
 * <p>Supports the following fields: 1. embedding_label: The label of the embedding, used to
 * identify the embedding 2. modelName: The name of the model to use for embedding generation 3.
 * modelEndpoint: The endpoint of the model to use for embedding generation 4. taskType: The task
 * type for embedding generation 5. nodeTypes: The node types to use for embedding generation, this
 * is a map of node_type with the list of properties to include for the node 6. nodeFilterType: The
 * type of node filter to use for embedding generation
 */
public record EmbeddingSpec(
    String embeddingLabel,
    String modelName,
    String modelEndpoint,
    String taskType,
    Map<String, List<String>> nodeTypes,
    NodeFilterType nodeFilterType)
    implements Serializable {

  /** Supported node filtering strategies from aggregation-helper. */
  public enum NodeFilterType {
    NO_FILTER,
    ENTITY_TYPES
  }

  public EmbeddingSpec {
    if (embeddingLabel == null || embeddingLabel.trim().isEmpty()) {
      throw new IllegalArgumentException("embeddingLabel must not be null or empty");
    }
    if (modelName == null || modelName.trim().isEmpty()) {
      throw new IllegalArgumentException("modelName must not be null or empty");
    }
    if (modelEndpoint == null || modelEndpoint.trim().isEmpty()) {
      throw new IllegalArgumentException("modelEndpoint must not be null or empty");
    }
    if (taskType == null || taskType.trim().isEmpty()) {
      throw new IllegalArgumentException("taskType must not be null or empty");
    }
    if (nodeTypes == null || nodeTypes.isEmpty()) {
      throw new IllegalArgumentException("nodeTypes must not be null or empty");
    }
    nodeTypes.forEach(
        (key, value) -> {
          if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("nodeTypes key must not be null or empty");
          }
        });
    if (nodeFilterType == null) {
      throw new IllegalArgumentException("nodeFilterType must not be null or unknown");
    }
  }

  /** Returns the ordered list of target node types configured in this spec. */
  public List<String> getNodeTypeNames() {
    return new ArrayList<>(nodeTypes.keySet());
  }
}
