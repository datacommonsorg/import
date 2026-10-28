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
import java.util.Objects;

/**
 * Specification for a single embedding generation setup, mirroring {@code EmbeddingSpec} in
 * aggregation-helper.
 *
 * <p>Supports both the list-based {@code node_types} format (e.g., {@code ["StatisticalVariable",
 * "Topic"]}) and the predicate-mapped format (e.g., {@code {"Class": ["description"]}}).
 */
public record EmbeddingSpec(
    String embeddingLabel,
    String modelName,
    String modelEndpoint,
    String taskType,
    Map<String, List<String>> nodeTypes,
    NodeFilterType nodeFilterType)
    implements Serializable {

  public static final String DEFAULT_MODEL_NAME = "NodeEmbeddingModel";
  public static final String DEFAULT_MODEL_ENDPOINT = "text-embedding-005";
  public static final String DEFAULT_TASK_TYPE = "RETRIEVAL_QUERY";

  /** Supported node filtering strategies from aggregation-helper. */
  public enum NodeFilterType {
    NO_FILTER,
    NL_STATISTICAL_VARIABLE,
    ENTITY_TYPES
  }

  public EmbeddingSpec {
    Objects.requireNonNull(embeddingLabel, "embeddingLabel must not be null");
    if (embeddingLabel.trim().isEmpty()) {
      throw new IllegalArgumentException("embeddingLabel must not be empty");
    }
    modelName =
        (modelName == null || modelName.trim().isEmpty()) ? DEFAULT_MODEL_NAME : modelName.trim();
    modelEndpoint =
        (modelEndpoint == null || modelEndpoint.trim().isEmpty())
            ? DEFAULT_MODEL_ENDPOINT
            : modelEndpoint.trim();
    taskType =
        (taskType == null || taskType.trim().isEmpty()) ? DEFAULT_TASK_TYPE : taskType.trim();
    Objects.requireNonNull(nodeTypes, "nodeTypes must not be null");
    if (nodeTypes.isEmpty()) {
      throw new IllegalArgumentException("nodeTypes must not be empty");
    }
    nodeFilterType = nodeFilterType == null ? NodeFilterType.NO_FILTER : nodeFilterType;
  }

  /** Returns the ordered list of target node types configured in this spec. */
  public List<String> getNodeTypeNames() {
    return new ArrayList<>(nodeTypes.keySet());
  }
}
