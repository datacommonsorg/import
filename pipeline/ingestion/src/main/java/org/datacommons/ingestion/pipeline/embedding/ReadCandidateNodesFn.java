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

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.NodeSubGraph;

/**
 * Filters candidate {@link NodeSubGraph} records for a single {@link EmbeddingSpec} from an
 * import's already-combined and transformed {@link NodeSubGraph}s.
 *
 * <p>Retains subject nodes that have a non-empty {@code subject_id}, non-empty {@code name}, and at
 * least one {@code type} in {@link EmbeddingSpec#getNodeTypeNames()} (skipping leaf value nodes and
 * non-matching types).
 */
public final class ReadCandidateNodesFn {

  private ReadCandidateNodesFn() {}

  /**
   * Filters candidate {@link NodeSubGraph} records matching {@code spec.getNodeTypeNames()} with
   * non-empty {@code subject_id} and {@code name}.
   */
  public static PCollection<NodeSubGraph> readCandidateNodes(
      String importName, PCollection<NodeSubGraph> nodeSubGraphs, EmbeddingSpec spec) {
    String stageSuffix = spec.embeddingLabel() + "-" + importName;
    ImmutableSet<String> targetNodeTypes = ImmutableSet.copyOf(spec.getNodeTypeNames());

    return nodeSubGraphs.apply(
        "FilterCandidateNodeSubGraphs_" + stageSuffix,
        ParDo.of(
            new DoFn<NodeSubGraph, NodeSubGraph>() {
              @ProcessElement
              public void processElement(
                  @Element NodeSubGraph nodeSubGraph, OutputReceiver<NodeSubGraph> receiver) {
                if (isCandidate(nodeSubGraph, targetNodeTypes)) {
                  receiver.output(nodeSubGraph);
                }
              }
            }));
  }

  /**
   * Returns {@code true} if {@code nodeSubGraph} represents a valid subject node candidate matching
   * at least one type in {@code targetNodeTypes} with a non-empty {@code subject_id} and {@code
   * name}. Leaf value nodes (empty {@code types}) return {@code false}.
   */
  static boolean isCandidate(NodeSubGraph nodeSubGraph, Set<String> targetNodeTypes) {
    if (nodeSubGraph == null || nodeSubGraph.getNode() == null) {
      return false;
    }
    Node node = nodeSubGraph.getNode();
    String subjectId = node.getSubjectId();
    String name = node.getName();
    List<String> types = node.getTypes();
    return subjectId != null
        && !subjectId.trim().isEmpty()
        && name != null
        && !name.trim().isEmpty()
        && !types.isEmpty()
        && !Collections.disjoint(types, targetNodeTypes);
  }

  /** Returns the normalized {@code subject_id} of a {@link NodeSubGraph}. */
  public static String getSubjectId(NodeSubGraph nodeSubGraph) {
    if (nodeSubGraph == null || nodeSubGraph.getNode() == null) {
      return "";
    }
    String subjectId = nodeSubGraph.getNode().getSubjectId();
    return subjectId == null ? "" : subjectId.trim();
  }
}
