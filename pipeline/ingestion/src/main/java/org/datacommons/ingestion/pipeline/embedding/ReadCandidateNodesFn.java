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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.util.GraphUtils;
import org.datacommons.util.McfUtil;

/**
 * Extracts and filters candidate {@link McfGraph} nodes for a single {@link EmbeddingSpec} from the
 * {@link McfGraph}s aggregated across all imports.
 *
 * <p>Filters imported {@link McfGraph} nodes by non-empty {@code name} and configured {@code
 * node_types}, and combines nodes with the same {@code subject_id} via {@link
 * PipelineUtils#combineGraphNodes}.
 */
public final class ReadCandidateNodesFn {

  private ReadCandidateNodesFn() {}

  /**
   * Extracts candidate {@link McfGraph} nodes from the aggregated import {@code graphs} filtered by
   * non-empty {@code name} and {@code spec.getNodeTypeNames()}, combined by {@code subject_id}.
   */
  public static PCollection<McfGraph> readCandidateNodes(
      PCollection<McfGraph> graphs, EmbeddingSpec spec) {
    String specId = spec.embeddingLabel();
    Set<String> targetNodeTypes = Set.copyOf(spec.getNodeTypeNames());

    PCollection<McfGraph> filteredGraphs =
        graphs.apply(
            "FilterCandidateGraphs_" + specId,
            ParDo.of(
                new DoFn<McfGraph, McfGraph>() {
                  @ProcessElement
                  public void processElement(
                      @Element McfGraph graph, OutputReceiver<McfGraph> receiver) {
                    extractCandidateGraphs(graph, targetNodeTypes).stream()
                        .forEach(c -> receiver.output(c));
                  }
                }));

    return PipelineUtils.combineGraphNodes("CandidateNodes_" + specId, filteredGraphs);
  }

  /**
   * Extracts single-node {@link McfGraph}s from {@code graph} for non-observation nodes that have a
   * non-empty {@code subject_id}, non-empty {@code name}, and at least one {@code typeOf} in {@code
   * targetNodeTypes}.
   */
  static List<McfGraph> extractCandidateGraphs(McfGraph graph, Set<String> targetNodeTypes) {
    if (graph == null || graph.getNodesMap().isEmpty()) {
      return List.of();
    }
    List<McfGraph> results = new ArrayList<>();
    graph.getNodesMap().entrySet().stream()
        .filter(e -> !GraphUtils.isObservation(e.getValue()))
        .forEach(
            entry -> {
              PropertyValues pvs = entry.getValue();
              String dcid =
                  McfUtil.stripNamespace(
                      GraphUtils.getPropVal(pvs, GraphUtils.Property.dcid.name()));
              String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(entry.getKey());
              String name = GraphUtils.getPropVal(pvs, GraphUtils.Property.name.name()).trim();
              List<String> types =
                  GraphUtils.getPropertyValues(pvs.getPvsMap(), GraphUtils.Property.typeOf.name());
              if (!subjectId.isEmpty()
                  && !name.isEmpty()
                  && !types.isEmpty()
                  && !Collections.disjoint(types, targetNodeTypes)) {
                results.add(McfGraph.newBuilder().putNodes(subjectId, pvs).build());
              }
            });
    return results;
  }

  /** Returns the normalized {@code subject_id} (DCID) of a single-node {@link McfGraph}. */
  public static String getSubjectId(McfGraph graph) {
    if (graph.getNodesMap().isEmpty()) {
      return "";
    }
    Map.Entry<String, PropertyValues> entry = graph.getNodesMap().entrySet().iterator().next();
    String dcid =
        McfUtil.stripNamespace(
            GraphUtils.getPropVal(entry.getValue(), GraphUtils.Property.dcid.name()));
    return !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(entry.getKey());
  }
}
