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
import com.google.cloud.spanner.Value;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * Extracts and filters candidate {@link NodeRecord}s for a single {@link EmbeddingSpec} from the
 * {@code Node} {@link Mutation}s aggregated across all imports.
 *
 * <p>Filters imported {@code Node} mutations by non-empty {@code name} and configured {@code
 * node_types}, and deduplicates candidates by {@code subject_id}. Predicate and edge properties are
 * not processed here; they are resolved downstream in {@link GenerateEmbeddingContentFn}.
 */
public final class ReadCandidateNodesFn {

  private ReadCandidateNodesFn() {}

  /**
   * Step 1 of {@link EmbeddingPipeline}: extracts candidate {@link NodeRecord}s from the aggregated
   * import {@code nodeMutations} filtered by non-empty {@code name} and {@code
   * spec.getNodeTypeNames()}, deduplicated by {@code subject_id}.
   */
  public static PCollection<NodeRecord> readCandidateNodes(
      PCollection<Mutation> nodeMutations, EmbeddingSpec spec) {
    String specId = spec.embeddingLabel();
    Set<String> targetNodeTypes = Set.copyOf(spec.getNodeTypeNames());

    return nodeMutations
        .apply(
            "FilterAndParseCandidateNodes_" + specId,
            ParDo.of(
                new DoFn<Mutation, NodeRecord>() {
                  @ProcessElement
                  public void processElement(
                      @Element Mutation mutation, OutputReceiver<NodeRecord> receiver) {
                    NodeRecord candidate = extractCandidateNode(mutation, targetNodeTypes);
                    if (candidate != null) {
                      receiver.output(candidate);
                    }
                  }
                }))
        .apply(
            "DeduplicateCandidateNodes_" + specId,
            Distinct.<NodeRecord, String>withRepresentativeValueFn(NodeRecord::subjectId)
                .withRepresentativeType(TypeDescriptors.strings()));
  }

  /**
   * Parses a {@code Node} {@link Mutation} into a {@link NodeRecord} if it has a non-empty {@code
   * name} and at least one type in {@code targetNodeTypes}; otherwise returns {@code null}.
   */
  static NodeRecord extractCandidateNode(Mutation mutation, Set<String> targetNodeTypes) {
    Map<String, Value> map = mutation.asMap();
    Value nameVal = map.get(NodeRecord.COL_NAME);
    Value typesVal = map.get(NodeRecord.COL_TYPES);
    if (nameVal == null
        || nameVal.isNull()
        || nameVal.getString().trim().isEmpty()
        || typesVal == null
        || typesVal.isNull()) {
      return null;
    }

    List<String> types = typesVal.getStringArray();
    if (Collections.disjoint(types, targetNodeTypes)) {
      return null;
    }

    Value valueVal = map.get(NodeRecord.COL_VALUE);
    Value bytesVal = map.get(NodeRecord.COL_BYTES);
    return new NodeRecord(
        map.get(NodeRecord.COL_SUBJECT_ID).getString(),
        (valueVal == null || valueVal.isNull()) ? null : valueVal.getString(),
        (bytesVal == null || bytesVal.isNull()) ? null : bytesVal.getBytes(),
        nameVal.getString(),
        types);
  }
}
