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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.util.GraphUtils;

/**
 * Generates {@link EmbeddingContent} payloads and FarmHash64 content keys from candidate {@link
 * McfGraph} nodes.
 *
 * <p>Extracts {@code subject_id}, {@code name}, {@code types}, and any configured predicate
 * properties in {@link EmbeddingSpec#nodeTypes()} (e.g., {@code "description"} for {@code "Class"})
 * directly from each {@link McfGraph} node.
 */
public class GenerateEmbeddingContentFn extends DoFn<McfGraph, EmbeddingContent> {
  private static final Gson GSON = new Gson();
  private static final ImmutableSet<String> NODE_TABLE_FIELDS =
      ImmutableSet.of(NodeRecord.COL_SUBJECT_ID, NodeRecord.COL_NAME);
  // Approximate max token cap (2048 tokens * ~4 chars/token) to be embedded.
  public static final int MAX_CHAR_LENGTH = 2048 * 4;

  private final EmbeddingSpec spec;
  private final Counter contentCounter;

  public GenerateEmbeddingContentFn(EmbeddingSpec spec) {
    this.spec = spec;
    this.contentCounter =
        Metrics.counter(
            GenerateEmbeddingContentFn.class,
            "embedding_content_generated:" + spec.embeddingLabel());
  }

  /**
   * Transforms candidate {@link McfGraph} nodes into {@link EmbeddingContent} records for {@code
   * spec}.
   */
  public static PCollection<EmbeddingContent> generateEmbeddingContent(
      PCollection<McfGraph> candidateNodes, EmbeddingSpec spec) {
    String label = spec.embeddingLabel();
    return candidateNodes.apply(
        "GenerateEmbeddingContent_" + label, ParDo.of(new GenerateEmbeddingContentFn(spec)));
  }

  @ProcessElement
  public void processElement(
      @Element McfGraph candidate, OutputReceiver<EmbeddingContent> receiver) {
    EmbeddingContent content = buildEmbeddingContent(candidate, spec);
    if (content != null) {
      contentCounter.inc();
      receiver.output(content);
    }
  }

  /**
   * Builds the {@link EmbeddingContent} for a single-node {@link McfGraph}.
   *
   * <p>Fields {@code "subject_id"} and {@code "name"} configured in {@link
   * EmbeddingSpec#nodeTypes()} are populated from the node's DCID and {@code name}, while any other
   * configured fields are populated from the node's MCF predicate values.
   */
  public static EmbeddingContent buildEmbeddingContent(McfGraph candidate, EmbeddingSpec spec) {
    if (candidate == null || candidate.getNodesMap().isEmpty()) {
      return null;
    }

    String subjectId = ReadCandidateNodesFn.getSubjectId(candidate).trim();
    PropertyValues pvs = candidate.getNodesMap().values().iterator().next();
    String name = GraphUtils.getPropVal(pvs, GraphUtils.Property.name.name()).trim();
    List<String> types =
        GraphUtils.getPropertyValues(pvs.getPvsMap(), GraphUtils.Property.typeOf.name());
    if (subjectId.isEmpty() || name.isEmpty() || types.isEmpty()) {
      return null;
    }

    ImmutableMap<String, String> contentMap =
        ImmutableMap.<String, String>builder()
            .putAll(buildBaseContentMap(subjectId, name, types, spec))
            .putAll(extractPredicateProperties(pvs, types, spec))
            .buildKeepingLast();

    if (contentMap.isEmpty()) {
      return null;
    }

    String jsonStr = GSON.toJson(contentMap);
    String contentKey = computeFarmFingerprintKey(jsonStr);
    String textToEmbed =
        jsonStr.length() > MAX_CHAR_LENGTH ? jsonStr.substring(0, MAX_CHAR_LENGTH) : jsonStr;
    return new EmbeddingContent(
        subjectId, spec.embeddingLabel(), contentKey, jsonStr, textToEmbed, types);
  }

  static ImmutableMap<String, String> buildBaseContentMap(
      String subjectId, String name, List<String> types, EmbeddingSpec spec) {
    ImmutableSet<String> embeddingFields = getEmbeddingNodeFields(types, spec);
    ImmutableMap.Builder<String, String> contentMap = ImmutableMap.builder();
    if (embeddingFields.contains(NodeRecord.COL_SUBJECT_ID)) {
      contentMap.put(NodeRecord.COL_SUBJECT_ID, subjectId);
    }
    if (embeddingFields.contains(NodeRecord.COL_NAME)) {
      contentMap.put(NodeRecord.COL_NAME, name);
    }
    return contentMap.buildOrThrow();
  }

  /**
   * Extracts configured predicate properties for a node from its {@link PropertyValues}, joining
   * multiple values for the same predicate with {@code ". "}.
   */
  public static ImmutableMap<String, String> extractPredicateProperties(
      PropertyValues pvs, List<String> types, EmbeddingSpec spec) {
    ImmutableSet<String> embeddingPredicates = getEmbeddingPredicates(types, spec);
    if (embeddingPredicates.isEmpty() || pvs == null) {
      return ImmutableMap.of();
    }

    return embeddingPredicates.stream()
        .map(predicate -> Map.entry(predicate, extractSinglePredicateValues(pvs, predicate)))
        .filter(entry -> !entry.getValue().isEmpty())
        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  static String extractSinglePredicateValues(PropertyValues pvs, String predicate) {
    ImmutableList<String> values =
        GraphUtils.getPropertyValues(pvs.getPvsMap(), predicate).stream()
            .map(String::trim)
            .filter(v -> !v.isEmpty())
            .sorted()
            .collect(ImmutableList.toImmutableList());
    return String.join(". ", values);
  }

  /**
   * Computes GoogleSQL's {@code CAST(FARM_FINGERPRINT(input) AS STRING)} using Guava's 64-bit
   * FarmHash Fingerprint64 implementation.
   */
  public static String computeFarmFingerprintKey(String input) {
    String safeInput = input == null ? "" : input;
    long fingerprint =
        Hashing.farmHashFingerprint64().hashString(safeInput, StandardCharsets.UTF_8).asLong();
    return Long.toString(fingerprint);
  }

  static ImmutableSet<String> getEmbeddingNodeFields(List<String> nodeTypes, EmbeddingSpec spec) {
    if (nodeTypes == null || nodeTypes.isEmpty()) {
      return ImmutableSet.of();
    }
    return nodeTypes.stream()
        .map(type -> spec.nodeTypes().get(type))
        .filter(Objects::nonNull)
        .flatMap(List::stream)
        .collect(ImmutableSet.toImmutableSet());
  }

  static ImmutableSet<String> getEmbeddingPredicates(List<String> nodeTypes, EmbeddingSpec spec) {
    return getEmbeddingNodeFields(nodeTypes, spec).stream()
        .filter(field -> !NODE_TABLE_FIELDS.contains(field))
        .collect(ImmutableSet.toImmutableSet());
  }
}
