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

import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
  private static final Set<String> NODE_TABLE_FIELDS =
      Set.of(NodeRecord.COL_SUBJECT_ID, NodeRecord.COL_NAME);
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

    Map<String, String> contentMap = buildBaseContentMap(subjectId, name, types, spec);
    contentMap.putAll(extractPredicateProperties(pvs, types, spec));

    if (contentMap.isEmpty()) {
      return null;
    }

    String jsonStr = GSON.toJson(contentMap);
    String contentKey = computeFarmFingerprintKey(jsonStr);
    String textToEmbed =
        jsonStr.length() > MAX_CHAR_LENGTH ? jsonStr.substring(0, MAX_CHAR_LENGTH) : jsonStr;
    return new EmbeddingContent(
        subjectId,
        spec.embeddingLabel(),
        contentKey,
        jsonStr,
        textToEmbed,
        types,
        spec.modelEndpoint(),
        spec.taskType());
  }

  static Map<String, String> buildBaseContentMap(
      String subjectId, String name, List<String> types, EmbeddingSpec spec) {
    Map<String, String> contentMap = new LinkedHashMap<>();
    Set<String> embeddingFields = getEmbeddingNodeFields(types, spec);
    if (embeddingFields.contains(NodeRecord.COL_SUBJECT_ID)) {
      contentMap.put(NodeRecord.COL_SUBJECT_ID, subjectId);
    }
    if (embeddingFields.contains(NodeRecord.COL_NAME)) {
      contentMap.put(NodeRecord.COL_NAME, name);
    }
    return contentMap;
  }

  /**
   * Extracts configured predicate properties for a node from its {@link PropertyValues}, joining
   * multiple values for the same predicate with {@code ". "}.
   */
  public static Map<String, String> extractPredicateProperties(
      PropertyValues pvs, List<String> types, EmbeddingSpec spec) {
    Set<String> embeddingPredicates = getEmbeddingPredicates(types, spec);
    if (embeddingPredicates.isEmpty() || pvs == null) {
      return Map.of();
    }

    Map<String, String> result = new LinkedHashMap<>();
    for (String predicate : embeddingPredicates) {
      List<String> values =
          GraphUtils.getPropertyValues(pvs.getPvsMap(), predicate).stream()
              .map(String::trim)
              .filter(v -> !v.isEmpty())
              .sorted()
              .toList();
      if (!values.isEmpty()) {
        result.put(predicate, String.join(". ", values));
      }
    }
    return result;
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

  static Set<String> getEmbeddingNodeFields(List<String> nodeTypes, EmbeddingSpec spec) {
    if (nodeTypes == null || nodeTypes.isEmpty()) {
      return Set.of();
    }
    Set<String> embeddingFields = new LinkedHashSet<>();
    for (String type : nodeTypes) {
      List<String> fields = spec.nodeTypes().get(type);
      if (fields != null) {
        embeddingFields.addAll(fields);
      }
    }
    return embeddingFields;
  }

  static Set<String> getEmbeddingPredicates(List<String> nodeTypes, EmbeddingSpec spec) {
    Set<String> embeddingPredicates = new LinkedHashSet<>();
    for (String field : getEmbeddingNodeFields(nodeTypes, spec)) {
      if (!NODE_TABLE_FIELDS.contains(field)) {
        embeddingPredicates.add(field);
      }
    }
    return embeddingPredicates;
  }
}
