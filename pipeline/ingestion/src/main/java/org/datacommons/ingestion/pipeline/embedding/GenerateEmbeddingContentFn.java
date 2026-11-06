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

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.common.hash.Hashing;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * Generates {@link EmbeddingContent} payloads and FarmHash64 content keys from batched candidate
 * {@link NodeRecord} rows.
 *
 * <p>When predicates are configured in {@link EmbeddingSpec#nodeTypes()} (e.g., {@code
 * "description"} for {@code "Class"}), queries Spanner per batch for {@code Edge} and object {@code
 * Node.value} rows for the candidate {@code subject_id}s and aggregates them into the JSON payload.
 */
public class GenerateEmbeddingContentFn extends DoFn<Iterable<NodeRecord>, EmbeddingContent> {
  private static final Gson GSON = new Gson();
  private static final Set<String> NODE_TABLE_FIELDS =
      Set.of(NodeRecord.COL_SUBJECT_ID, NodeRecord.COL_NAME);
  public static final int PREDICATE_QUERY_BATCH_SIZE = 1000;
  private static final int BATCH_SHARDS = 32;

  private final SpannerClient spannerClient;
  private final EmbeddingSpec spec;
  private final Counter contentCounter;

  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public GenerateEmbeddingContentFn(SpannerClient spannerClient, EmbeddingSpec spec) {
    this.spannerClient = spannerClient;
    this.spec = spec;
    this.contentCounter =
        Metrics.counter(
            GenerateEmbeddingContentFn.class,
            "embedding_content_generated:" + spec.embeddingLabel());
  }

  public GenerateEmbeddingContentFn(EmbeddingSpec spec) {
    this(null, spec);
  }

  /**
   * Resolved predicate-object value for a subject node.
   *
   * @param subjectId the subject node's DCID
   * @param predicate the edge predicate (e.g., {@code "description"})
   * @param value the object node's string {@code value}
   */
  public record PredicateObjectValue(String subjectId, String predicate, String value)
      implements Serializable {}

  /**
   * Step 3 of {@link EmbeddingPipeline}: batches {@code candidateNodes} via {@link
   * GroupIntoBatches}, resolves any configured predicate properties from Spanner, and transforms
   * candidates into {@link EmbeddingContent} records for {@code spec}.
   */
  public static PCollection<EmbeddingContent> generateEmbeddingContent(
      PCollection<NodeRecord> candidateNodes, SpannerClient spannerClient, EmbeddingSpec spec) {
    String label = spec.embeddingLabel();
    return candidateNodes
        .apply(
            "KeyCandidatesForBatching_" + label,
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(), TypeDescriptor.of(NodeRecord.class)))
                .via(
                    n ->
                        KV.of(
                            Math.floorMod(
                                (n.subjectId() == null ? "" : n.subjectId()).hashCode(),
                                BATCH_SHARDS),
                            n)))
        .apply(
            "BatchCandidatesForContent_" + label,
            GroupIntoBatches.ofSize(PREDICATE_QUERY_BATCH_SIZE))
        .apply("ExtractCandidateBatches_" + label, Values.create())
        .apply(
            "GenerateEmbeddingContent_" + label,
            ParDo.of(new GenerateEmbeddingContentFn(spannerClient, spec)));
  }

  @Setup
  public void setup() {
    if (spannerClient != null) {
      this.spanner = spannerClient.createSpanner();
      this.dbClient = spannerClient.getDatabaseClient(spanner);
    }
  }

  @Teardown
  public void teardown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @ProcessElement
  public void processElement(
      @Element Iterable<NodeRecord> candidates, OutputReceiver<EmbeddingContent> receiver) {
    if (candidates == null) {
      return;
    }

    List<NodeRecord> nodesWithPredicates = new ArrayList<>();
    for (NodeRecord candidate : candidates) {
      if (candidate == null
          || candidate.subjectId() == null
          || candidate.subjectId().trim().isEmpty()
          || candidate.name() == null
          || candidate.name().trim().isEmpty()) {
        continue;
      }

      if (getAllowedPredicatesForNode(candidate.types(), spec).isEmpty()) {
        EmbeddingContent content = buildEmbeddingContent(candidate, Map.of(), spec);
        if (content != null) {
          contentCounter.inc();
          receiver.output(content);
        }
      } else {
        nodesWithPredicates.add(candidate);
      }
    }

    if (!nodesWithPredicates.isEmpty()) {
      for (EmbeddingContent content : buildBatchContents(nodesWithPredicates)) {
        contentCounter.inc();
        receiver.output(content);
      }
    }
  }

  private List<EmbeddingContent> buildBatchContents(List<NodeRecord> batchNodes) {
    List<PredicateObjectValue> resolvedValues =
        dbClient != null ? fetchPredicateValuesFromSpanner(batchNodes) : List.of();
    Map<String, Map<String, String>> propsBySubjectId =
        aggregatePropertiesBySubject(batchNodes, resolvedValues, spec);

    List<EmbeddingContent> results = new ArrayList<>(batchNodes.size());
    for (NodeRecord node : batchNodes) {
      Map<String, String> props = propsBySubjectId.getOrDefault(node.subjectId(), Map.of());
      EmbeddingContent content = buildEmbeddingContent(node, props, spec);
      if (content != null) {
        results.add(content);
      }
    }
    return results;
  }

  private List<PredicateObjectValue> fetchPredicateValuesFromSpanner(List<NodeRecord> batchNodes) {
    Set<String> subjectIds = new LinkedHashSet<>();
    Set<String> predicates = new LinkedHashSet<>();
    for (NodeRecord node : batchNodes) {
      subjectIds.add(node.subjectId());
      predicates.addAll(getAllowedPredicatesForNode(node.types(), spec));
    }
    Statement stmt =
        buildPredicateQueryStatement(
            spannerClient.getEdgeTableName(),
            spannerClient.getNodeTableName(),
            subjectIds,
            predicates);
    List<PredicateObjectValue> values = new ArrayList<>();
    try (ResultSet rs = dbClient.singleUse().executeQuery(stmt)) {
      while (rs.next()) {
        values.add(
            new PredicateObjectValue(
                rs.getString(EdgeRecord.COL_SUBJECT_ID),
                rs.getString(EdgeRecord.COL_PREDICATE),
                rs.getString(NodeRecord.COL_VALUE)));
      }
    }
    return values;
  }

  /**
   * Builds the parameterized Spanner SQL statement to fetch predicate edges and object {@code
   * Node.value}s for a batch of candidate {@code subject_id}s.
   */
  public static Statement buildPredicateQueryStatement(
      String edgeTable, String nodeTable, Set<String> subjectIds, Set<String> predicates) {
    String sql =
        String.format(
            "SELECT e.%s, e.%s, o.%s "
                + "FROM %s e JOIN %s o ON e.%s = o.%s "
                + "WHERE e.%s IN UNNEST(@subject_ids) "
                + "AND e.%s IN UNNEST(@predicates) "
                + "AND o.%s IS NOT NULL AND o.%s != '' "
                + "ORDER BY e.%s, e.%s, o.%s",
            EdgeRecord.COL_SUBJECT_ID,
            EdgeRecord.COL_PREDICATE,
            NodeRecord.COL_VALUE,
            edgeTable,
            nodeTable,
            EdgeRecord.COL_OBJECT_ID,
            NodeRecord.COL_SUBJECT_ID,
            EdgeRecord.COL_SUBJECT_ID,
            EdgeRecord.COL_PREDICATE,
            NodeRecord.COL_VALUE,
            NodeRecord.COL_VALUE,
            EdgeRecord.COL_SUBJECT_ID,
            EdgeRecord.COL_PREDICATE,
            NodeRecord.COL_VALUE);
    return Statement.newBuilder(sql)
        .bind("subject_ids")
        .toStringArray(subjectIds)
        .bind("predicates")
        .toStringArray(predicates)
        .build();
  }

  /**
   * Aggregates resolved {@link PredicateObjectValue}s per candidate node, joining multiple values
   * for the same predicate with {@code ". "}.
   */
  public static Map<String, Map<String, String>> aggregatePropertiesBySubject(
      List<NodeRecord> nodes, Iterable<PredicateObjectValue> predicateValues, EmbeddingSpec spec) {
    Map<String, Set<String>> allowedPredsBySubject = new LinkedHashMap<>();
    for (NodeRecord node : nodes) {
      allowedPredsBySubject.put(node.subjectId(), getAllowedPredicatesForNode(node.types(), spec));
    }

    Map<String, Map<String, String>> result = new LinkedHashMap<>();
    for (PredicateObjectValue pv : predicateValues) {
      Set<String> allowed = allowedPredsBySubject.get(pv.subjectId());
      if (allowed != null && allowed.contains(pv.predicate())) {
        result
            .computeIfAbsent(pv.subjectId(), k -> new LinkedHashMap<>())
            .merge(pv.predicate(), pv.value(), (existing, next) -> existing + ". " + next);
      }
    }
    return result;
  }

  /**
   * Builds the {@link EmbeddingContent} for a single {@link NodeRecord} and its resolved {@code
   * properties}.
   *
   * <p>Fields {@code "subject_id"} and {@code "name"} configured in {@link
   * EmbeddingSpec#nodeTypes()} are populated directly from the {@link NodeRecord}, while any other
   * configured fields are populated from the resolved {@code properties} map.
   */
  public static EmbeddingContent buildEmbeddingContent(
      NodeRecord candidate, Map<String, String> properties, EmbeddingSpec spec) {
    if (candidate == null
        || candidate.subjectId() == null
        || candidate.subjectId().trim().isEmpty()
        || candidate.name() == null
        || candidate.name().trim().isEmpty()) {
      return null;
    }

    JsonObject jsonObj = buildBaseJsonObject(candidate, spec);
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, String> entry : properties.entrySet()) {
        jsonObj.addProperty(entry.getKey(), entry.getValue());
      }
    }

    if (jsonObj.size() == 0) {
      return null;
    }

    String jsonStr = GSON.toJson(jsonObj);
    String contentKey = computeFarmFingerprintKey(jsonStr);
    return new EmbeddingContent(
        candidate.subjectId(),
        spec.embeddingLabel(),
        contentKey,
        jsonStr,
        jsonStr,
        candidate.types(),
        spec.modelEndpoint(),
        spec.taskType());
  }

  static JsonObject buildBaseJsonObject(NodeRecord candidate, EmbeddingSpec spec) {
    JsonObject jsonObj = new JsonObject();
    Set<String> allowedFields = getAllowedFieldsForNode(candidate.types(), spec);
    if (allowedFields.contains(NodeRecord.COL_SUBJECT_ID)) {
      jsonObj.addProperty(NodeRecord.COL_SUBJECT_ID, candidate.subjectId());
    }
    if (allowedFields.contains(NodeRecord.COL_NAME)) {
      jsonObj.addProperty(NodeRecord.COL_NAME, candidate.name());
    }
    return jsonObj;
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

  static Set<String> getAllowedFieldsForNode(List<String> nodeTypes, EmbeddingSpec spec) {
    if (nodeTypes == null || nodeTypes.isEmpty()) {
      return Set.of();
    }
    Set<String> allowed = new LinkedHashSet<>();
    for (String type : nodeTypes) {
      List<String> fields = spec.nodeTypes().get(type);
      if (fields != null) {
        allowed.addAll(fields);
      }
    }
    return allowed;
  }

  static Set<String> getAllowedPredicatesForNode(List<String> nodeTypes, EmbeddingSpec spec) {
    Set<String> allowed = new LinkedHashSet<>();
    for (String field : getAllowedFieldsForNode(nodeTypes, spec)) {
      if (!NODE_TABLE_FIELDS.contains(field)) {
        allowed.add(field);
      }
    }
    return allowed;
  }
}
