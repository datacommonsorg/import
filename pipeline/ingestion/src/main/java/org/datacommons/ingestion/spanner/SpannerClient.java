package org.datacommons.ingestion.spanner;

import static org.datacommons.ingestion.data.ProtoUtil.compressProto;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Mutation.WriteBuilder;
import com.google.cloud.spanner.Value;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.KV;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;

import org.joda.time.Duration;

public class SpannerClient implements Serializable {
  // Decrease batch size for observations (bigger rows)
  private static final int SPANNER_BATCH_SIZE_BYTES = 500 * 1024;
  // Increase batch size for Nodes/Edges (smaller rows)
  private static final int SPANNER_MAX_NUM_ROWS = 2000;
  // Higher value ensures this limit is not encountered before MaxNumRows
  private static final int SPANNER_MAX_NUM_MUTATIONS = 10000;
  // Use more rows for sorting/batching to limit batch to fewer splits
  private static final int SPANNER_GROUPING_FACTOR = 3000;
  // Commit deadline for spanner writes. Use large value for bigger batches.
  private static final int SPANNER_COMMIT_DEADLINE_SECONDS = 120;

  // Predicates for which the object value should be stored as bytes in Spanner.
  private static final Set<String> STORE_VALUE_AS_BYTES_PREDICATES =
      ImmutableSet.of(
          "geoJsonCoordinates",
          "geoJsonCoordinatesDP1",
          "geoJsonCoordinatesDP2",
          "geoJsonCoordinatesDP3",
          "kmlCoordinates");

  private final String gcpProjectId;
  private final String spannerInstanceId;
  private final String spannerDatabaseId;
  private final String nodeTableName;
  private final String edgeTableName;
  private final String observationTableName;
  private final int numShards;

  private SpannerClient(Builder builder) {
    this.gcpProjectId = builder.gcpProjectId;
    this.spannerInstanceId = builder.spannerInstanceId;
    this.spannerDatabaseId = builder.spannerDatabaseId;
    this.nodeTableName = builder.nodeTableName;
    this.edgeTableName = builder.edgeTableName;
    this.observationTableName = builder.observationTableName;
    this.numShards = builder.numShards;
  }

  public Write getWriteTransform() {
    return SpannerIO.write()
        .withProjectId(gcpProjectId)
        .withInstanceId(spannerInstanceId)
        .withDatabaseId(ValueProvider.StaticValueProvider.of(spannerDatabaseId))
        .withBatchSizeBytes(SPANNER_BATCH_SIZE_BYTES)
        .withMaxNumRows(SPANNER_MAX_NUM_ROWS)
        .withGroupingFactor(SPANNER_GROUPING_FACTOR)
        .withMaxNumMutations(SPANNER_MAX_NUM_MUTATIONS)
        .withCommitDeadline(Duration.standardSeconds(SPANNER_COMMIT_DEADLINE_SECONDS));
  }

  public WriteGrouped getWriteGroupedTransform() {
    return new WriteGrouped(getWriteTransform());
  }

  public Mutation toNodeMutation(Node node) {
    return Mutation.newInsertOrUpdateBuilder(nodeTableName)
        .set("subject_id")
        .to(node.getSubjectId())
        .set("name")
        .to(node.getName())
        .set("types")
        .toStringArray(node.getTypes())
        .build();
  }

  public Mutation toEdgeMutation(Edge edge) {
    WriteBuilder builder =
        Mutation.newInsertOrUpdateBuilder(edgeTableName)
            .set("subject_id")
            .to(edge.getSubjectId())
            .set("predicate")
            .to(edge.getPredicate())
            .set("object_id")
            .to(edge.getObjectId())
            .set("provenance")
            .to(edge.getProvenance())
            .set("object_hash")
            .to(edge.getObjectHash());
    if (storeValueAsBytes(edge.getPredicate())) {
      builder.set("object_bytes").to(ByteArray.copyFrom(compressString(edge.getObjectValue())));
    } else {
      builder.set("object_value").to(edge.getObjectValue());
    }
    return builder.build();
  }

  /**
   * Returns true if the object value for the given predicate should be stored as bytes in Spanner,
   * false otherwise.
   */
  private boolean storeValueAsBytes(String predicate) {
    return STORE_VALUE_AS_BYTES_PREDICATES.contains(predicate);
  }

  public static byte[] compressString(String data) {
    try {
      var out = new ByteArrayOutputStream();
      try (GZIPOutputStream gout = new GZIPOutputStream(out)) {
        // Default charset can differ across platforms. Using UTF-8 here.
        gout.write(data.getBytes(StandardCharsets.UTF_8));
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error serializing string: " + data, e);
    }
  }

  public Mutation toObservationMutation(Observation observation) {
    return Mutation.newInsertOrUpdateBuilder(observationTableName)
        .set("variable_measured")
        .to(observation.getVariableMeasured())
        .set("observation_about")
        .to(observation.getObservationAbout())
        .set("observation_period")
        .to(observation.getObservationPeriod())
        .set("measurement_method")
        .to(observation.getMeasurementMethod())
        .set("unit")
        .to(observation.getUnit())
        .set("scaling_factor")
        .to(observation.getScalingFactor())
        .set("observations")
        .to(ByteArray.copyFrom(compressProto(observation.getObservations())))
        .set("import_name")
        .to(observation.getImportName())
        .set("provenance_url")
        .to(observation.getProvenanceUrl())
        .build();
  }

  public List<KV<String, Mutation>> toGraphKVMutations(List<Node> nodes, List<Edge> edges) {
    return Stream.concat(
            nodes.stream().map(this::toNodeMutation), edges.stream().map(this::toEdgeMutation))
        .map(mutation -> KV.of(getGraphKVKey(mutation), mutation))
        .toList();
  }

  public List<KV<String, Mutation>> toObservationKVMutations(List<Observation> observations) {
    return observations.stream()
        .map(this::toObservationMutation)
        .map(mutation -> KV.of(getObservationKVKey(mutation), mutation))
        .toList();
  }

  public List<KV<String, Mutation>> filterObservationKVMutations(
      List<KV<String, Mutation>> kvs, Set<String> seenObs) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var key = getFullObservationKey(kv.getValue());
      if (seenObs.contains(key)) {
        continue;
      }
      seenObs.add(key);

      filtered.add(kv);
    }
    return filtered;
  }

  public List<KV<String, Mutation>> filterGraphKVMutations(
      List<KV<String, Mutation>> kvs,
      Set<String> seenNodes,
      Set<String> seenEdges,
      Counter duplicateNodesCounter,
      Counter duplicateEdgesCounter) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var mutation = kv.getValue();
      // Skip duplicate node mutations for the same subject_id
      if (mutation.getTable().equals(nodeTableName)) {
        String subjectId = getSubjectId(mutation);
        if (seenNodes.contains(subjectId)) {
          duplicateNodesCounter.inc();
          continue;
        }
        seenNodes.add(subjectId);
      } else if (mutation.getTable().equals(edgeTableName)) {
        // Skip duplicate edge mutations for the same edge key
        String edgeKey = getEdgeKey(mutation);
        if (seenEdges.contains(edgeKey)) {
          duplicateEdgesCounter.inc();
          continue;
        }
        seenEdges.add(edgeKey);
      }

      filtered.add(kv);
    }
    return filtered;
  }

  public static String getSubjectId(Mutation mutation) {
    return getMutationValue(mutation, "subject_id");
  }

  private static String getMutationValue(Mutation mutation, String columnName) {
    return getMutationValue(mutation.asMap(), columnName);
  }

  /**
   * Returns a string mutation value from a mutation map.
   *
   * <p>Prefer using this method when multiple mutation values are to be fetched from a given
   * mutation. Call mutation.asMap() on the mutation and then call this method by passing the map.
   * This is more efficient since asMap() iterates over the columns and creates a new map each time.
   *
   * <p>Example usage: <code>
   *     Mutation mutation = ...;
   *     var mutationMap = mutation.asMap();
   *     var value1 = getMutationValue(mutationMap, "column1");
   *     var value2 = getMutationValue(mutationMap, "column2");
   *     ...
   *     var valueN = getMutationValue(mutationMap, "columnN");
   * </code>
   */
  private static String getMutationValue(Map<String, Value> mutationMap, String columnName) {
    return mutationMap.getOrDefault(columnName, Value.string("")).getString();
  }

  public static String getEdgeKey(Mutation mutation) {
    var mutationMap = mutation.asMap();
    return Joiner.on("::")
        .join(
            getMutationValue(mutationMap, "subject_id"),
            getMutationValue(mutationMap, "predicate"),
            getMutationValue(mutationMap, "object_id"),
            getMutationValue(mutationMap, "object_hash"),
            getMutationValue(mutationMap, "provenance"));
  }

  /**
   * Returns the key for grouping graph mutations (Nodes and Edges) in a KV.
   *
   * <p>Note: For effective de-duplication, the grouping key should be a subset of the primary keys
   * from the relevant tables (e.g. edges, nodes, observations).
   */
  public String getGraphKVKey(Mutation mutation) {
    var mutationMap = mutation.asMap();
    String subjectId = getMutationValue(mutationMap, "subject_id");
    if (numShards <= 1 || !mutation.getTable().equals(edgeTableName)) {
      return subjectId;
    }

    String objectId = getMutationValue(mutationMap, "object_id");
    String objectHash = getMutationValue(mutationMap, "object_hash");
    int shard = Math.abs(Objects.hash(objectId, objectHash)) % numShards;

    return Joiner.on("::").join(subjectId, shard);
  }

  public String getObservationKVKey(Mutation mutation) {
    var mutationMap = mutation.asMap();
    var parts =
        new Object[] {
          getMutationValue(mutationMap, "variable_measured"),
          hashShard(
              getMutationValue(mutationMap, "observation_about"),
              getMutationValue(mutationMap, "import_name"))
        };

    return Joiner.on("::").join(parts);
  }

  private int hashShard(Object... values) {
    return Math.abs(Objects.hash(values)) % numShards;
  }

  public static String getFullObservationKey(Mutation mutation) {
    var mutationMap = mutation.asMap();
    var parts =
        new String[] {
          getMutationValue(mutationMap, "variable_measured"),
          getMutationValue(mutationMap, "observation_about"),
          getMutationValue(mutationMap, "import_name"),
          getMutationValue(mutationMap, "observation_period"),
          getMutationValue(mutationMap, "measurement_method"),
          getMutationValue(mutationMap, "unit"),
          getMutationValue(mutationMap, "scaling_factor")
        };

    return Joiner.on("::").join(parts);
  }

  public String getGcpProjectId() {
    return gcpProjectId;
  }

  public String getSpannerInstanceId() {
    return spannerInstanceId;
  }

  public String getSpannerDatabaseId() {
    return spannerDatabaseId;
  }

  public String getNodeTableName() {
    return nodeTableName;
  }

  public String getEdgeTableName() {
    return edgeTableName;
  }

  public String getObservationTableName() {
    return observationTableName;
  }

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return String.format(
        "SpannerClient{"
            + "gcpProjectId='%s', "
            + "spannerInstanceId='%s', "
            + "spannerDatabaseId='%s', "
            + "nodeTableName='%s', "
            + "edgeTableName='%s', "
            + "observationTableName='%s'"
            + "}",
        gcpProjectId,
        spannerInstanceId,
        spannerDatabaseId,
        nodeTableName,
        edgeTableName,
        observationTableName);
  }

  public static class Builder {
    private String gcpProjectId;
    private String spannerInstanceId;
    private String spannerDatabaseId;
    private String nodeTableName = "Node";
    private String edgeTableName = "Edge";
    private String observationTableName = "Observation";
    private int numShards = 0;

    private Builder() {}

    public Builder gcpProjectId(String gcpProjectId) {
      this.gcpProjectId = gcpProjectId;
      return this;
    }

    public Builder spannerInstanceId(String spannerInstanceId) {
      this.spannerInstanceId = spannerInstanceId;
      return this;
    }

    public Builder spannerDatabaseId(String spannerDatabaseId) {
      this.spannerDatabaseId = spannerDatabaseId;
      return this;
    }

    public Builder nodeTableName(String nodeTableName) {
      this.nodeTableName = nodeTableName;
      return this;
    }

    public Builder edgeTableName(String edgeTableName) {
      this.edgeTableName = edgeTableName;
      return this;
    }

    public Builder observationTableName(String observationTableName) {
      this.observationTableName = observationTableName;
      return this;
    }

    public Builder numShards(int numShards) {
      this.numShards = numShards;
      return this;
    }

    public SpannerClient build() {
      return new SpannerClient(this);
    }
  }
}
