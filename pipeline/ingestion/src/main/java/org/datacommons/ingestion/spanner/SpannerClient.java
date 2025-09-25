package org.datacommons.ingestion.spanner;

import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.common.base.Joiner;
import com.google.protobuf.ByteString;
import com.google.spanner.admin.database.v1.CreateDatabaseRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerClient implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerClient.class);

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

  /**
   * Parses DDL statements from a BufferedReader. DDL statements can span multiple lines and are are
   * delimited with a newline.
   */
  private List<String> parseDdlStatements(BufferedReader reader) throws IOException {
    String fullText = reader.lines().collect(Collectors.joining("\n"));
    String[] blocksArray = fullText.split("\\n\\s*\\n+", 0);
    return Arrays.asList(blocksArray);
  }

  public void createDatabase() {
    try {
      DatabaseAdminClient dbAdminClient = DatabaseAdminClient.create();

      try {
        dbAdminClient.getDatabase(
            String.format(
                "projects/%s/instances/%s/databases/%s",
                gcpProjectId, spannerInstanceId, spannerDatabaseId));
        LOGGER.info("Spanner database {} already exists.", spannerDatabaseId);
      } catch (com.google.api.gax.rpc.NotFoundException notFoundE) {
        LOGGER.info("Spanner database {} not found. Creating it now.", spannerDatabaseId);

        List<String> ddlStatements;
        try (InputStream inputStream =
            getClass().getClassLoader().getResourceAsStream("spanner_schema.sql")) {
          if (inputStream == null) {
            throw new java.io.FileNotFoundException(
                "Could not find spanner_schema.sql in resources.");
          }
          try (BufferedReader reader =
              new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            ddlStatements = parseDdlStatements(reader);
          }
        } catch (IOException ioE) {
          throw new IOException("Failed to read DDL file", ioE);
        }

        ByteString protoDescriptors;
        try (InputStream inputStream =
            getClass().getClassLoader().getResourceAsStream("descriptor.proto.bin")) {
          if (inputStream == null) {
            throw new java.io.FileNotFoundException(
                "Could not find proto descriptor file (descriptor.proto.bin) in resources.");
          }
          protoDescriptors = ByteString.copyFrom(inputStream.readAllBytes());
        } catch (IOException ioE) {
          throw new IOException("Failed to read proto descriptor file", ioE);
        }

        CreateDatabaseRequest request =
            CreateDatabaseRequest.newBuilder()
                .setParent(
                    String.format("projects/%s/instances/%s", gcpProjectId, spannerInstanceId))
                .setCreateStatement(String.format("CREATE DATABASE `%s`", spannerDatabaseId))
                .addAllExtraStatements(ddlStatements)
                .setProtoDescriptors(protoDescriptors)
                .build();

        try {
          dbAdminClient.createDatabaseAsync(request).get();
        } catch (java.util.concurrent.ExecutionException executionE) {
          throw SpannerExceptionFactory.newSpannerException(
              ErrorCode.UNKNOWN, "An error occurred during database creation.", executionE);
        } catch (InterruptedException interruptedE) {
          LOGGER.error(
              "Operation was interrupted while waiting for database creation.", interruptedE);
          throw SpannerExceptionFactory.propagateInterrupt(interruptedE);
        }
        LOGGER.info("Successfully created Spanner database {}.", spannerDatabaseId);
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create DatabaseAdminClient.", e);
    }
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
        .set("value")
        .to(node.getValue())
        .set("bytes")
        .to(node.getBytes())
        .set("name")
        .to(node.getName())
        .set("types")
        .toStringArray(node.getTypes())
        .build();
  }

  public Mutation toEdgeMutation(Edge edge) {
    return Mutation.newInsertOrUpdateBuilder(edgeTableName)
        .set("subject_id")
        .to(edge.getSubjectId())
        .set("predicate")
        .to(edge.getPredicate())
        .set("object_id")
        .to(edge.getObjectId())
        .set("provenance")
        .to(edge.getProvenance())
        .build();
  }

  public Mutation toObservationMutation(Observation observation) {
    return Mutation.newInsertOrUpdateBuilder(observationTableName)
        .set("variable_measured")
        .to(observation.getVariableMeasured())
        .set("observation_about")
        .to(observation.getObservationAbout())
        .set("facet_id")
        .to(observation.getFacetId())
        .set("observation_period")
        .to(observation.getObservationPeriod())
        .set("measurement_method")
        .to(observation.getMeasurementMethod())
        .set("unit")
        .to(observation.getUnit())
        .set("scaling_factor")
        .to(observation.getScalingFactor())
        .set("observations")
        .to(Value.protoMessage(observation.getObservations()))
        .set("import_name")
        .to(observation.getImportName())
        .set("provenance_url")
        .to(observation.getProvenanceUrl())
        .set("is_dc_aggregate")
        .to(observation.getIsDcAggregate())
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
      List<KV<String, Mutation>> kvs, Map<String, Boolean> seenObs) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var key = getFullObservationKey(kv.getValue());
      if (seenObs.get(key) != null) {
        continue;
      }
      seenObs.putIfAbsent(key, true);

      filtered.add(kv);
    }
    return filtered;
  }

  public List<KV<String, Mutation>> filterGraphKVMutations(
      List<KV<String, Mutation>> kvs,
      Map<String, Boolean> seenNodes,
      Map<String, Boolean> seenEdges,
      Counter duplicateNodesCounter,
      Counter duplicateEdgesCounter) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var mutation = kv.getValue();
      // Skip duplicate node mutations for the same subject_id
      if (mutation.getTable().equals(nodeTableName)) {
        String subjectId = getSubjectId(mutation);
        if (seenNodes.get(subjectId) != null) {
          duplicateNodesCounter.inc();
          continue;
        }
        seenNodes.putIfAbsent(subjectId, true);
      } else if (mutation.getTable().equals(edgeTableName)) {
        // Skip duplicate edge mutations for the same edge key
        String edgeKey = getEdgeKey(mutation);
        if (seenEdges.get(edgeKey) != null) {
          duplicateEdgesCounter.inc();
          continue;
        }
        seenEdges.putIfAbsent(edgeKey, true);
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
    int shard = Math.abs(Objects.hash(objectId)) % numShards;

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
          getMutationValue(mutationMap, "facet_id")
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
