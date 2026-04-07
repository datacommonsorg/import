package org.datacommons.ingestion.spanner;

import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ErrorCode;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerExceptionFactory;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminClient;
import com.google.cloud.spanner.admin.database.v1.DatabaseAdminSettings;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
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
  // Maximum size for a single column value in Spanner (10MB)
  public static final int MAX_SPANNER_COLUMN_SIZE = 10 * 1024 * 1024;
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
  private final String emulatorHost;

  protected SpannerClient(Builder builder) {
    this.gcpProjectId = builder.gcpProjectId;
    this.spannerInstanceId = builder.spannerInstanceId;
    this.spannerDatabaseId = builder.spannerDatabaseId;
    this.nodeTableName = builder.nodeTableName;
    this.edgeTableName = builder.edgeTableName;
    this.observationTableName = builder.observationTableName;
    this.numShards = builder.numShards;
    this.emulatorHost = builder.emulatorHost;
  }

  /**
   * Helper method to write mutations to Spanner.
   *
   * @param pipeline The Beam pipeline.
   * @param name The name prefix for the transforms (e.g., "Node", "Edge").
   * @param mutations The PCollection of mutations to write.
   * @return The result of the Spanner write operation.
   */
  public SpannerWriteResult writeMutations(
      Pipeline pipeline, String name, PCollection<Mutation> mutations) {
    return mutations.apply(name, getWriteTransform());
  }

  /**
   * Parses DDL statements from a BufferedReader. DDL statements can span multiple lines and are are
   * delimited with a newline.
   */
  private List<String> parseDdlStatements(BufferedReader reader) throws IOException {
    String fullText = reader.lines().collect(Collectors.joining("\n"));
    String[] blocksArray = fullText.split("\\n\\s*\\n+", 0);
    return new ArrayList<>(Arrays.asList(blocksArray));
  }

  private ByteString loadProtoDescriptors() {
    InputStream is = getClass().getClassLoader().getResourceAsStream("descriptor.proto.bin");
    if (is == null) {
      throw new IllegalStateException(
          "Could not find proto descriptor file (descriptor.proto.bin) in resources.");
    }
    try {
      return ByteString.copyFrom(is.readAllBytes());
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read proto descriptor file.", e);
    }
  }

  /** Initializes the remote Spanner database. */
  private void initializeRemoteDatabase(DatabaseAdminClient dbAdminClient) {
    LOGGER.info("Spanner database {} not found. Creating it now.", spannerDatabaseId);

    CreateDatabaseRequest request =
        CreateDatabaseRequest.newBuilder()
            .setParent(String.format("projects/%s/instances/%s", gcpProjectId, spannerInstanceId))
            .setCreateStatement(String.format("CREATE DATABASE `%s`", spannerDatabaseId))
            .build();

    try {
      // Block until the database is created.
      dbAdminClient.createDatabaseAsync(request).get();
    } catch (java.util.concurrent.ExecutionException executionE) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.UNKNOWN, "An error occurred during database creation.", executionE);
    } catch (InterruptedException interruptedE) {
      LOGGER.error("Operation was interrupted while waiting for database creation.", interruptedE);
      throw SpannerExceptionFactory.propagateInterrupt(interruptedE);
    }
    LOGGER.info("Successfully created Spanner database {}.", spannerDatabaseId);
  }

  /**
   * Validates that the Spanner database and tables exist. Initializes the database if it doesn't.
   */
  public void validateOrInitializeDatabase() {
    DatabaseAdminClient dbAdminClient;
    try {
      if (emulatorHost != null) {
        DatabaseAdminSettings.Builder settingsBuilder = DatabaseAdminSettings.newBuilder();
        settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        settingsBuilder.setEndpoint(emulatorHost);
        settingsBuilder.setTransportChannelProvider(
            InstantiatingGrpcChannelProvider.newBuilder()
                .setEndpoint(emulatorHost)
                .setChannelConfigurator(io.grpc.ManagedChannelBuilder::usePlaintext)
                .build());
        dbAdminClient = DatabaseAdminClient.create(settingsBuilder.build());
      } else {
        dbAdminClient = DatabaseAdminClient.create();
      }
    } catch (IOException e) {
      throw new IllegalStateException("Failed to create DatabaseAdminClient.", e);
    }

    // Ensure the Database now has the proper Tables and Proto Bundle.
    String databasePath =
        String.format(
            "projects/%s/instances/%s/databases/%s",
            gcpProjectId, spannerInstanceId, spannerDatabaseId);

    try {
      dbAdminClient.getDatabase(databasePath);
      LOGGER.info("Spanner database {} already exists.", spannerDatabaseId);
    } catch (com.google.api.gax.rpc.NotFoundException notFoundE) {
      initializeRemoteDatabase(dbAdminClient);
    }

    // Check if tables exist by fetching current DDL from Spanner once.
    List<String> currentDdlStatements =
        dbAdminClient.getDatabaseDdl(databasePath).getStatementsList();

    boolean nodeExists = checkTableExists(currentDdlStatements, "Node");
    boolean edgeExists = checkTableExists(currentDdlStatements, "Edge");
    boolean observationExists = checkTableExists(currentDdlStatements, "Observation");
    boolean protoBundleExists =
        currentDdlStatements.stream()
            .anyMatch(s -> s.trim().toUpperCase().startsWith("CREATE PROTO BUNDLE"));

    if (nodeExists && edgeExists && observationExists && protoBundleExists) {
      LOGGER.info("Database is fully initialized.");
      return;
    }

    if (nodeExists || edgeExists || observationExists) {
      throw new IllegalStateException(
          String.format(
              "Database is in an inconsistent state. Node: %b, Edge: %b, Observation: %b. Please clean up the database manually.",
              nodeExists, edgeExists, observationExists));
    }

    LOGGER.info("Database is empty. Applying DDL statements to existing database.");
    List<String> statementsToApply = readDdlStatements();
    if (protoBundleExists) {
      LOGGER.info("Proto bundle already exists in database. Skipping CREATE PROTO BUNDLE.");
      statementsToApply.removeIf(s -> s.trim().toUpperCase().startsWith("CREATE PROTO BUNDLE"));
    }

    ByteString protoDescriptors = loadProtoDescriptors();
    try {
      // Update the Schema to include the new tables and proto bundle.
      dbAdminClient
          .updateDatabaseDdlAsync(
              com.google.spanner.admin.database.v1.UpdateDatabaseDdlRequest.newBuilder()
                  .setDatabase(databasePath)
                  .addAllStatements(statementsToApply)
                  .setProtoDescriptors(protoDescriptors)
                  .build())
          .get();
      LOGGER.info("Successfully applied DDL statements to existing database.");
    } catch (java.util.concurrent.ExecutionException executionE) {
      throw SpannerExceptionFactory.newSpannerException(
          ErrorCode.UNKNOWN, "An error occurred during DDL update.", executionE);
    } catch (InterruptedException interruptedE) {
      LOGGER.error("Operation was interrupted while waiting for DDL update.", interruptedE);
      throw SpannerExceptionFactory.propagateInterrupt(interruptedE);
    }
  }

  /** Reads DDL statements from the spanner_schema.sql file in the resources directory. */
  List<String> readDdlStatements() {
    InputStream inputStream = getClass().getClassLoader().getResourceAsStream("spanner_schema.sql");
    if (inputStream == null) {
      throw new IllegalStateException("Could not find spanner_schema.sql in resources.");
    }
    try (BufferedReader reader =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
      return parseDdlStatements(reader);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to read spanner_schema.sql", e);
    }
  }

  /**
   * Checks if a table exists in the list of DDL statements.
   *
   * @param statements The list of DDL statements retrieved from Spanner.
   * @param tableName The name of the table to check.
   * @return true if the table exists, false otherwise.
   */
  boolean checkTableExists(List<String> statements, String tableName) {
    String regex = "(?i)\\bCREATE\\s+TABLE\\s+`?" + tableName + "`?\\b";
    Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
    return statements.stream().anyMatch(s -> pattern.matcher(s).find());
  }

  public PCollection<Void> deleteDataForImport(
      Pipeline pipeline, String importName, String tableName, String columnName) {
    String stageName = tableName + "-" + importName.replaceFirst("^dc/base/", "");
    return pipeline
        .apply("StartDelete" + stageName, Create.of(importName))
        .apply(
            "ExecuteDelete" + stageName,
            ParDo.of(new DeleteByColumnFn(this, tableName, columnName)));
  }

  static class DeleteByColumnFn extends DoFn<String, Void> {
    private final SpannerClient spannerClient;
    private final String tableName;
    private final String columnName;

    public DeleteByColumnFn(SpannerClient spannerClient, String tableName, String columnName) {
      this.spannerClient = spannerClient;
      this.tableName = tableName;
      this.columnName = columnName;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      String value = c.element();
      SpannerOptions.Builder builder =
          SpannerOptions.newBuilder().setProjectId(spannerClient.gcpProjectId);
      if (spannerClient.emulatorHost != null) {
        builder.setEmulatorHost(spannerClient.emulatorHost);
        builder.setCredentials(NoCredentials.getInstance());
      }
      try (Spanner spanner = builder.build().getService()) {
        DatabaseClient dbClient =
            spanner.getDatabaseClient(
                DatabaseId.of(
                    spannerClient.gcpProjectId,
                    spannerClient.spannerInstanceId,
                    spannerClient.spannerDatabaseId));
        String dml =
            String.format("DELETE FROM %s WHERE %s = @%s", tableName, columnName, columnName);
        Statement statement = Statement.newBuilder(dml).bind(columnName).to(value).build();
        long rowCount = dbClient.executePartitionedUpdate(statement);
        LOGGER.info("Deleted {} rows from {} for {} {}", rowCount, tableName, columnName, value);
        c.output(null);
      }
    }
  }

  public SpannerIO.Write getWriteTransform() {
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId(gcpProjectId)
            .withInstanceId(spannerInstanceId)
            .withDatabaseId(ValueProvider.StaticValueProvider.of(spannerDatabaseId))
            .withBatchSizeBytes(SPANNER_BATCH_SIZE_BYTES)
            .withMaxNumRows(SPANNER_MAX_NUM_ROWS)
            .withGroupingFactor(SPANNER_GROUPING_FACTOR)
            .withMaxNumMutations(SPANNER_MAX_NUM_MUTATIONS)
            .withCommitDeadline(Duration.standardSeconds(SPANNER_COMMIT_DEADLINE_SECONDS));

    if (emulatorHost != null) {
      write = write.withEmulatorHost(emulatorHost);
    }
    return write;
  }

  public WriteGrouped getWriteGroupedTransform() {
    return new WriteGrouped(getWriteTransform());
  }

  public Mutation toNodeMutation(Node node) {
    // Only update subject_id for provisional nodes.
    if (node.getTypes().size() == 1 && node.getTypes().contains("ProvisionalNode")) {
      return Mutation.newInsertOrUpdateBuilder(nodeTableName)
          .set("subject_id")
          .to(node.getSubjectId())
          .build();
    }
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
        .set("observation_about")
        .to(observation.getObservationAbout())
        .set("variable_measured")
        .to(observation.getVariableMeasured())
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
          getMutationValue(mutationMap, "observation_about"),
          getMutationValue(mutationMap, "variable_measured"),
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
    private String emulatorHost;

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

    public Builder emulatorHost(String emulatorHost) {
      this.emulatorHost = emulatorHost;
      return this;
    }

    public SpannerClient build() {
      return new SpannerClient(this);
    }
  }
}
