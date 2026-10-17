package org.datacommons.ingestion.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.ingestion.spanner.model.ObservationRecord;
import org.datacommons.ingestion.spanner.model.TimeSeriesRecord;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerClient implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerClient.class);
  private static final Gson GSON = new Gson();

  // Decrease batch size for observations (bigger rows)
  private static final int SPANNER_BATCH_SIZE_BYTES = 500 * 1024;
  // Maximum size for a single string column value in Spanner (characters)
  public static final int MAX_SPANNER_STRING_COLUMN_SIZE = 2621440;
  // Maximum size for a single bytes column value in Spanner (10MB)
  public static final int MAX_SPANNER_COLUMN_SIZE = 10 * 1024 * 1024;
  // Increase batch size for Nodes/Edges (smaller rows)
  private static final int SPANNER_MAX_NUM_ROWS = 2000;
  // Higher value ensures this limit is not encountered before MaxNumRows
  private static final int SPANNER_MAX_NUM_MUTATIONS = 10000;
  // Use more rows for sorting/batching to limit batch to fewer splits
  private static final int SPANNER_GROUPING_FACTOR = 3000;
  // Commit deadline for spanner writes. Use large value for bigger batches.
  private static final int SPANNER_COMMIT_DEADLINE_SECONDS = 120;
  // Client-side deadline for Partitioned DML (Spanner client default is 2 hours). Large
  // provenance deletes cascade to millions of interleaved Observation rows in production.
  private static final long PARTITIONED_DML_TIMEOUT_HOURS = 6;

  private final String gcpProjectId;
  private final String spannerInstanceId;
  private final String spannerDatabaseId;
  private final String nodeTableName;
  private final String edgeTableName;
  private final String timeSeriesTableName;
  private final String observationTableName;
  private final int numShards;
  private final String emulatorHost;

  protected SpannerClient(Builder builder) {
    this.gcpProjectId = builder.gcpProjectId;
    this.spannerInstanceId = builder.spannerInstanceId;
    this.spannerDatabaseId = builder.spannerDatabaseId;
    this.nodeTableName = builder.nodeTableName;
    this.edgeTableName = builder.edgeTableName;
    this.timeSeriesTableName = builder.timeSeriesTableName;
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

  public PCollection<Void> deleteDataForImport(
      Pipeline pipeline, String importName, String tableName, String columnName) {
    String stageName = tableName + "-" + importName.replaceFirst("^dc/base/", "");
    return pipeline
        .apply("StartDelete" + stageName, Create.of(importName))
        .apply(
            "ExecuteDelete" + stageName,
            ParDo.of(new DeleteByColumnFn(this, tableName, columnName)));
  }

  /**
   * Deletes TimeSeries rows (and their interleaved Observation rows via ON DELETE CASCADE) for the
   * given provenances, issuing one Partitioned DML statement per {@code variable_measured}.
   *
   * <p>A single {@code DELETE FROM TimeSeries WHERE provenance = ...} over a large import can leave
   * a few dense variables undeleted: the partitions covering them cascade to millions of
   * Observation rows (amplified by change streams in production), never complete, and the statement
   * runs until the Partitioned DML deadline. Bounding each statement by {@code variable_measured}
   * (the leading primary key column) keeps every statement to a contiguous key range of the base
   * table.
   *
   * @return the total number of TimeSeries rows deleted.
   */
  public long deleteTimeSeriesByProvenances(
      DatabaseClient dbClient, String tableName, List<String> provenances) {
    if (provenances == null || provenances.isEmpty()) {
      return 0L;
    }
    // Index-only lookup on TimeSeriesByProvenance (provenance, variable_measured, ...).
    Statement variablesQuery =
        Statement.newBuilder(
                String.format(
                    "SELECT DISTINCT %s FROM %s WHERE %s IN UNNEST(@%s)",
                    TimeSeriesRecord.COL_VARIABLE_MEASURED,
                    tableName,
                    TimeSeriesRecord.COL_PROVENANCE,
                    TimeSeriesRecord.COL_PROVENANCE))
            .bind(TimeSeriesRecord.COL_PROVENANCE)
            .toStringArray(provenances)
            .build();
    List<String> variables = new ArrayList<>();
    try (ResultSet rs = dbClient.singleUse().executeQuery(variablesQuery)) {
      while (rs.next()) {
        variables.add(rs.getString(TimeSeriesRecord.COL_VARIABLE_MEASURED));
      }
    }

    String deleteDml =
        String.format(
            "DELETE FROM %s WHERE %s = @%s AND %s IN UNNEST(@%s)",
            getDeleteTarget(tableName, TimeSeriesRecord.COL_PROVENANCE),
            TimeSeriesRecord.COL_VARIABLE_MEASURED,
            TimeSeriesRecord.COL_VARIABLE_MEASURED,
            TimeSeriesRecord.COL_PROVENANCE,
            TimeSeriesRecord.COL_PROVENANCE);
    long totalDeleted = 0L;
    for (String variable : variables) {
      Statement deleteStmt =
          Statement.newBuilder(deleteDml)
              .bind(TimeSeriesRecord.COL_VARIABLE_MEASURED)
              .to(variable)
              .bind(TimeSeriesRecord.COL_PROVENANCE)
              .toStringArray(provenances)
              .build();
      long deleted = dbClient.executePartitionedUpdate(deleteStmt);
      LOGGER.info(
          "Deleted {} rows from {} for variable_measured {} and provenance IN {}",
          deleted,
          tableName,
          variable,
          provenances);
      totalDeleted += deleted;
    }
    return totalDeleted;
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
      try (Spanner spanner = spannerClient.createSpanner()) {
        DatabaseClient dbClient = spannerClient.getDatabaseClient(spanner);
        long rowCount;
        if (spannerClient.isTimeSeriesProvenanceDelete(tableName, columnName)) {
          rowCount =
              spannerClient.deleteTimeSeriesByProvenances(dbClient, tableName, List.of(value));
        } else {
          String dml =
              String.format("DELETE FROM %s WHERE %s = @%s", tableName, columnName, columnName);
          Statement statement = Statement.newBuilder(dml).bind(columnName).to(value).build();
          rowCount = dbClient.executePartitionedUpdate(statement);
        }
        LOGGER.info("Deleted {} rows from {} for {} {}", rowCount, tableName, columnName, value);
        c.output(null);
      }
    }
  }

  public SpannerIO.Read getReadTransform() {
    SpannerIO.Read read =
        SpannerIO.read()
            .withProjectId(gcpProjectId)
            .withInstanceId(spannerInstanceId)
            .withDatabaseId(spannerDatabaseId)
            .withLowPriority();
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      read = read.withEmulatorHost(emulatorHost.trim());
    }
    return read;
  }

  public Spanner createSpanner() {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder()
            .setProjectId(gcpProjectId)
            .setPartitionedDmlTimeoutDuration(
                java.time.Duration.ofHours(PARTITIONED_DML_TIMEOUT_HOURS));
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      builder.setEmulatorHost(emulatorHost.trim());
      builder.setCredentials(NoCredentials.getInstance());
    }
    return builder.build().getService();
  }

  public DatabaseClient getDatabaseClient(Spanner spanner) {
    return spanner.getDatabaseClient(
        DatabaseId.of(gcpProjectId, spannerInstanceId, spannerDatabaseId));
  }

  public SpannerIO.Write getWriteTransform() {
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId(gcpProjectId)
            .withInstanceId(spannerInstanceId)
            .withDatabaseId(ValueProvider.StaticValueProvider.of(spannerDatabaseId))
            // Note: add parameter tuning here based on the size of mutations and latency
            // requirements.
            // .withBatchSizeBytes(SPANNER_BATCH_SIZE_BYTES)
            // .withMaxNumRows(SPANNER_MAX_NUM_ROWS)
            // .withGroupingFactor(SPANNER_GROUPING_FACTOR)
            // .withMaxNumMutations(SPANNER_MAX_NUM_MUTATIONS)
            .withCommitDeadline(Duration.standardSeconds(SPANNER_COMMIT_DEADLINE_SECONDS))
            .withLowPriority();

    if (emulatorHost != null) {
      write = write.withEmulatorHost(emulatorHost);
    }
    return write;
  }

  public Mutation toNodeMutation(Node node) {
    // Only update subject_id for provisional nodes.
    if (node.getTypes().size() == 1 && node.getTypes().contains("ProvisionalNode")) {
      return Mutation.newInsertOrUpdateBuilder(nodeTableName)
          .set(NodeRecord.COL_SUBJECT_ID)
          .to(node.getSubjectId())
          .set(NodeRecord.COL_LAST_UPDATE_TIMESTAMP)
          .to(Value.COMMIT_TIMESTAMP)
          .build();
    }
    return NodeRecord.from(node).toMutation(this.nodeTableName);
  }

  public Mutation toEdgeMutation(Edge edge) {
    return EdgeRecord.from(edge).toMutation(this.edgeTableName);
  }

  public List<KV<String, Mutation>> toGraphKVMutations(List<Node> nodes, List<Edge> edges) {
    return Stream.concat(
            nodes.stream().map(this::toNodeMutation), edges.stream().map(this::toEdgeMutation))
        .map(mutation -> KV.of(getGraphKVKey(mutation), mutation))
        .toList();
  }

  public Mutation toTimeSeriesMutation(TimeSeries obs) {
    return TimeSeriesRecord.from(obs).toMutation(this.timeSeriesTableName);
  }

  public Mutation toObservationMutation(Observation obs) {
    return ObservationRecord.from(obs).toMutation(this.observationTableName);
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
  public static String getMutationValue(Map<String, Value> mutationMap, String columnName) {
    return mutationMap.getOrDefault(columnName, Value.string("")).getString();
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

  public String getTimeSeriesTableName() {
    return timeSeriesTableName;
  }

  public String getObservationTableName() {
    return observationTableName;
  }

  public String getEmulatorHost() {
    return emulatorHost;
  }

  /** Returns true if this is a delete of TimeSeries rows by provenance. */
  public boolean isTimeSeriesProvenanceDelete(String tableName, String columnName) {
    return tableName.equals(timeSeriesTableName)
        && TimeSeriesRecord.COL_PROVENANCE.equals(columnName);
  }

  /**
   * Returns the table reference to use in a Partitioned DML {@code DELETE ... WHERE columnName ...}
   * statement.
   *
   * <p>For {@code TimeSeries} deletes by {@code provenance}, the query planner otherwise picks the
   * {@code TimeSeriesByProvenance} index, whose partitions are not aligned with the interleaved
   * {@code Observation} rows. Cascading deletes across unaligned partitions can exceed
   * per-partition transaction limits (especially with change streams) and stall until {@code
   * DEADLINE_EXCEEDED}. Forcing a base table scan aligns partitions with the {@code
   * TimeSeries}/{@code Observation} key ranges. The hint is skipped on the emulator.
   */
  public String getDeleteTarget(String tableName, String columnName) {
    boolean isEmulator = emulatorHost != null && !emulatorHost.trim().isEmpty();
    if (!isEmulator
        && tableName.equals(timeSeriesTableName)
        && TimeSeriesRecord.COL_PROVENANCE.equals(columnName)) {
      return tableName + "@{FORCE_INDEX=_BASE_TABLE}";
    }
    return tableName;
  }

  /**
   * Prepares a SQL query for SpannerIO partitioned reads by conditionally prepending the
   * emulator-specific hint ({@code @{spanner_emulator.disable_query_partitionability_check=true}})
   * when running against the Cloud Spanner Emulator. In production Cloud Spanner, standard SQL is
   * preserved.
   */
  public String formatPartitionQuery(String format, Object... args) {
    String query = (args == null || args.length == 0) ? format : String.format(format, args);
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      return "@{spanner_emulator.disable_query_partitionability_check=true} " + query;
    }
    return query;
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
            + "timeSeriesTableName='%s', "
            + "observationTableName='%s'"
            + "}",
        gcpProjectId,
        spannerInstanceId,
        spannerDatabaseId,
        nodeTableName,
        edgeTableName,
        timeSeriesTableName,
        observationTableName);
  }

  public static class Builder {
    private String gcpProjectId;
    private String spannerInstanceId;
    private String spannerDatabaseId;
    private String nodeTableName = "Node";
    private String edgeTableName = "Edge";
    private String timeSeriesTableName = "TimeSeries";
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

    public Builder timeSeriesTableName(String timeSeriesTableName) {
      this.timeSeriesTableName = timeSeriesTableName;
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
