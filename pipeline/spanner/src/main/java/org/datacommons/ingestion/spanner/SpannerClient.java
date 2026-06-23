package org.datacommons.ingestion.spanner;

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Value;
import com.google.common.base.Joiner;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Serializable;
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
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
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
            // Note: add parameter tuning here based on the size of mutations and latency
            // requirements.
            // .withBatchSizeBytes(SPANNER_BATCH_SIZE_BYTES)
            // .withMaxNumRows(SPANNER_MAX_NUM_ROWS)
            // .withGroupingFactor(SPANNER_GROUPING_FACTOR)
            // .withMaxNumMutations(SPANNER_MAX_NUM_MUTATIONS)
            .withCommitDeadline(Duration.standardSeconds(SPANNER_COMMIT_DEADLINE_SECONDS));

    if (emulatorHost != null) {
      write = write.withEmulatorHost(emulatorHost);
    }
    return write;
  }

  public Mutation toNodeMutation(Node node) {
    // Only update subject_id for provisional nodes.
    if (node.getTypes().size() == 1 && node.getTypes().contains("ProvisionalNode")) {
      return Mutation.newInsertOrUpdateBuilder(nodeTableName)
          .set("subject_id")
          .to(node.getSubjectId())
          .set("last_update_timestamp")
          .to(Value.COMMIT_TIMESTAMP)
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
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
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

  public List<KV<String, Mutation>> toGraphKVMutations(List<Node> nodes, List<Edge> edges) {
    return Stream.concat(
            nodes.stream().map(this::toNodeMutation), edges.stream().map(this::toEdgeMutation))
        .map(mutation -> KV.of(getGraphKVKey(mutation), mutation))
        .toList();
  }

  public Mutation toTimeSeriesMutation(TimeSeries obs) {
    String variableMeasured = obs.getVariableMeasured();
    String entity1 = obs.getEntity1();
    String extraEntitiesId = obs.getExtraEntitiesId();
    String facetId = obs.getFacetId();

    // Create entities JSON
    JsonObject entitiesJson = new JsonObject();
    entitiesJson.addProperty("entity1", entity1);
    List<String> extra = obs.getExtraEntities();
    if (extra != null) {
      if (extra.size() > 0) {
        addPropertyIfNotEmpty(entitiesJson, "entity2", extra.get(0));
      }
      if (extra.size() > 1) {
        addPropertyIfNotEmpty(entitiesJson, "entity3", extra.get(1));
      }
    }

    // Create facet JSON
    JsonObject facetJson = new JsonObject();
    facetJson.addProperty(
        "provenance", ProvenanceUtils.getProvenanceDcid(obs.getImportName(), obs.getIsBaseDc()));
    addPropertyIfNotEmpty(facetJson, "measurementMethod", obs.getMeasurementMethod());
    addPropertyIfNotEmpty(facetJson, "observationPeriod", obs.getObservationPeriod());
    addPropertyIfNotEmpty(facetJson, "scalingFactor", obs.getScalingFactor());
    addPropertyIfNotEmpty(facetJson, "unit", obs.getUnit());
    facetJson.addProperty("isDcAggregate", obs.getIsDcAggregate());

    return Mutation.newInsertOrUpdateBuilder(timeSeriesTableName)
        .set("variable_measured")
        .to(variableMeasured)
        // entity1 is a STORED generated column in TimeSeries, DO NOT write to it directly!
        .set("extra_entities_id")
        .to(extraEntitiesId)
        .set("facet_id")
        .to(facetId)
        .set("entities")
        .to(Value.json(GSON.toJson(entitiesJson)))
        .set("facet")
        .to(Value.json(GSON.toJson(facetJson)))
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
  }

  private static void addPropertyIfNotEmpty(JsonObject json, String property, String value) {
    if (value != null && !value.trim().isEmpty()) {
      json.addProperty(property, value);
    }
  }

  public Mutation toObservationMutation(Observation obs) {
    TimeSeriesKey key = obs.getSeriesKey();
    return Mutation.newInsertOrUpdateBuilder(observationTableName)
        .set("variable_measured")
        .to(key.getVariableMeasured())
        .set("entity1")
        .to(key.getEntity1())
        .set("extra_entities_id")
        .to(key.getExtraEntitiesId())
        .set("facet_id")
        .to(key.getFacetId())
        .set("date")
        .to(obs.getDate())
        .set("value")
        .to(obs.getValue())
        .set("last_update_timestamp")
        .to(Value.COMMIT_TIMESTAMP)
        .build();
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
