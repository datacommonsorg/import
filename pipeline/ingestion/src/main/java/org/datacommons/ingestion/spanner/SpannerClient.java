package org.datacommons.ingestion.spanner;

import static org.datacommons.ingestion.data.ProtoUtil.serializeSpannerProto;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Options.RpcPriority;
import com.google.cloud.spanner.Value;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.SpannerConfig;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped;
import org.apache.beam.sdk.values.KV;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpannerClient implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerClient.class);

  private static final int MAX_LOG_SAMPLES = 1000;
  private int numObsDupsLogged = 0;
  private int numNodeDupsLogged = 0;
  private int numEdgeGroupByKeysLogged = 0;

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
        .withSpannerConfig(SpannerConfig.create().withRpcPriority(RpcPriority.HIGH))
        .withProjectId(gcpProjectId)
        .withInstanceId(spannerInstanceId)
        .withDatabaseId(spannerDatabaseId)
        .withMaxCommitDelay(50)
        .withBatchSizeBytes(1024 * 1024)
        .withMaxNumMutations(10000)
        .withGroupingFactor(100)
        .withCommitDeadline(Duration.standardSeconds(120));
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
    return Mutation.newInsertOrUpdateBuilder(edgeTableName)
        .set("subject_id")
        .to(edge.getSubjectId())
        .set("predicate")
        .to(edge.getPredicate())
        .set("object_id")
        .to(edge.getObjectId())
        .set("object_value")
        .to(edge.getObjectValue())
        .set("provenance")
        .to(edge.getProvenance())
        .set("object_hash")
        .to(edge.getObjectHash())
        .build();
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
        .to(ByteArray.copyFrom(serializeSpannerProto(observation.getObservations())))
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
        if (numObsDupsLogged < MAX_LOG_SAMPLES) {
          LOGGER.info("Duplicate observation: {}", key);
          numObsDupsLogged++;
        }
        continue;
      }
      seenObs.add(key);

      filtered.add(kv);
    }
    return filtered;
  }

  public List<KV<String, Mutation>> filterGraphKVMutations(
      List<KV<String, Mutation>> kvs, Set<String> seenNodes) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var mutation = kv.getValue();
      // Skip duplicate node mutations for the same subject_id
      if (mutation.getTable().equals(nodeTableName)) {
        String subjectId = getSubjectId(mutation);
        if (seenNodes.contains(subjectId)) {
          if (numNodeDupsLogged < MAX_LOG_SAMPLES) {
            LOGGER.info("Duplicate node: {}", subjectId);
            numNodeDupsLogged++;
          }
          continue;
        }
        seenNodes.add(subjectId);
      }

      filtered.add(kv);
    }
    return filtered;
  }

  public static String getSubjectId(Mutation mutation) {
    return getMutationValue(mutation, "subject_id");
  }

  private static String getMutationValue(Mutation mutation, String columnName) {
    return mutation.asMap().getOrDefault(columnName, Value.string("")).getString();
  }

  public String getGraphKVKey(Mutation mutation) {
    String subjectId = getSubjectId(mutation);
    if (numShards <= 1 || !mutation.getTable().equals(edgeTableName)) {
      return subjectId;
    }

    String objectId = getMutationValue(mutation, "object_id");
    String objectHash = getMutationValue(mutation, "object_hash");
    int shard = Math.abs(Objects.hash(objectId, objectHash)) % numShards;

    var key = Joiner.on("::").join(subjectId, shard);

    if (numEdgeGroupByKeysLogged < MAX_LOG_SAMPLES) {
      LOGGER.info("Edge GroupBy key: {}", key);
      numEdgeGroupByKeysLogged++;
    }

    return key;
  }

  public String getObservationKVKey(Mutation mutation) {
    var parts =
        new Object[] {
          getMutationValue(mutation, "variable_measured"),
          hashShard(
              getMutationValue(mutation, "observation_about"),
              getMutationValue(mutation, "import_name"))
        };

    return Joiner.on("::").join(parts);
  }

  private int hashShard(Object... values) {
    return Math.abs(Objects.hash(values)) % numShards;
  }

  public static String getFullObservationKey(Mutation mutation) {
    var parts =
        new String[] {
          getMutationValue(mutation, "variable_measured"),
          getMutationValue(mutation, "observation_about"),
          getMutationValue(mutation, "import_name"),
          getMutationValue(mutation, "observation_period"),
          getMutationValue(mutation, "measurement_method"),
          getMutationValue(mutation, "unit"),
          getMutationValue(mutation, "scaling_factor")
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
