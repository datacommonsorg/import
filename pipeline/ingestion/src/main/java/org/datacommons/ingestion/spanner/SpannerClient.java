package org.datacommons.ingestion.spanner;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.WriteGrouped;
import org.apache.beam.sdk.values.KV;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;

public class SpannerClient implements Serializable {

  private final String gcpProjectId;
  private final String spannerInstanceId;
  private final String spannerDatabaseId;
  private final String nodeTableName;
  private final String edgeTableName;
  private final String observationTableName;

  private SpannerClient(Builder builder) {
    this.gcpProjectId = builder.gcpProjectId;
    this.spannerInstanceId = builder.spannerInstanceId;
    this.spannerDatabaseId = builder.spannerDatabaseId;
    this.nodeTableName = builder.nodeTableName;
    this.edgeTableName = builder.edgeTableName;
    this.observationTableName = builder.observationTableName;
  }

  public Write getWriteTransform() {
    return SpannerIO.write()
        .withProjectId(gcpProjectId)
        .withInstanceId(spannerInstanceId)
        .withDatabaseId(spannerDatabaseId);
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
        .set("provenance")
        .to(observation.getProvenance())
        .set("observation_period")
        .to(observation.getObservationPeriod())
        .set("measurement_method")
        .to(observation.getMeasurementMethod())
        .set("unit")
        .to(observation.getUnit())
        .set("scaling_factor")
        .to(observation.getScalingFactor())
        .set("observations")
        .to(Value.jsonArray(observation.getObservationsAsJsonStrings()))
        .set("import_name")
        .to(observation.getImportName())
        .set("provenance_url")
        .to(observation.getProvenanceUrl())
        .build();
  }

  public List<KV<String, Mutation>> toNodeMutations(List<Node> nodes) {
    return nodes.stream()
        .map(node -> KV.of(node.getSubjectId(), toNodeMutation(node)))
        .toList();
  }

  public List<KV<String, Mutation>> toEdgeMutations(List<Edge> edges) {
    return edges.stream()
        .map(edge -> KV.of(edge.getSubjectId(), toEdgeMutation(edge)))
        .toList();
  }

  public List<MutationGroup> toObservationMutations(List<Observation> observations) {
    return toObservationMutationGroups(observations.stream().map(this::toObservationMutation).toList());
  }

  public List<Mutation> toGraphMutations(List<Node> nodes, List<Edge> edges) {
    return Stream.concat(
            nodes.stream().map(this::toNodeMutation), edges.stream().map(this::toEdgeMutation))
        .toList();
  }

  public List<KV<String, Mutation>> toGraphKVMutations(List<Node> nodes, List<Edge> edges) {
    return Stream.concat(
                    nodes.stream().map(this::toNodeMutation),
                    edges.stream().map(this::toEdgeMutation))
            .map(mutation -> KV.of(getSubjectId(mutation), mutation))
            .toList();
  }

  public List<KV<String, Mutation>> filterGraphKVMutations(List<KV<String, Mutation>> kvs, Set<String> seenNodes) {
    var filtered = new ArrayList<KV<String, Mutation>>();
    for (var kv : kvs) {
      var mutation = kv.getValue();
      // Skip duplicate node mutations for the same subject_id
      if (mutation.getTable().equals(nodeTableName)) {
        String subjectId = getSubjectId(mutation);
        if (seenNodes.contains(subjectId)) {
          continue;
        }
        seenNodes.add(subjectId);
      }

      filtered.add(kv);
    }
    return filtered;
  }

  public List<MutationGroup> toGraphMutationGroups(List<Mutation> mutations) {
    Map<String, List<Mutation>> mutationMap = new HashMap<>();
    Set<String> nodeIds = new HashSet<>();

    for (Mutation mutation : mutations) {
      String subjectId = getSubjectId(mutation);

      // Skip duplicate node mutations for the same subject_id
      if (mutation.getTable().equals(nodeTableName) && nodeIds.contains(subjectId)) {
        continue;
      }

      nodeIds.add(subjectId);
      mutationMap.computeIfAbsent(subjectId, k -> new ArrayList<>()).add(mutation);
    }

    List<MutationGroup> mutationGroups = new ArrayList<>();
    for (List<Mutation> mutationList : mutationMap.values()) {
      if (mutationList.size() == 1) {
        mutationGroups.add(MutationGroup.create(mutationList.get(0)));
      } else {
        Mutation first = mutationList.get(0);
        List<Mutation> rest = mutationList.subList(1, mutationList.size());
        mutationGroups.add(MutationGroup.create(first, rest));
      }
    }
    return mutationGroups;
  }

  /**
   * Creates mutation groups for observations. Each group contains mutations for the same variable
   * and place.
   */
  public List<MutationGroup> toObservationMutationGroups(List<Mutation> mutations) {
    // Map from Pair(variable_measured, observation_about) to mutation.
    // Using SimpleEntry for representing a pair.
    Map<SimpleEntry<String, String>, List<Mutation>> mutationMap = new HashMap<>();

    for (Mutation mutation : mutations) {
      String variableMeasured = mutation.asMap().get("variable_measured").getString();
      String observationAbout = mutation.asMap().get("observation_about").getString();

      SimpleEntry<String, String> key = new SimpleEntry<>(variableMeasured, observationAbout);

      mutationMap.computeIfAbsent(key, k -> new ArrayList<>()).add(mutation);
    }

    List<MutationGroup> mutationGroups = new ArrayList<>();
    for (List<Mutation> mutationList : mutationMap.values()) {
      if (mutationList.size() == 1) {
        mutationGroups.add(MutationGroup.create(mutationList.get(0)));
      } else {
        Mutation first = mutationList.get(0);
        List<Mutation> rest = mutationList.subList(1, mutationList.size());
        mutationGroups.add(MutationGroup.create(first, rest));
      }
    }
    return mutationGroups;
  }

  public static String getSubjectId(Mutation mutation) {
    return mutation.asMap().get("subject_id").getString();
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

    public SpannerClient build() {
      return new SpannerClient(this);
    }
  }
}
