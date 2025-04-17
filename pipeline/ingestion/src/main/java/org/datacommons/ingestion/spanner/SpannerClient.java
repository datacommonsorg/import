package org.datacommons.ingestion.spanner;

import static java.util.stream.Collectors.toList;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Stream;

import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Write;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;

import com.google.cloud.spanner.Mutation;

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

    public Mutation toNodeMutation(Node node) {
        return Mutation.newInsertOrUpdateBuilder(nodeTableName)
                .set("subject_id").to(node.getSubjectId())
                .set("name").to(node.getName())
                .set("types").toStringArray(node.getTypes())
                .build();
    }

    public Mutation toEdgeMutation(Edge edge) {
        return Mutation.newInsertOrUpdateBuilder(edgeTableName)
                .set("subject_id").to(edge.getSubjectId())
                .set("predicate").to(edge.getPredicate())
                .set("object_id").to(edge.getObjectId())
                .set("object_value").to(edge.getObjectValue())
                .set("provenance").to(edge.getProvenance())
                .set("object_hash").to(edge.getObjectHash())
                .build();
    }

    public List<Mutation> toGraphMutations(List<Node> nodes, List<Edge> edges) {
        return Stream.concat(
                nodes.stream().map(this::toNodeMutation),
                edges.stream().map(this::toEdgeMutation))
                .collect(toList());
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
                "SpannerClient{" +
                        "gcpProjectId='%s', " +
                        "spannerInstanceId='%s', " +
                        "spannerDatabaseId='%s', " +
                        "nodeTableName='%s', " +
                        "edgeTableName='%s', " +
                        "observationTableName='%s'" +
                        "}",
                gcpProjectId, spannerInstanceId, spannerDatabaseId, nodeTableName, edgeTableName, observationTableName);
    }

    public static class Builder {
        private String gcpProjectId;
        private String spannerInstanceId;
        private String spannerDatabaseId;
        private String nodeTableName = "Node";
        private String edgeTableName = "Edge";
        private String observationTableName = "Observation";

        private Builder() {
        }

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
