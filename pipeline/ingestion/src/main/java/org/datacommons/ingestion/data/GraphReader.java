package org.datacommons.ingestion.data;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.datacommons.Storage.Observations;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.util.GraphUtils;
import org.datacommons.util.McfUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphReader implements Serializable {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphReader.class);
  private static final String DC_AGGREGATE = "dcAggregate/";
  private static final String DATCOM_AGGREGATE = "DataCommonsAggregate";

  public static List<Node> graphToNodes(McfGraph graph, Counter mcfNodesWithoutTypeCounter) {
    List<Node> nodes = new ArrayList<>();
    for (Map.Entry<String, PropertyValues> nodeEntry : graph.getNodesMap().entrySet()) {
      PropertyValues pvs = nodeEntry.getValue();
      if (!GraphUtils.isObservation(pvs)) {

        // Generate corresponding node
        Map<String, McfGraph.Values> pv = pvs.getPvsMap();
        Node.Builder node = Node.builder();
        String dcid = GraphUtils.getPropertyValue(pv, "dcid");
        String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());
        node.subjectId(subjectId);

        List<String> types = GraphUtils.getPropertyValues(pv, "typeOf");
        if (types.isEmpty()) {
          types = List.of(PipelineUtils.TYPE_THING);
          LOGGER.info("Found MCF node with no type: {}", nodeEntry.getKey());
          mcfNodesWithoutTypeCounter.inc();
        }
        node.value(subjectId);
        node.name(GraphUtils.getPropertyValue(pv, "name"));
        node.types(types);
        nodes.add(node.build());

        // Generate any leaf nodes
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) {
          for (TypedValue val : entry.getValue().getTypedValuesList()) {
            if (val.getType() != ValueType.RESOLVED_REF) {
              int valSize = val.getValue().getBytes(StandardCharsets.UTF_8).length;
              if (valSize > SpannerClient.MAX_SPANNER_COLUMN_SIZE) {
                LOGGER.warn(
                    "Dropping node from {} because value size {} exceeds max size.",
                    subjectId,
                    valSize);
                continue;
              }
              node = Node.builder();
              node.subjectId(PipelineUtils.generateObjectValueKey(val.getValue()));
              if (PipelineUtils.storeValueAsBytes(entry.getKey())) {
                node.bytes(ByteArray.copyFrom(PipelineUtils.compressString(val.getValue())));
              } else {
                node.value(val.getValue());
              }
              nodes.add(node.build());
            }
          }
        }
      }
    }
    return nodes;
  }

  public static List<Edge> graphToEdges(McfGraph graph, String provenance) {
    List<Edge> edges = new ArrayList<>();
    for (Map.Entry<String, PropertyValues> nodeEntry : graph.getNodesMap().entrySet()) {
      PropertyValues pvs = nodeEntry.getValue();
      if (!GraphUtils.isObservation(pvs)) {
        Map<String, McfGraph.Values> pv = pvs.getPvsMap();
        String dcid = GraphUtils.getPropertyValue(pv, "dcid");
        String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) {
          for (TypedValue val : entry.getValue().getTypedValuesList()) {
            if (val.getType() != ValueType.RESOLVED_REF) {
              int valSize = val.getValue().getBytes(StandardCharsets.UTF_8).length;
              if (valSize > SpannerClient.MAX_SPANNER_COLUMN_SIZE) {
                LOGGER.warn(
                    "Dropping edge from {} because value size {} exceeds max size.",
                    subjectId,
                    valSize);
                continue;
              }
            }
            Edge.Builder edge = Edge.builder();
            edge.subjectId(subjectId);
            edge.predicate(entry.getKey());
            edge.provenance(provenance);
            if (val.getType() == ValueType.RESOLVED_REF) {
              edge.objectId(McfUtil.stripNamespace(val.getValue()));
            } else {
              edge.objectId(PipelineUtils.generateObjectValueKey(val.getValue()));
            }
            edges.add(edge.build());
          }
        }
      }
    }
    return edges;
  }

  public static Observation graphToObservations(McfOptimizedGraph graph, String importName) {
    Observation.Builder obs = Observation.builder();
    String measurementMethod = graph.getSvObsSeries().getKey().getMeasurementMethod();
    obs.observationAbout(graph.getSvObsSeries().getKey().getObservationAbout());
    obs.observationPeriod(graph.getSvObsSeries().getKey().getObservationPeriod());
    obs.importName(importName);
    if (measurementMethod.startsWith(DC_AGGREGATE)) {
      obs.isDcAggregate(true);
      measurementMethod = measurementMethod.replace(DC_AGGREGATE, "");
    }
    if (measurementMethod == DATCOM_AGGREGATE) {
      obs.isDcAggregate(true);
      measurementMethod = "";
    }
    obs.measurementMethod(measurementMethod);
    obs.variableMeasured(graph.getSvObsSeries().getKey().getVariableMeasured());
    obs.unit(graph.getSvObsSeries().getKey().getUnit());
    obs.scalingFactor(graph.getSvObsSeries().getKey().getScalingFactor());
    Observations.Builder ob = Observations.newBuilder();
    for (StatVarObs svo : graph.getSvObsSeries().getSvObsListList()) {
      if (svo.hasNumber()) {
        ob.putValues(svo.getDate(), Double.toString(svo.getNumber()));
      } else if (svo.hasText()) {
        ob.putValues(svo.getDate(), svo.getText());
      }
    }
    obs.observations(ob.build());
    return obs.build();
  }

  public static PCollection<Mutation> getDeleteMutations(
      String importName, String provenance, Pipeline pipeline, SpannerClient spannerClient) {
    return PCollectionList.of(spannerClient.getObservationDeleteMutations(importName, pipeline))
        .and(spannerClient.getEdgeDeleteMutations(provenance, pipeline))
        .apply("FlattenDeleteMutations", Flatten.pCollections());
  }

  public static PCollection<KV<String, Mutation>> graphToObservations(
      PCollection<McfOptimizedGraph> graph,
      String importName,
      SpannerClient spannerClient,
      Counter obsCounter) {
    return graph.apply(
        "GraphToObs",
        ParDo.of(
            new DoFn<McfOptimizedGraph, KV<String, Mutation>>() {
              @ProcessElement
              public void processElement(
                  @Element McfOptimizedGraph element,
                  OutputReceiver<KV<String, Mutation>> receiver) {
                Observation observations = graphToObservations(element, importName);
                List<KV<String, Mutation>> obs =
                    spannerClient.toObservationKVMutations(List.of(observations));
                obs.stream()
                    .forEach(
                        e -> {
                          receiver.output(e);
                        });
                obsCounter.inc(obs.size());
              }
            }));
  }

  public static PCollection<KV<String, Mutation>> graphToNodes(
      PCollection<McfGraph> graph,
      SpannerClient spannerClient,
      Counter nodeCounter,
      Counter mcfNodesWithoutTypeCounter) {
    return graph.apply(
        "GraphToNodes",
        ParDo.of(
            new DoFn<McfGraph, KV<String, Mutation>>() {
              @ProcessElement
              public void processElement(
                  @Element McfGraph element, OutputReceiver<KV<String, Mutation>> receiver) {
                List<Node> nodes = graphToNodes(element, mcfNodesWithoutTypeCounter);
                List<KV<String, Mutation>> mutations =
                    spannerClient.toGraphKVMutations(nodes, Collections.emptyList());
                mutations.stream()
                    .forEach(
                        e -> {
                          receiver.output(e);
                        });
                nodeCounter.inc(mutations.size());
              }
            }));
  }

  public static PCollection<KV<String, Mutation>> graphToEdges(
      PCollection<McfGraph> graph,
      String provenance,
      SpannerClient spannerClient,
      Counter edgeCounter) {
    return graph.apply(
        "GraphToEdges",
        ParDo.of(
            new DoFn<McfGraph, KV<String, Mutation>>() {
              @ProcessElement
              public void processElement(
                  @Element McfGraph element, OutputReceiver<KV<String, Mutation>> receiver) {
                List<Edge> edges = graphToEdges(element, provenance);
                List<KV<String, Mutation>> mutations =
                    spannerClient.toGraphKVMutations(Collections.emptyList(), edges);
                mutations.stream()
                    .forEach(
                        e -> {
                          receiver.output(e);
                        });
                edgeCounter.inc(mutations.size());
              }
            }));
  }
}
