package org.datacommons.ingestion.data;

import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Storage.Observations;
import org.datacommons.util.GraphUtils;

public class GraphReader implements Serializable {
  public static List<Node> graphToNodes(McfGraph graph) {
    List<Node> nodes = new ArrayList<>();
    for (Map.Entry<String, PropertyValues> entry : graph.getNodesMap().entrySet()) {
      PropertyValues pvs = entry.getValue();
      if (!GraphUtils.isObservation(pvs)) {
        Map<String, McfGraph.Values> pv = pvs.getPvsMap();
        Node.Builder node = Node.builder();
        node.subjectId(entry.getKey());
        node.name(GraphUtils.getPropertyValue(pv, "name"));
        node.types(GraphUtils.getPropertyValues(pv, "typeOf"));
        nodes.add(node.build());
      }
    }
    return nodes;
  }

  public static List<Edge> graphToEdges(McfGraph graph) {
    List<Edge> edges = new ArrayList<>();
    for (Map.Entry<String, PropertyValues> nodeEntry : graph.getNodesMap().entrySet()) {
      PropertyValues pvs = nodeEntry.getValue();
      if (!GraphUtils.isObservation(pvs)) {
        Map<String, McfGraph.Values> pv = pvs.getPvsMap();
        String subjectId = nodeEntry.getKey(); // Use the map key as the subjectId
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) { // Iterate over properties
          for (TypedValue val : entry.getValue().getTypedValuesList()) {
            Edge.Builder edge = Edge.builder();
            edge.subjectId(subjectId);
            edge.predicate(entry.getKey());
            edge.objectId(val.getValue());
            edges.add(edge.build());
          }
        }
      }
    }
    return edges;
  }

  public static Observation graphToObservations(McfOptimizedGraph graph) {
    Observation.Builder obs = Observation.builder();
    obs.observationAbout(graph.getSvObsSeries().getKey().getObservationAbout());
    obs.observationPeriod(graph.getSvObsSeries().getKey().getObservationPeriod());
    obs.measurementMethod(graph.getSvObsSeries().getKey().getMeasurementMethod());
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

  public static PCollection<KV<String, Mutation>> graphToObservations(
      PCollection<McfOptimizedGraph> graph, SpannerClient spannerClient) {
    return graph.apply(
        "GraphToObs",
        ParDo.of(
            new DoFn<McfOptimizedGraph, KV<String, Mutation>>() {
              @ProcessElement
              public void processElement(
                  @Element McfOptimizedGraph element,
                  OutputReceiver<KV<String, Mutation>> receiver) {
                Observation observations = graphToObservations(element);
                List<KV<String, Mutation>> obs =
                    spannerClient.toObservationKVMutations(List.of(observations));
                obs.stream()
                    .forEach(
                        e -> {
                          receiver.output(e);
                        });
              }
            }));
  }

  public static PCollection<KV<String, Mutation>> graphToNodeEdges(
      PCollection<McfGraph> graph, SpannerClient spannerClient) {
    return graph.apply(
        "GrapphToNodeEdge",
        ParDo.of(
            new DoFn<McfGraph, KV<String, Mutation>>() {
              @ProcessElement
              public void processElement(
                  @Element McfGraph element, OutputReceiver<KV<String, Mutation>> receiver) {
                List<Edge> edges = graphToEdges(element);
                List<Node> nodes = graphToNodes(element);
                List<KV<String, Mutation>> obs = spannerClient.toGraphKVMutations(nodes, edges);
                obs.stream()
                    .forEach(
                        e -> {
                          receiver.output(e);
                        });
              }
            }));
  }
}
