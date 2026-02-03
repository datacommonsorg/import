package org.datacommons.ingestion.data;

import com.google.cloud.spanner.Mutation;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptor;
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
  // Maximum size for a single column value in Spanner (10MB)
  private static final String DC_AGGREGATE = "dcAggregate/";
  private static final String DATCOM_AGGREGATE = "DataCommonsAggregate";
  private static final String IMPORT_METADATA_FILE = "import_metadata_mcf.mcf";

  public static PCollection<Node> combineNodes(PCollection<Node> nodes) {
    return nodes
        .apply(
            "MapNodesToKV",
            ParDo.of(
                new DoFn<Node, KV<String, Node>>() {
                  @ProcessElement
                  public void processElement(
                      @Element Node node, OutputReceiver<KV<String, Node>> receiver) {
                    receiver.output(KV.of(node.getSubjectId(), node));
                  }
                }))
        .apply(
            "CombineNodes",
            Combine.perKey(
                new Combine.CombineFn<Node, List<Node>, Node>() {
                  @Override
                  public List<Node> createAccumulator() {
                    return new ArrayList<>();
                  }

                  @Override
                  public List<Node> addInput(List<Node> accumulator, Node input) {
                    accumulator.add(input);
                    return accumulator;
                  }

                  @Override
                  public List<Node> mergeAccumulators(Iterable<List<Node>> accumulators) {
                    List<Node> merged = new ArrayList<>();
                    for (List<Node> acc : accumulators) {
                      merged.addAll(acc);
                    }
                    return merged;
                  }

                  @Override
                  public Node extractOutput(List<Node> accumulator) {
                    if (accumulator.isEmpty()) return null;
                    Node first = accumulator.get(0);
                    Node.Builder builder =
                        Node.builder()
                            .subjectId(first.getSubjectId())
                            .value(first.getValue())
                            .name(first.getName())
                            .types(first.getTypes())
                            .bytes(first.getBytes());

                    Set<String> types = new java.util.TreeSet<>();
                    for (Node n : accumulator) {
                      types.addAll(n.getTypes());
                      if (!n.getValue().isEmpty()) {
                        builder.value(n.getValue());
                      }
                      if (!n.getName().isEmpty()) {
                        builder.name(n.getName());
                      }
                      if (n.getBytes().length > 0) {
                        builder.bytes(n.getBytes());
                      }
                    }
                    if (types.size() > 1 && types.contains("ProvisionalNode")) {
                      types.remove("ProvisionalNode");
                    }
                    builder.types(new ArrayList<>(types));
                    return builder.build();
                  }
                }))
        .apply(
            "ExtractNodes",
            ParDo.of(
                new DoFn<KV<String, Node>, Node>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, Node> element, OutputReceiver<Node> receiver) {
                    receiver.output(element.getValue());
                  }
                }));
  }

  public static PCollection<Mutation> nodeToMutations(
      PCollection<Node> nodes, SpannerClient spannerClient) {
    return nodes.apply(
        "NodesToMutations",
        ParDo.of(
            new DoFn<Node, Mutation>() {
              @ProcessElement
              public void processElement(@Element Node node, OutputReceiver<Mutation> receiver) {
                Mutation mutation = spannerClient.toNodeMutation(node);
                if (mutation != null) {
                  receiver.output(mutation);
                }
              }
            }));
  }

  public static PCollection<Mutation> edgeToMutations(
      PCollection<Edge> edges, SpannerClient spannerClient) {
    return edges.apply(
        "EdgesToMutations",
        ParDo.of(
            new DoFn<Edge, Mutation>() {
              @ProcessElement
              public void processElement(@Element Edge edge, OutputReceiver<Mutation> receiver) {
                Mutation mutation = spannerClient.toEdgeMutation(edge);
                if (mutation != null) {
                  receiver.output(mutation);
                }
              }
            }));
  }

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
        node.value(subjectId);
        node.name(GraphUtils.getPropertyValue(pv, "name"));

        List<String> types = GraphUtils.getPropertyValues(pv, "typeOf");
        if (types.isEmpty()) {
          types = List.of(PipelineUtils.TYPE_THING);
          LOGGER.info("Found MCF node with no type: {}", nodeEntry.getKey());
          mcfNodesWithoutTypeCounter.inc();
        }
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
                node.bytes(PipelineUtils.compressString(val.getValue()));
              } else {
                node.value(val.getValue());
              }
              node.types(List.of(ValueType.TEXT.toString()));
              nodes.add(node.build());
            }
          }
        }
      }
    }
    return nodes;
  }

  public static PCollection<McfGraph> getProvenanceMcf(
      String bucketName, String importName, String latestVersion, Pipeline p) {
    String provenanceFile = "gs://" + bucketName + "/" + "provenance/" + importName + ".mcf";
    String metadataFile = latestVersion + "/" + IMPORT_METADATA_FILE;
    LOGGER.info("Reading provenance mcf from {} {}", provenanceFile, metadataFile);
    List<McfGraph> mcfList = new ArrayList<>();
    String defaultProvenance =
        "Node: dcid:dc/base/" + importName + "\n" + "typeOf: dcid:Provenance\n";
    mcfList.add(GraphUtils.convertToGraph(defaultProvenance));
    // try {
    //   mcfList.add(GraphUtils.convertToGraph(PipelineUtils.getGCSFileContent(metadataFile)));
    // } catch (IOException e) {
    //   LOGGER.warn("Failed to read provenance metadata file: " + e.getMessage());
    // }
    try {
      mcfList.add(GraphUtils.convertToGraph(PipelineUtils.getGCSFileContent(provenanceFile)));
    } catch (IOException e) {
      LOGGER.warn("Failed to read provenance metadata file: " + e.getMessage());
    }
    return p.apply(Create.of(mcfList).withType(TypeDescriptor.of(McfGraph.class)));
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

  public static PCollection<Node> mcfToNodes(
      PCollection<McfGraph> graph, Counter nodeCounter, Counter mcfNodesWithoutTypeCounter) {
    return graph.apply(
        "McfToNodes",
        ParDo.of(
            new DoFn<McfGraph, Node>() {
              @ProcessElement
              public void processElement(@Element McfGraph element, OutputReceiver<Node> receiver) {
                List<Node> nodes = graphToNodes(element, mcfNodesWithoutTypeCounter);
                for (Node node : nodes) {
                  // LOGGER.info("Node: {}", node.toString());
                  receiver.output(node);
                }
                nodeCounter.inc(nodes.size());
              }
            }));
  }

  public static PCollection<Edge> mcfToEdges(
      PCollection<McfGraph> graph, String provenance, Counter edgeCounter) {
    return graph.apply(
        "McfToEdges",
        ParDo.of(
            new DoFn<McfGraph, Edge>() {
              @ProcessElement
              public void processElement(@Element McfGraph element, OutputReceiver<Edge> receiver) {
                List<Edge> edges = graphToEdges(element, provenance);
                for (Edge edge : edges) {
                  receiver.output(edge);
                  // LOGGER.info("Edge : {}", edge.toString());
                }
                edgeCounter.inc(edges.size());
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
