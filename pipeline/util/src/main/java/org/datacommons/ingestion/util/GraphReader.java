package org.datacommons.ingestion.util;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
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
        String dcid = GraphUtils.getPropVal(pvs, GraphUtils.Property.dcid.name());
        String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());
        node.subjectId(subjectId);

        List<String> types = GraphUtils.getPropertyValues(pv, "typeOf");
        if (types.isEmpty()) {
          types = List.of(PipelineUtils.TYPE_THING);
          LOGGER.info("Found MCF node with no type: {}", nodeEntry.getKey());
          mcfNodesWithoutTypeCounter.inc();
        }
        node.value(subjectId);
        String name = GraphUtils.getPropVal(pvs, GraphUtils.Property.name.name());
        node.name(!name.isEmpty() ? name : subjectId);
        node.types(types);
        nodes.add(node.build());

        // Generate any leaf nodes
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) {
          if (entry.getKey().equals("dcid")) {
            continue;
          }
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
        String dcid = GraphUtils.getPropVal(pvs, GraphUtils.Property.dcid.name());
        String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) {
          if (entry.getKey().equals("dcid")) {
            continue;
          }
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

  public static PCollection<TimeSeries> extractUniqueSeries(
      PCollection<McfGraph> graph, String importName, boolean isBaseDc, Counter tsCounter) {
    PCollection<McfStatVarObsSeries.Key> keys =
        graph.apply(
            "ExtractSeriesKeys-" + importName,
            ParDo.of(
                new DoFn<McfGraph, McfStatVarObsSeries.Key>() {
                  @ProcessElement
                  public void processElement(
                      @Element McfGraph g, OutputReceiver<McfStatVarObsSeries.Key> receiver) {
                    for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
                      PropertyValues pv = entry.getValue();
                      if (GraphUtils.isObservation(pv)) {
                        McfStatVarObsSeries svoSeries =
                            GraphUtils.convertMcfGraphToMcfStatVarObsSeries(entry.getKey(), pv);
                        receiver.output(svoSeries.getKey());
                      }
                    }
                  }
                }));

    PCollection<McfStatVarObsSeries.Key> uniqueKeys =
        keys.apply("DeduplicateKeys-" + importName, Distinct.create());

    return uniqueKeys.apply(
        "KeysToTimeSeriesObservations-" + importName,
        ParDo.of(
            new DoFn<McfStatVarObsSeries.Key, TimeSeries>() {
              @ProcessElement
              public void processElement(
                  @Element McfStatVarObsSeries.Key key, OutputReceiver<TimeSeries> receiver) {
                receiver.output(toTimeSeries(key, importName, isBaseDc));
                tsCounter.inc();
              }
            }));
  }

  public static PCollection<Observation> extractObservations(
      PCollection<McfGraph> graph, String importName, boolean isBaseDc, Counter obsCounter) {
    return graph.apply(
        "ExtractObservationDataPoints-" + importName,
        ParDo.of(
            new DoFn<McfGraph, Observation>() {
              @ProcessElement
              public void processElement(
                  @Element McfGraph g, OutputReceiver<Observation> receiver) {
                for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
                  PropertyValues pv = entry.getValue();
                  if (GraphUtils.isObservation(pv)) {
                    McfStatVarObsSeries svoSeries =
                        GraphUtils.convertMcfGraphToMcfStatVarObsSeries(entry.getKey(), pv);
                    TimeSeriesKey seriesKey = toTimeSeriesKey(svoSeries.getKey(), importName);
                    for (StatVarObs obs : svoSeries.getSvObsListList()) {
                      receiver.output(toObservation(seriesKey, obs));
                      obsCounter.inc();
                    }
                  }
                }
              }
            }));
  }

  static TimeSeries toTimeSeries(McfStatVarObsSeries.Key key, String importName, boolean isBaseDc) {
    String measurementMethod = key.getMeasurementMethod();
    boolean isDcAggregate = false;
    if (measurementMethod.startsWith(DC_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = measurementMethod.replace(DC_AGGREGATE, "");
    }
    if (measurementMethod.equals(DATCOM_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = "";
    }

    return TimeSeries.builder()
        .variableMeasured(key.getVariableMeasured())
        .entity1(key.getObservationAbout())
        .observationPeriod(key.getObservationPeriod())
        .measurementMethod(measurementMethod)
        .unit(key.getUnit())
        .scalingFactor(key.getScalingFactor())
        .importName(importName)
        .isBaseDc(isBaseDc)
        .isDcAggregate(isDcAggregate)
        .provenanceUrl(key.getProvenanceUrl())
        .build();
  }

  static TimeSeriesKey toTimeSeriesKey(McfStatVarObsSeries.Key key, String importName) {
    String measurementMethod = key.getMeasurementMethod();
    boolean isDcAggregate = false;
    if (measurementMethod.startsWith(DC_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = measurementMethod.replace(DC_AGGREGATE, "");
    }
    if (measurementMethod.equals(DATCOM_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = "";
    }

    String facetId =
        TimeSeries.calculateFacetId(
            importName,
            measurementMethod,
            key.getObservationPeriod(),
            key.getScalingFactor(),
            key.getUnit(),
            isDcAggregate);

    return new TimeSeriesKey(
        key.getVariableMeasured(),
        key.getObservationAbout(),
        "",
        key.getObservationPeriod(),
        measurementMethod,
        key.getUnit(),
        key.getScalingFactor(),
        facetId);
  }

  static Observation toObservation(TimeSeriesKey seriesKey, StatVarObs obs) {
    String value = "";
    if (obs.hasNumber()) {
      value = Double.toString(obs.getNumber());
    } else if (obs.hasText()) {
      value = obs.getText();
    }

    return Observation.builder().seriesKey(seriesKey).date(obs.getDate()).value(value).build();
  }

  public static PCollection<KV<String, Mutation>> graphToNodes(
      String importName,
      PCollection<McfGraph> graph,
      SpannerClient spannerClient,
      Counter nodeCounter,
      Counter mcfNodesWithoutTypeCounter) {
    return graph.apply(
        "GenerateNodeMutations-" + importName,
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
      String importName,
      PCollection<McfGraph> graph,
      String provenance,
      SpannerClient spannerClient,
      Counter edgeCounter) {
    return graph.apply(
        "GenerateEdgeMutations-" + importName,
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
