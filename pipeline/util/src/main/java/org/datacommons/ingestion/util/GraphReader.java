package org.datacommons.ingestion.util;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
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

  public static PCollection<TimeSeries> extractUniqueSeries(
      PCollection<McfGraph> graph, String importName, boolean isBaseDc, Counter tsCounter) {
    PCollection<TimeSeries> series =
        graph.apply(
            "ExtractTimeSeries-" + importName,
            ParDo.of(new ExtractTimeSeriesFn(importName, isBaseDc)));

    PCollection<TimeSeries> uniqueSeries =
        series.apply(
            "DeduplicateSeries-" + importName,
            Distinct.withRepresentativeValueFn(
                new SerializableFunction<TimeSeries, String>() {
                  @Override
                  public String apply(TimeSeries ts) {
                    return ts.getDedupeKey();
                  }
                }));

    return uniqueSeries.apply(
        "CountTimeSeries-" + importName, ParDo.of(new CountTimeSeriesFn(tsCounter)));
  }

  public static class CountTimeSeriesFn extends DoFn<TimeSeries, TimeSeries> {
    private final Counter tsCounter;

    public CountTimeSeriesFn(Counter tsCounter) {
      this.tsCounter = tsCounter;
    }

    @ProcessElement
    public void processElement(@Element TimeSeries ts, OutputReceiver<TimeSeries> receiver) {
      tsCounter.inc();
      receiver.output(ts);
    }
  }

  public static PCollection<Observation> extractObservations(
      PCollection<McfGraph> graph, String importName, boolean isBaseDc, Counter obsCounter) {
    return graph.apply(
        "ExtractObservationDataPoints-" + importName,
        ParDo.of(new ExtractObservationsFn(importName, obsCounter)));
  }

  /** Extracts unique TimeSeries (metadata series keys) from observation nodes. */
  public static class ExtractTimeSeriesFn extends DoFn<McfGraph, TimeSeries> {
    private static final int MAX_BUNDLE_CACHE_SIZE = 100_000;
    private final String importName;
    private final boolean isBaseDc;
    private transient Set<String> bundleSeenKeys;

    public ExtractTimeSeriesFn(String importName, boolean isBaseDc) {
      this.importName = importName;
      this.isBaseDc = isBaseDc;
    }

    @StartBundle
    public void startBundle() {
      bundleSeenKeys = new HashSet<>();
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      McfGraph g = c.element();
      for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
        PropertyValues pv = entry.getValue();
        if (GraphUtils.isObservation(pv)) {
          TimeSeries ts = extractTimeSeries(entry.getKey(), pv, importName, isBaseDc);
          if (bundleSeenKeys.size() >= MAX_BUNDLE_CACHE_SIZE) {
            bundleSeenKeys.clear();
          }
          if (bundleSeenKeys.add(ts.getDedupeKey())) {
            c.output(ts);
          }
        }
      }
    }

    @FinishBundle
    public void finishBundle() {
      bundleSeenKeys = null;
    }
  }

  /**
   * Extracts individual Observation data points (date, value, series key) from observation nodes.
   */
  public static class ExtractObservationsFn extends DoFn<McfGraph, Observation> {
    private final String importName;
    private final Counter obsCounter;

    public ExtractObservationsFn(String importName, Counter obsCounter) {
      this.importName = importName;
      this.obsCounter = obsCounter;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      McfGraph g = c.element();
      for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
        PropertyValues pv = entry.getValue();
        if (!GraphUtils.isObservation(pv)) {
          continue;
        }
        TimeSeriesKey seriesKey = extractTimeSeriesKey(entry.getKey(), pv, importName);

        String date = GraphUtils.getPropVal(pv, "observationDate");
        String value = GraphUtils.getPropVal(pv, "value");

        Observation obs =
            Observation.builder().seriesKey(seriesKey).date(date).value(value).build();

        c.output(obs);
        obsCounter.inc();
      }
    }
  }

  private static class EntitiesInfo {
    final String entity1;
    final List<String> extraEntities;

    EntitiesInfo(String entity1, List<String> extraEntities) {
      this.entity1 = entity1;
      this.extraEntities = extraEntities;
    }
  }

  private static EntitiesInfo extractEntities(String nodeId, PropertyValues pv) {
    String entity1 = "";
    List<String> extraEntities = new ArrayList<>();

    List<String> obsProps = null;
    List<String> rawProps = GraphUtils.getPropertyValues(pv.getPvsMap(), "observationProperties");
    if (rawProps != null && !rawProps.isEmpty()) {
      obsProps = rawProps.stream().sorted().collect(Collectors.toList());
    }

    if (obsProps != null && !obsProps.isEmpty()) {
      // Multi-entity case
      List<String> entityValues = new ArrayList<>();
      for (String prop : obsProps) {
        String val = GraphUtils.getPropVal(pv, prop);
        if (val.isEmpty()) {
          throw new RuntimeException(
              String.format("Observation %s is missing expected entity property %s", nodeId, prop));
        }
        entityValues.add(val);
      }
      if (!entityValues.isEmpty()) {
        entity1 = entityValues.get(0);
        for (int i = 1; i < entityValues.size(); i++) {
          extraEntities.add(entityValues.get(i));
        }
      }
    } else {
      // Standard case
      entity1 = GraphUtils.getPropVal(pv, "observationAbout");
      if (entity1.isEmpty()) {
        throw new RuntimeException(
            String.format("Observation %s is missing observationAbout", nodeId));
      }
    }
    return new EntitiesInfo(entity1, extraEntities);
  }

  public static TimeSeriesKey extractTimeSeriesKey(
      String nodeId, PropertyValues pv, String importName) {
    String sv = GraphUtils.getPropVal(pv, "variableMeasured");

    EntitiesInfo entitiesInfo = extractEntities(nodeId, pv);
    String entity1 = entitiesInfo.entity1;
    String extraEntitiesId = Joiner.on("^").useForNull("").join(entitiesInfo.extraEntities);

    String measurementMethod = GraphUtils.getPropVal(pv, "measurementMethod");
    boolean isDcAggregate = false;
    if (measurementMethod.startsWith(DC_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = measurementMethod.replace(DC_AGGREGATE, "");
    }
    if (measurementMethod.equals(DATCOM_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = "";
    }

    String observationPeriod = GraphUtils.getPropVal(pv, "observationPeriod");
    String unit = GraphUtils.getPropVal(pv, "unit");
    String scalingFactor = GraphUtils.getPropVal(pv, "scalingFactor");

    String facetId =
        TimeSeries.calculateFacetId(
            importName, measurementMethod, observationPeriod, scalingFactor, unit, isDcAggregate);

    return new TimeSeriesKey(
        sv,
        entity1,
        extraEntitiesId,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        facetId);
  }

  public static TimeSeries extractTimeSeries(
      String nodeId, PropertyValues pv, String importName, boolean isBaseDc) {

    String sv = GraphUtils.getPropVal(pv, "variableMeasured");

    EntitiesInfo entitiesInfo = extractEntities(nodeId, pv);
    String entity1 = entitiesInfo.entity1;

    String measurementMethod = GraphUtils.getPropVal(pv, "measurementMethod");
    boolean isDcAggregate = false;
    if (measurementMethod.startsWith(DC_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = measurementMethod.replace(DC_AGGREGATE, "");
    }
    if (measurementMethod.equals(DATCOM_AGGREGATE)) {
      isDcAggregate = true;
      measurementMethod = "";
    }

    String observationPeriod = GraphUtils.getPropVal(pv, "observationPeriod");
    String unit = GraphUtils.getPropVal(pv, "unit");
    String scalingFactor = GraphUtils.getPropVal(pv, "scalingFactor");
    String provenanceUrl = GraphUtils.getPropVal(pv, "provenanceUrl");

    return TimeSeries.builder()
        .variableMeasured(sv)
        .entity1(entity1)
        .extraEntities(entitiesInfo.extraEntities)
        .observationPeriod(observationPeriod)
        .measurementMethod(measurementMethod)
        .unit(unit)
        .scalingFactor(scalingFactor)
        .importName(importName)
        .isBaseDc(isBaseDc)
        .isDcAggregate(isDcAggregate)
        .provenanceUrl(provenanceUrl)
        .build();
  }

  public static PCollection<TimeSeries> extractSeriesFromOptimized(
      PCollection<McfOptimizedGraph> graph,
      String importName,
      boolean isBaseDc,
      Counter tsCounter) {
    return graph.apply(
        "SeriesToTimeSeriesObservations-" + importName,
        ParDo.of(
            new DoFn<McfOptimizedGraph, TimeSeries>() {
              @ProcessElement
              public void processElement(
                  @Element McfOptimizedGraph g, OutputReceiver<TimeSeries> receiver) {
                receiver.output(toTimeSeries(g.getSvObsSeries().getKey(), importName, isBaseDc));
                tsCounter.inc();
              }
            }));
  }

  public static PCollection<Observation> extractObservationsFromOptimized(
      PCollection<McfOptimizedGraph> graph,
      String importName,
      boolean isBaseDc,
      Counter obsCounter) {
    return graph.apply(
        "ExtractObservationDataPoints-" + importName,
        ParDo.of(
            new DoFn<McfOptimizedGraph, Observation>() {
              @ProcessElement
              public void processElement(
                  @Element McfOptimizedGraph g, OutputReceiver<Observation> receiver) {
                McfStatVarObsSeries svoSeries = g.getSvObsSeries();
                TimeSeriesKey seriesKey = toTimeSeriesKey(svoSeries.getKey(), importName);
                for (StatVarObs obs : svoSeries.getSvObsListList()) {
                  receiver.output(toObservation(seriesKey, obs));
                  obsCounter.inc();
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

  static ImmutableList<NodeSubGraph> extractNodeSubGraphsFromGraph(
      McfGraph graph, String provenance, Counter nodesWithoutTypeCounter) {
    return graph.getNodesMap().entrySet().stream()
        .filter(nodeEntry -> !GraphUtils.isObservation(nodeEntry.getValue()))
        .flatMap(
            nodeEntry ->
                extractNodeSubGraphs(nodeEntry, provenance, nodesWithoutTypeCounter).stream())
        .collect(ImmutableList.toImmutableList());
  }

  static ImmutableList<NodeSubGraph> extractNodeSubGraphs(
      Map.Entry<String, PropertyValues> nodeEntry,
      String provenance,
      Counter nodesWithoutTypeCounter) {
    PropertyValues pvs = nodeEntry.getValue();
    Map<String, McfGraph.Values> pv = pvs.getPvsMap();
    String dcid = GraphUtils.getPropVal(pvs, GraphUtils.Property.dcid.name());
    String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());

    List<String> types = GraphUtils.getPropertyValues(pv, GraphUtils.Property.typeOf.name());
    if (types.isEmpty()) {
      types = ImmutableList.of(PipelineUtils.TYPE_THING);
      LOGGER.info("Found MCF node with no type: {}", nodeEntry.getKey());
      nodesWithoutTypeCounter.inc();
    }
    String name = GraphUtils.getPropVal(pvs, GraphUtils.Property.name.name());
    Node subjectNode =
        Node.builder()
            .subjectId(subjectId)
            .value(subjectId)
            .name(!name.isEmpty() ? name : subjectId)
            .types(types)
            .build();

    ImmutableList<KV<Edge, Node>> edgesAndLeafNodes =
        pv.entrySet().stream()
            .filter(entry -> !entry.getKey().equals(GraphUtils.Property.dcid.name()))
            .flatMap(
                entry ->
                    entry.getValue().getTypedValuesList().stream()
                        .map(
                            val ->
                                extractEdgeAndLeafNode(subjectId, entry.getKey(), provenance, val))
                        .filter(Objects::nonNull))
            .collect(ImmutableList.toImmutableList());

    ImmutableList<Edge> subjectEdges =
        edgesAndLeafNodes.stream().map(KV::getKey).collect(ImmutableList.toImmutableList());

    ImmutableList<NodeSubGraph> leafEntries =
        edgesAndLeafNodes.stream()
            .map(KV::getValue)
            .filter(Objects::nonNull)
            .map(leafNode -> NodeSubGraph.builder().node(leafNode).build())
            .collect(ImmutableList.toImmutableList());

    return ImmutableList.<NodeSubGraph>builder()
        .add(NodeSubGraph.builder().node(subjectNode).edges(subjectEdges).build())
        .addAll(leafEntries)
        .build();
  }

  static KV<Edge, Node> extractEdgeAndLeafNode(
      String subjectId, String predicate, String provenance, TypedValue val) {
    String objectId;
    Node leafNode = null;
    if (val.getType() == ValueType.RESOLVED_REF) {
      objectId = McfUtil.stripNamespace(val.getValue());
    } else {
      leafNode = buildLeafNode(subjectId, predicate, val.getValue());
      if (leafNode == null) {
        return null;
      }
      objectId = leafNode.getSubjectId();
    }
    Edge edge =
        Edge.builder()
            .subjectId(subjectId)
            .predicate(predicate)
            .provenance(provenance)
            .objectId(objectId)
            .build();
    return KV.of(edge, leafNode);
  }

  static Node buildLeafNode(String subjectId, String predicate, String rawValue) {
    boolean storeAsBytes = PipelineUtils.storeValueAsBytes(predicate);
    byte[] compressedBytes = storeAsBytes ? PipelineUtils.compressString(rawValue) : null;
    int valSize = storeAsBytes ? compressedBytes.length : rawValue.length();
    int maxSize =
        storeAsBytes
            ? SpannerClient.MAX_SPANNER_COLUMN_SIZE
            : SpannerClient.MAX_SPANNER_STRING_COLUMN_SIZE;
    if (valSize > maxSize) {
      LOGGER.warn(
          "Dropping node and edge from {} because value size {} exceeds max size.",
          subjectId,
          valSize);
      return null;
    }
    Node.Builder leafNode =
        Node.builder().subjectId(PipelineUtils.generateObjectValueKey(rawValue));
    if (storeAsBytes) {
      leafNode.bytes(ByteArray.copyFrom(compressedBytes));
    } else {
      leafNode.value(rawValue);
    }
    return leafNode.build();
  }

  public static PCollection<NodeSubGraph> extractNodeSubGraphsFromGraph(
      String importName,
      PCollection<McfGraph> graph,
      String provenance,
      Counter nodesWithoutTypeCounter) {
    return graph.apply(
        "ExtractNodeSubGraphsFromGraph-" + importName,
        ParDo.of(
            new DoFn<McfGraph, NodeSubGraph>() {
              @ProcessElement
              public void processElement(
                  @Element McfGraph element, OutputReceiver<NodeSubGraph> receiver) {
                extractNodeSubGraphsFromGraph(element, provenance, nodesWithoutTypeCounter)
                    .forEach(receiver::output);
              }
            }));
  }

  public static PCollection<Mutation> nodesToMutations(
      String importName,
      PCollection<NodeSubGraph> nodeSubGraphs,
      SpannerClient spannerClient,
      Counter nodeCounter) {
    return nodeSubGraphs.apply(
        "GenerateNodeMutations-" + importName,
        ParDo.of(
            new DoFn<NodeSubGraph, Mutation>() {
              @ProcessElement
              public void processElement(
                  @Element NodeSubGraph element, OutputReceiver<Mutation> receiver) {
                receiver.output(spannerClient.toNodeMutation(element.getNode()));
                nodeCounter.inc();
              }
            }));
  }

  public static PCollection<Mutation> edgesToMutations(
      String importName,
      PCollection<NodeSubGraph> nodeSubGraphs,
      SpannerClient spannerClient,
      Counter edgeCounter) {
    return nodeSubGraphs.apply(
        "GenerateEdgeMutations-" + importName,
        ParDo.of(
            new DoFn<NodeSubGraph, Mutation>() {
              @ProcessElement
              public void processElement(
                  @Element NodeSubGraph element, OutputReceiver<Mutation> receiver) {
                List<Edge> edges = element.getEdges();
                edges.forEach(edge -> receiver.output(spannerClient.toEdgeMutation(edge)));
                edgeCounter.inc(edges.size());
              }
            }));
  }
}
