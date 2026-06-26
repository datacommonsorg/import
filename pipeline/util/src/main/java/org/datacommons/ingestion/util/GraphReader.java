package org.datacommons.ingestion.util;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.common.base.Joiner;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
              boolean storeAsBytes = PipelineUtils.storeValueAsBytes(entry.getKey());
              byte[] compressedBytes =
                  storeAsBytes ? PipelineUtils.compressString(val.getValue()) : null;
              int valSize = storeAsBytes ? compressedBytes.length : val.getValue().length();
              int maxSize =
                  storeAsBytes
                      ? SpannerClient.MAX_SPANNER_COLUMN_SIZE
                      : SpannerClient.MAX_SPANNER_STRING_COLUMN_SIZE;
              if (valSize > maxSize) {
                LOGGER.warn(
                    "Dropping node from {} because value size {} exceeds max size.",
                    subjectId,
                    valSize);
                continue;
              }
              node = Node.builder();
              node.subjectId(PipelineUtils.generateObjectValueKey(val.getValue()));
              if (storeAsBytes) {
                node.bytes(ByteArray.copyFrom(compressedBytes));
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
        String nodeProvenance = getProvenance(pvs, provenance);
        String dcid = GraphUtils.getPropVal(pvs, GraphUtils.Property.dcid.name());
        String subjectId = !dcid.isEmpty() ? dcid : McfUtil.stripNamespace(nodeEntry.getKey());
        for (Map.Entry<String, McfGraph.Values> entry : pv.entrySet()) {
          if (entry.getKey().equals("dcid")) {
            continue;
          }
          for (TypedValue val : entry.getValue().getTypedValuesList()) {
            if (val.getType() != ValueType.RESOLVED_REF) {
              boolean storeAsBytes = PipelineUtils.storeValueAsBytes(entry.getKey());
              int maxSize =
                  storeAsBytes
                      ? SpannerClient.MAX_SPANNER_COLUMN_SIZE
                      : SpannerClient.MAX_SPANNER_STRING_COLUMN_SIZE;

              int valSize = val.getValue().length();
              if (storeAsBytes && valSize > maxSize) {
                valSize = PipelineUtils.compressString(val.getValue()).length;
              }

              if (valSize > maxSize) {
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
            edge.provenance(nodeProvenance);
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
        "CountTimeSeries-" + importName,
        ParDo.of(
            new DoFn<TimeSeries, TimeSeries>() {
              @ProcessElement
              public void processElement(
                  @Element TimeSeries ts, OutputReceiver<TimeSeries> receiver) {
                tsCounter.inc();
                receiver.output(ts);
              }
            }));
  }

  public static PCollection<Observation> extractObservations(
      PCollection<McfGraph> graph, String importName, boolean isBaseDc, Counter obsCounter) {
    return graph.apply(
        "ExtractObservationDataPoints-" + importName,
        ParDo.of(new ExtractObservationsFn(importName, obsCounter)));
  }

  /** Extracts unique TimeSeries (metadata series keys) from observation nodes. */
  public static class ExtractTimeSeriesFn extends DoFn<McfGraph, TimeSeries> {
    private final String importName;
    private final boolean isBaseDc;

    public ExtractTimeSeriesFn(String importName, boolean isBaseDc) {
      this.importName = importName;
      this.isBaseDc = isBaseDc;
    }

    @ProcessElement
    public void processElement(ProcessContext c) {
      McfGraph g = c.element();
      for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
        PropertyValues pv = entry.getValue();
        if (GraphUtils.isObservation(pv)) {
          TimeSeries ts = extractTimeSeries(entry.getKey(), pv, importName, isBaseDc);
          c.output(ts);
        }
      }
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

    String provenance = getProvenance(pv, importName);

    String facetId =
        TimeSeries.calculateFacetId(
            provenance, measurementMethod, observationPeriod, scalingFactor, unit, isDcAggregate);

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

    String provenance = getProvenance(pv, importName);

    return TimeSeries.builder()
        .variableMeasured(sv)
        .entity1(entity1)
        .extraEntities(entitiesInfo.extraEntities)
        .observationPeriod(observationPeriod)
        .measurementMethod(measurementMethod)
        .unit(unit)
        .scalingFactor(scalingFactor)
        .importName(provenance)
        .isBaseDc(isBaseDc)
        .isDcAggregate(isDcAggregate)
        .provenanceUrl(provenanceUrl)
        .build();
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

  /**
   * Fallback logic to handle base DC case where provenance mcf node may not be a part of the data
   * files of the import.
   */
  private static String getProvenance(PropertyValues pv, String fallbackProvenance) {
    String provenance = GraphUtils.getPropVal(pv, "provenance");
    return provenance.isEmpty() ? fallbackProvenance : provenance;
  }
}
