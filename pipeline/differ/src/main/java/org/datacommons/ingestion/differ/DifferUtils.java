package org.datacommons.ingestion.differ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.util.GraphUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util functions for the differ pipeline. */
public class DifferUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(DifferUtils.class);

  public enum Diff {
    ADDED,
    DELETED,
    MODIFIED,
    MODIFIED_PREVIOUS,
    UNMODIFIED;
  }

  // Counters
  public static final Counter numPreviousObs =
      Metrics.counter(DifferUtils.class, "previous_obs_count");
  public static final Counter numCurrentObs =
      Metrics.counter(DifferUtils.class, "current_obs_count");
  public static final Counter numPreviousSchema =
      Metrics.counter(DifferUtils.class, "previous_schema_count");
  public static final Counter numCurrentSchema =
      Metrics.counter(DifferUtils.class, "current_schema_count");

  public static final Counter numAddedObs = Metrics.counter(DifferUtils.class, "added_obs_count");
  public static final Counter numDeletedObs =
      Metrics.counter(DifferUtils.class, "deleted_obs_count");
  public static final Counter numModifiedObs =
      Metrics.counter(DifferUtils.class, "modified_obs_count");

  public static final Counter numAddedSchema =
      Metrics.counter(DifferUtils.class, "added_schema_count");
  public static final Counter numDeletedSchema =
      Metrics.counter(DifferUtils.class, "deleted_schema_count");
  public static final Counter numModifiedSchema =
      Metrics.counter(DifferUtils.class, "modified_schema_count");

  public static final TupleTag<KV<String, PropertyValues>> OBSERVATION_NODES_TAG =
      new TupleTag<KV<String, PropertyValues>>() {};
  public static final TupleTag<KV<String, PropertyValues>> SCHEMA_NODES_TAG =
      new TupleTag<KV<String, PropertyValues>>() {};

  public static final String[] GROUPBY_PROPERTIES = {
    "variableMeasured",
    "observationAbout",
    "observationDate",
    "observationPeriod",
    "measurementMethod",
    "unit",
    "scalingFactor"
  };

  /**
   * Converts an MCF graph to a PCollectionTuple of observation and schema nodes.
   *
   * @param graph input graph to process
   * @param isCurrent whether the graph is from the current data
   * @return PCollectionTuple of observation and schema nodes
   */
  public static PCollectionTuple processGraph(PCollection<McfGraph> graph, boolean isCurrent) {
    return graph.apply(
        "ProcessGraph",
        ParDo.of(
                new DoFn<McfGraph, KV<String, PropertyValues>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    McfGraph g = c.element();
                    for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
                      PropertyValues pvs = entry.getValue();
                      Map<String, McfGraph.Values> pv = pvs.getPvsMap();
                      if (GraphUtils.isObservation(pvs)) {
                        if (isCurrent) {
                          numCurrentObs.inc();
                        } else {
                          numPreviousObs.inc();
                        }
                        String pointKey =
                            Arrays.asList(GROUPBY_PROPERTIES).stream()
                                .map(prop -> GraphUtils.getPropertyValue(pv, prop))
                                .collect(Collectors.joining(";"));
                        c.output(OBSERVATION_NODES_TAG, KV.of(pointKey, pvs));
                      } else {
                        if (isCurrent) {
                          numCurrentSchema.inc();
                        } else {
                          numPreviousSchema.inc();
                        }
                        c.output(SCHEMA_NODES_TAG, KV.of(entry.getKey(), pvs));
                      }
                    }
                  }
                })
            .withOutputTags(OBSERVATION_NODES_TAG, TupleTagList.of(SCHEMA_NODES_TAG)));
  }

  /**
   * Generates diffs of two versions of a dataset
   *
   * @param currentNodes current data
   * @param previousNodes previous data
   * @param isObservation whether the nodes are observations
   * @return PCollection of diffs b/w datasets (KV of DiffType and MCF String)
   */
  public static PCollection<KV<String, String>> performDiff(
      PCollection<KV<String, PropertyValues>> currentNodes,
      PCollection<KV<String, PropertyValues>> previousNodes,
      boolean isObservation,
      boolean updateCounters) {
    TupleTag<PropertyValues> currentTag = new TupleTag<>();
    TupleTag<PropertyValues> previousTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> joinedResult =
        KeyedPCollectionTuple.of(currentTag, currentNodes)
            .and(previousTag, previousNodes)
            .apply(CoGroupByKey.create());

    return joinedResult.apply(
        "PerformDiff",
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
              @ProcessElement
              public void process(ProcessContext c) {
                KV<String, CoGbkResult> input = c.element();
                Iterable<PropertyValues> currentPvs = input.getValue().getAll(currentTag);
                Iterable<PropertyValues> previousPvs = input.getValue().getAll(previousTag);

                List<String> currentCanonicals = new ArrayList<>();
                for (PropertyValues pv : currentPvs) {
                  currentCanonicals.add(getCanonicalNodeValue(pv));
                }
                Collections.sort(currentCanonicals);
                String currentCanonical = String.join("||", currentCanonicals);

                List<String> previousCanonicals = new ArrayList<>();
                for (PropertyValues pv : previousPvs) {
                  previousCanonicals.add(getCanonicalNodeValue(pv));
                }
                Collections.sort(previousCanonicals);
                String previousCanonical = String.join("||", previousCanonicals);

                Diff diff;
                if (!currentCanonical.equals(previousCanonical)) {
                  String mcfString = "";
                  String nodeId = input.getKey();

                  if (currentCanonicals.isEmpty()) {
                    diff = Diff.DELETED;
                    if (updateCounters) {
                      if (isObservation) {
                        numDeletedObs.inc();
                      } else {
                        numDeletedSchema.inc();
                      }
                    }
                    mcfString = iterablesToMcfString(previousPvs);
                  } else if (previousCanonicals.isEmpty()) {
                    diff = Diff.ADDED;
                    if (updateCounters) {
                      if (isObservation) {
                        numAddedObs.inc();
                      } else {
                        numAddedSchema.inc();
                      }
                    }
                    mcfString = iterablesToMcfString(currentPvs);
                  } else {
                    diff = Diff.MODIFIED;
                    if (updateCounters) {
                      if (isObservation) {
                        numModifiedObs.inc();
                      } else {
                        numModifiedSchema.inc();
                      }
                    }
                    mcfString = iterablesToMcfString(currentPvs);
                    c.output(
                        KV.of(Diff.MODIFIED_PREVIOUS.name(), iterablesToMcfString(previousPvs)));
                  }
                  c.output(KV.of(diff.name(), mcfString));
                }
              }
            }));
  }

  private static String iterablesToMcfString(Iterable<PropertyValues> pvs) {
    List<String> nodeStrings = new ArrayList<>();
    for (PropertyValues pv : pvs) {
      String nodeId = GraphUtils.getPropVal(pv, "dcid");
      if (nodeId.isEmpty() && pv.getPvsMap().containsKey("Node")) {
        nodeId = pv.getPvsMap().get("Node").getTypedValues(0).getValue();
      }
      if (nodeId.isEmpty()) {
        nodeId = "dcid:dc/o/" + java.util.UUID.randomUUID().toString().replace("-", "");
      }
      nodeStrings.add(nodeToMcfString(nodeId, pv));
    }
    Collections.sort(nodeStrings);
    StringBuilder sb = new StringBuilder();
    for (String nodeStr : nodeStrings) {
      sb.append(nodeStr).append("\n");
    }
    return sb.toString();
  }

  /**
   * Generates a canonical string representation of a node's values.
   *
   * @param pvs property values of the node
   * @return canonical string string
   */
  private static String getCanonicalNodeValue(PropertyValues pvs) {
    // Use TreeMap to ensure deterministic ordering of keys for diffing.
    Map<String, McfGraph.Values> sortedPv = new java.util.TreeMap<>(pvs.getPvsMap());
    if (!GraphUtils.isObservation(pvs)) {
      sortedPv.remove(GraphUtils.Property.dcid.name());
      sortedPv.remove("Node");
    }

    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, McfGraph.Values> entry : sortedPv.entrySet()) {
      sb.append(entry.getKey()).append("=");
      List<String> values = new ArrayList<>();
      for (McfGraph.TypedValue tv : entry.getValue().getTypedValuesList()) {
        values.add(tv.toString().replace("\n", " "));
      }
      Collections.sort(values);
      sb.append(values.toString()).append(";");
    }
    return sb.toString();
  }

  /**
   * Converts a node to its MCF string representation.
   *
   * @param nodeId the node ID
   * @param pvs property values
   * @return MCF string
   */
  private static String nodeToMcfString(String nodeId, PropertyValues pvs) {
    StringBuilder sb = new StringBuilder();
    sb.append("Node: ").append(nodeId).append("\n");
    // Use TreeMap to ensure deterministic ordering of properties
    Map<String, McfGraph.Values> sortedPvs = new java.util.TreeMap<>(pvs.getPvsMap());
    for (Map.Entry<String, McfGraph.Values> entry : sortedPvs.entrySet()) {
      String prop = entry.getKey();
      if (prop.equals("Node")) continue;

      List<String> formattedValues = new ArrayList<>();
      for (McfGraph.TypedValue tv : entry.getValue().getTypedValuesList()) {
        String val = tv.getValue();
        // Simple escaping and formatting
        if (tv.getType().name().equals("TEXT")) { // Using name() to avoid import issues if possible
          val = "\"" + val.replace("\"", "\\\"") + "\"";
        } else if (tv.getType().name().equals("RESOLVED_REF")) {
          val = "dcid:" + val;
        }
        formattedValues.add(val);
      }
      Collections.sort(formattedValues);

      for (String val : formattedValues) {
        sb.append(prop).append(": ").append(val).append("\n");
      }
    }
    return sb.toString();
  }

  public static void generateSummary(
      org.apache.beam.sdk.PipelineResult result, String outputLocation) {
    org.apache.beam.sdk.metrics.MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                org.apache.beam.sdk.metrics.MetricsFilter.builder()
                    .addNameFilter(
                        org.apache.beam.sdk.metrics.MetricNameFilter.inNamespace(DifferUtils.class))
                    .build());

    List<String> expectedCounters =
        Arrays.asList(
            "previous_obs_count",
            "current_obs_count",
            "previous_schema_count",
            "current_schema_count",
            "added_obs_count",
            "deleted_obs_count",
            "modified_obs_count",
            "added_schema_count",
            "deleted_schema_count",
            "modified_schema_count");

    Map<String, Long> counterValues = new java.util.HashMap<>();
    for (String expected : expectedCounters) {
      counterValues.put(expected, 0L);
    }

    for (org.apache.beam.sdk.metrics.MetricResult<Long> counter : metrics.getCounters()) {
      counterValues.put(counter.getName().getName(), counter.getAttempted());
    }

    long obsDiffCount =
        counterValues.get("added_obs_count")
            + counterValues.get("deleted_obs_count")
            + counterValues.get("modified_obs_count");
    long schemaDiffCount =
        counterValues.get("added_schema_count")
            + counterValues.get("deleted_schema_count")
            + counterValues.get("modified_schema_count");

    counterValues.put("obs_diff_count", obsDiffCount);
    counterValues.put("schema_diff_count", schemaDiffCount);

    // Maintain consistent output ordering
    List<String> outputKeys = new ArrayList<>(expectedCounters);
    outputKeys.add("obs_diff_count");
    outputKeys.add("schema_diff_count");

    StringBuilder sb = new StringBuilder();
    sb.append("{\n");
    java.util.List<String> entries = new java.util.ArrayList<>();
    for (String key : outputKeys) {
      entries.add("  \"" + key + "\": " + counterValues.get(key));
    }
    sb.append(String.join(",\n", entries));
    sb.append("\n}\n");

    if (outputLocation != null && !outputLocation.isEmpty()) {
      try {
        org.apache.beam.sdk.io.fs.ResourceId resourceId =
            org.apache.beam.sdk.io.FileSystems.matchNewResource(
                outputLocation + "/differ-summary.json", false);
        try (java.nio.channels.WritableByteChannel outChannel =
                org.apache.beam.sdk.io.FileSystems.create(resourceId, "application/json");
            java.io.PrintWriter writer =
                new java.io.PrintWriter(java.nio.channels.Channels.newOutputStream(outChannel))) {
          writer.print(sb.toString());
        }
      } catch (Exception e) {
        LOGGER.error("Failed to write counters", e);
      }
    }
    LOGGER.info("Counters:\n{}", sb.toString());
  }
}
