package org.datacommons.pipeline.differ;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.datacommons.util.GraphUtils.Property;

/** Util functions for the differ pipeline. */
public class DifferUtils {
  enum Diff {
    ADDED,
    DELETED,
    MODIFIED,
    UNMODIFIED;
  }

  public static final TupleTag<KV<String, String>> OBSERVATION_NODES_TAG =
      new TupleTag<KV<String, String>>() {};
  public static final TupleTag<KV<String, String>> SCHEMA_NODES_TAG =
      new TupleTag<KV<String, String>>() {};

  public static final String[] GRUOPBY_PROPERTIES = {
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
   * @return PCollectionTuple of observation and schema nodes
   */
  static PCollectionTuple processGraph(PCollection<McfGraph> graph) {
    return graph.apply(
        "ProcessGraph",
        ParDo.of(
                new DoFn<McfGraph, KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    McfGraph g = c.element();
                    for (Map.Entry<String, PropertyValues> entry : g.getNodesMap().entrySet()) {
                      PropertyValues pvs = entry.getValue();
                      Map<String, McfGraph.Values> pv = pvs.getPvsMap();
                      if (org.datacommons.util.GraphUtils.isObservation(pvs)) {
                        String key =
                            Arrays.asList(GRUOPBY_PROPERTIES).stream()
                                .map(prop -> GraphUtils.getPropertyValue(pv, prop))
                                .collect(Collectors.joining(","));
                        String value = GraphUtils.getPropertyValue(pv, Property.value.name());
                        c.output(KV.of(key, value));
                      } else {
                        List<String> keys = new ArrayList<>(pv.keySet());
                        Collections.sort(keys);
                        String value =
                            keys.stream()
                                .map(key -> GraphUtils.getPropertyValue(pv, key))
                                .collect(Collectors.joining(","));

                        c.output(SCHEMA_NODES_TAG, KV.of(entry.getKey(), value));
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
   * @return PCollection of diffs b/w datasets
   */
  static PCollection<String> performDiff(
      PCollection<KV<String, String>> currentNodes, PCollection<KV<String, String>> previousNodes) {
    TupleTag<String> currentTag = new TupleTag<>();
    TupleTag<String> previousTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> joinedResult =
        KeyedPCollectionTuple.of(currentTag, currentNodes)
            .and(previousTag, previousNodes)
            .apply(CoGroupByKey.create());

    return joinedResult.apply(
        "PerformDiff",
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, String>() {
              @ProcessElement
              public void process(ProcessContext c) {
                KV<String, CoGbkResult> input = c.element();
                Diff diff;
                if (!input
                    .getValue()
                    .getOnly(currentTag, "")
                    .equals(input.getValue().getOnly(previousTag, ""))) {
                  if (input.getValue().getOnly(currentTag, "").isEmpty()) {
                    diff = Diff.DELETED;
                  } else if (input.getValue().getOnly(previousTag, "").isEmpty()) {
                    diff = Diff.ADDED;
                  } else {
                    diff = Diff.MODIFIED;
                  }
                  // Format: PVs,current_value,previous_value,diff_type
                  c.output(
                      String.format(
                          "%s,%s,%s,%s",
                          input.getKey(),
                          input.getValue().getOnly(currentTag, ""),
                          input.getValue().getOnly(previousTag, ""),
                          diff.name()));
                } else {
                  // Ignore nodes with no change
                  diff = Diff.UNMODIFIED;
                }
              }
            }));
  }
}
