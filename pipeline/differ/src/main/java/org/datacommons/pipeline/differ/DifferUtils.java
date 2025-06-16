package org.datacommons.pipeline.differ;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.GraphUtils;

/** Util functions for the differ pipeline. */
public class DifferUtils {
  enum Diff {
    ADDED,
    DELETED,
    MODIFIED,
    UNMODIFIED;
  }

  /**
   * Converts an MCF graph (SVObs only) to a PCollection of Key-Val pairs where key contains PVs
   * excluding value.
   *
   * @param graph input graph to process
   * @return PCollection of Key-Val pairs
   */
  static PCollection<KV<String, String>> processGraph(PCollection<McfGraph> graph) {
    PCollection<KV<String, String>> nodes =
        graph.apply(
            "ProcessGraph",
            ParDo.of(
                new DoFn<McfGraph, KV<String, String>>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    McfGraph g = c.element();
                    for (String[] pv : GraphUtils.getSeriesValues(g)) {
                      c.output(KV.of(pv[0], pv[1]));
                    }
                  }
                }));
    return nodes;
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
