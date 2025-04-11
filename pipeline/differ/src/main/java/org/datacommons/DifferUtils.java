package org.datacommons;

import static org.apache.beam.sdk.io.Compression.GZIP;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;

/**
 * Util functions for the differ pipeline.
 */
public class DifferUtils {
  enum Diff {
    ADDED,
    DELETED,
    MODIFIED,
    TOTAL,
    SAME;
  }

  /**
   * Converts an MCF graph to a PCollection of Key-Val pairs
   * Key: PVs excluding value
   * Val: Value, scaling factor properties
   * @param graph
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
                    try {
                      McfGraph g = c.element();
                      for (PropertyValues pvs : g.getNodesMap().values()) {
                        String[] pv = GraphUtil.GetPropValKV(pvs);
                        c.output(KV.of(pv[0], pv[1]));
                      }
                    } catch (Exception e) {
                      // return null;
                    }
                  }
                }));
    return nodes;
  }

  /**
   * Generates diffs of two versions of a dataset 
   * @param currentNodes current data
   * @param previousNodes previous data
   * @return PCollection of diffs b/w datasets
   */
  static PCollection<String> performDiff(
      PCollection<KV<String, String>> currentNodes, PCollection<KV<String, String>> previousNodes) {
    TupleTag<String> keyTag = new TupleTag<>();
    TupleTag<String> valueTag = new TupleTag<>();

    PCollection<KV<String, CoGbkResult>> joinedResult =
        KeyedPCollectionTuple.of(keyTag, currentNodes)
            .and(valueTag, previousNodes)
            .apply(CoGroupByKey.create());

    return joinedResult.apply(
        "PerformMerge",
        ParDo.of(
            new DoFn<KV<String, CoGbkResult>, String>() {
              @ProcessElement
              public void process(ProcessContext c) {
                KV<String, CoGbkResult> input = c.element();
                Diff diff;
                if (!input
                    .getValue()
                    .getOnly(keyTag, "")
                    .equals(input.getValue().getOnly(valueTag, ""))) {
                  if (input.getValue().getOnly(keyTag, "").isEmpty()) {
                    diff = Diff.DELETED;
                  } else if (input.getValue().getOnly(valueTag, "").isEmpty()) {
                    diff = Diff.ADDED;
                  } else {
                    diff = Diff.MODIFIED;
                  }
                  // Format: PVs,current_value,previous_value,diff_type
                  c.output(
                      String.format(
                          "%s,%s,%s,%s",
                          input.getKey(),
                          input.getValue().getOnly(keyTag, ""),
                          input.getValue().getOnly(valueTag, ""),
                          diff.name()));
                } else {
                  // Ignore nodes with no change
                  diff = Diff.SAME;
                }
              }
            }));
  }

  /**
   * Reads an MCF graph from TFRecord files
   * @param files input files (regex supported)
   * @param p dataflow pipeline
   * @return PCollection of MCF graph proto
   */
  public static PCollection<McfGraph> readMcfGraph(ValueProvider<String> files, Pipeline p) {
    return p.apply(
            "ReadMcfGraph", TFRecordIO.read().from(files).withCompression(GZIP).withoutValidation())
        // .apply(ParDo.of(new ByteArrayToString()));
        .apply(
            "ProcessGraph",
            ParDo.of(
                new DoFn<byte[], McfGraph>() {
                  @ProcessElement
                  public void processElement(
                      @Element byte[] element, OutputReceiver<McfGraph> receiver) {

                    List<McfGraph> graphList = GraphUtil.parseToGraph(element);
                    for (McfGraph g : graphList) {
                      receiver.output(g);
                    }
                  }
                }));
  }

  /**
   * Reads a graph from MCF textproto files
   * @param files input files (regex supported)
   * @param p dataflow pipeline
   * @return PCollection of MCF graph proto
   */
  public static PCollection<McfGraph> readMcfFile(ValueProvider<String> file, Pipeline p) {
    String delimiter = "\n\n";
    PCollection<String> nodes =
        p.apply("ReadMcfFile", TextIO.read().withDelimiter(delimiter.getBytes()).from(file));
    PCollection<McfGraph> mcf =
        nodes.apply(
            "ProcesGraph",
            MapElements.via(
                new SimpleFunction<String, McfGraph>() {
                  @Override
                  public McfGraph apply(String input) {
                    return GraphUtil.convertToGraph(input);
                  }
                }));
    return mcf;
  }
}
