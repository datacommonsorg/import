package org.datacommons.pipeline.util;

import static org.apache.beam.sdk.io.Compression.GZIP;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util functions for processing MCF graphs. */
public class GraphUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphUtils.class);

  /**
   * Parses a byte array into an McfOptimizedGraph protocol buffer.
   *
   * @param element The byte array to parse.
   * @return The parsed McfOptimizedGraph.
   */
  public static McfOptimizedGraph parseToOptimizedGraph(byte[] element) {
    try {
      McfOptimizedGraph optimized_graph = McfOptimizedGraph.parseFrom(element);
      return optimized_graph;
    } catch (InvalidProtocolBufferException e) {
      throw new IllegalArgumentException(
          "Failed to parse protocol buffer descriptor for generated code.", e);
    }
  }

  /**
   * Reads an MCF graph from TFRecord files
   *
   * @param files input files (regex supported)
   * @param p dataflow pipeline
   * @return PCollection of MCF graph proto
   */
  public static PCollection<McfGraph> readMcfGraph(ValueProvider<String> files, Pipeline p) {
    PCollection<McfOptimizedGraph> graph = readOptimizedMcfGraph(files, p);
    return graph.apply(
        ParDo.of(
            new DoFn<McfOptimizedGraph, McfGraph>() {
              @ProcessElement
              public void processElement(
                  @Element McfOptimizedGraph graph, OutputReceiver<McfGraph> receiver) {
                for (McfGraph g :
                    org.datacommons.util.GraphUtils.convertMcfStatVarObsSeriesToMcfGraph(graph)) {
                  receiver.output(g);
                }
              }
            }));
  }

  /**
   * Reads an optimized MCF graph from TFRecord files.
   *
   * @param files Input files (regex supported).
   * @param p Dataflow pipeline.
   * @return PCollection of McfOptimizedGraph proto.
   */
  public static PCollection<McfOptimizedGraph> readOptimizedMcfGraph(
      ValueProvider<String> files, Pipeline p) {
    PCollection<byte[]> nodes =
        p.apply(
            "ReadMcfGraph",
            TFRecordIO.read().from(files).withCompression(GZIP).withoutValidation());

    PCollection<McfOptimizedGraph> graph =
        nodes.apply(
            "ProcessGraph",
            ParDo.of(
                new DoFn<byte[], McfOptimizedGraph>() {
                  @ProcessElement
                  public void processElement(
                      @Element byte[] element, OutputReceiver<McfOptimizedGraph> receiver) {
                    McfOptimizedGraph g = parseToOptimizedGraph(element);
                    receiver.output(g);
                  }
                }));
    return graph;
  }

  /**
   * Reads an MCF graph from a text file.
   *
   * @param file Input file.
   * @param p Dataflow pipeline.
   * @return PCollection of McfGraph proto.
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
                    return org.datacommons.util.GraphUtils.convertToGraph(input);
                  }
                }));
    return mcf;
  }
}
