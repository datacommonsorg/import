package org.datacommons.pipeline.util;

import static org.apache.beam.sdk.io.Compression.GZIP;

import com.google.protobuf.InvalidProtocolBufferException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
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
  public static PCollection<McfGraph> readMcfGraph(String files, Pipeline p) {
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
  public static PCollection<McfOptimizedGraph> readOptimizedMcfGraph(String files, Pipeline p) {
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
  public static PCollection<McfGraph> readMcfFile(String file, Pipeline p) {
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

  /**
   * Builds an optimized MCF graph from a PCollection of McfGraph protos.
   *
   * @param graph PCollection of McfGraph protos.()
   * @return PCollection of McfOptimizedGraph protos.
   */
  public static PCollection<McfOptimizedGraph> buildOptimizedMcfGraph(PCollection<McfGraph> graph) {
    PCollection<McfOptimizedGraph> svObs =
        graph
            .apply(
                ParDo.of(
                    new DoFn<
                        McfGraph, KV<McfStatVarObsSeries.Key, McfStatVarObsSeries.StatVarObs>>() {
                      @ProcessElement
                      public void processElement(
                          @Element McfGraph graph,
                          OutputReceiver<
                                  KV<McfStatVarObsSeries.Key, McfStatVarObsSeries.StatVarObs>>
                              receiver) {
                        for (PropertyValues pv : graph.getNodesMap().values()) {
                          if (org.datacommons.util.GraphUtils.isObservation(pv)) {
                            McfStatVarObsSeries svoSeries =
                                org.datacommons.util.GraphUtils
                                    .convertMcfGraphToMcfStatVarObsSeries(pv);
                            receiver.output(KV.of(svoSeries.getKey(), svoSeries.getSvObsList(0)));
                          }
                        }
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                ParDo.of(
                    new DoFn<
                        KV<McfStatVarObsSeries.Key, Iterable<McfStatVarObsSeries.StatVarObs>>,
                        McfOptimizedGraph>() {
                      @ProcessElement
                      public void processElement(
                          @Element
                              KV<McfStatVarObsSeries.Key, Iterable<McfStatVarObsSeries.StatVarObs>>
                                  element,
                          OutputReceiver<McfOptimizedGraph> receiver) {
                        McfStatVarObsSeries.Builder svObsSeries = McfStatVarObsSeries.newBuilder();

                        svObsSeries.setKey(element.getKey());
                        for (McfStatVarObsSeries.StatVarObs svo : element.getValue()) {
                          svObsSeries.addSvObsList(svo);
                        }
                        McfOptimizedGraph.Builder res =
                            McfOptimizedGraph.newBuilder().setSvObsSeries(svObsSeries);
                        receiver.output(res.build());
                        LOGGER.info(res.build().toString());
                      }
                    }));

    return svObs;
  }

  /**
   * Combines nodes from multiple McfGraph protos into a single PCollection of McfGraph protos,
   * where each output McfGraph contains a single combined node.
   *
   * @param graph A PCollection of McfGraph protos to combine.
   * @return A PCollection of McfGraph protos, each containing a single combined node.
   */
  public static PCollection<McfGraph> combineGraphNodes(PCollection<McfGraph> graph) {
    PCollection<KV<String, PropertyValues>> graphNodes =
        graph.apply(
            "MapGraphToNodes",
            ParDo.of(
                new DoFn<McfGraph, KV<String, PropertyValues>>() {
                  @ProcessElement
                  public void processElement(
                      @Element McfGraph graph,
                      OutputReceiver<KV<String, PropertyValues>> receiver) {
                    Map<String, PropertyValues> nodes = graph.getNodesMap();
                    for (Map.Entry<String, PropertyValues> node : nodes.entrySet()) {
                      receiver.output(KV.of(node.getKey(), node.getValue()));
                    }
                  }
                }));

    PCollection<KV<String, PropertyValues>> combined =
        graphNodes.apply(
            "CombineGraphNodes",
            Combine.perKey(
                new Combine.CombineFn<PropertyValues, List<PropertyValues>, PropertyValues>() {
                  @Override
                  public List<PropertyValues> createAccumulator() {
                    return new ArrayList<>();
                  }

                  @Override
                  public List<PropertyValues> addInput(
                      List<PropertyValues> accumulator, PropertyValues input) {
                    accumulator.add(input);
                    return accumulator;
                  }

                  @Override
                  public List<PropertyValues> mergeAccumulators(
                      Iterable<List<PropertyValues>> accumulators) {
                    List<PropertyValues> merged = new ArrayList<>();
                    for (List<PropertyValues> acc : accumulators) {
                      merged.addAll(acc);
                    }
                    return merged;
                  }

                  @Override
                  public PropertyValues extractOutput(List<PropertyValues> accumulator) {
                    PropertyValues.Builder combined = PropertyValues.newBuilder();
                    Map<String, Values> props = new HashMap<>();
                    for (PropertyValues pv : accumulator) {
                      for (Map.Entry<String, Values> entry : pv.getPvsMap().entrySet()) {
                        String property = entry.getKey();
                        Values values = entry.getValue();
                        if (!props.containsKey(property)) {
                          props.put(property, values);
                        } else {
                          Values v = props.get(property);
                          Values.Builder val = Values.newBuilder();
                          val.addAllTypedValues(v.getTypedValuesList());
                          val.addAllTypedValues(values.getTypedValuesList());
                          // Add logic to remove duplicates from val
                          Set<String> seenValues = new HashSet<>();
                          Values.Builder uniqueVal = Values.newBuilder();
                          for (org.datacommons.proto.Mcf.McfGraph.TypedValue tv :
                              val.getTypedValuesList()) {
                            if (!seenValues.contains(tv.getValue())) {
                              uniqueVal.addTypedValues(tv);
                              seenValues.add(tv.getValue());
                            }
                          }
                          props.put(property, uniqueVal.build());
                        }
                      }
                    }
                    combined.putAllPvs(props);
                    return combined.build();
                  }
                }));

    PCollection<McfGraph> combinedGraph =
        combined.apply(
            "MapCombinedNodesToGraph",
            ParDo.of(
                new DoFn<KV<String, PropertyValues>, McfGraph>() {
                  @ProcessElement
                  public void processElement(
                      @Element KV<String, PropertyValues> element,
                      OutputReceiver<McfGraph> receiver) {
                    McfGraph.Builder graphBuilder = McfGraph.newBuilder();
                    graphBuilder.putNodes(element.getKey(), element.getValue());
                    receiver.output(graphBuilder.build());
                    System.out.println(graphBuilder.build().toString());
                  }
                }));
    return combinedGraph;
  }
}
