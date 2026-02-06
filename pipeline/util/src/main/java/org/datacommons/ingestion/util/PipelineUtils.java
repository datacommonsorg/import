package org.datacommons.ingestion.util;

import static org.apache.beam.sdk.io.Compression.GZIP;

import com.google.common.collect.ImmutableSet;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import java.util.zip.GZIPOutputStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.datacommons.ingestion.data.Encode;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.util.GraphUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Util functions for processing MCF graphs. */
public class PipelineUtils {
  // Default type for MCF nodes.
  public static final String TYPE_THING = "Thing";

  // Length of prefix of object value to use for key.
  public static final int OBJECT_VALUE_PREFIX = 16;

  private static final Logger LOGGER = LoggerFactory.getLogger(PipelineUtils.class);

  // Predicates for which the object value should be stored as bytes.
  private static final Set<String> STORE_VALUE_AS_BYTES_PREDICATES =
      ImmutableSet.of(
          "geoJsonCoordinates",
          "geoJsonCoordinatesDP1",
          "geoJsonCoordinatesDP2",
          "geoJsonCoordinatesDP3",
          "kmlCoordinates");

  public static final TupleTag<McfGraph> OBSERVATION_NODES_TAG = new TupleTag<McfGraph>() {};
  public static final TupleTag<McfGraph> SCHEMA_NODES_TAG = new TupleTag<McfGraph>() {};

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
                for (McfGraph g : GraphUtils.convertMcfStatVarObsSeriesToMcfGraph(graph)) {
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
   * Reads MCF graphs from text files.
   *
   * @param files Input files.
   * @param p Dataflow pipeline.
   * @return PCollection of McfGraph proto.
   */
  public static PCollection<McfGraph> readMcfFiles(String files, Pipeline p) {
    String delimiter = "\n\n";
    PCollection<String> nodes =
        p.apply(
            "ReadMcfFiles",
            TextIO.read()
                .withDelimiter(delimiter.getBytes())
                .from(files)
                .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW));

    PCollection<McfGraph> mcf =
        nodes.apply(
            "MapToGraph",
            MapElements.via(
                new SimpleFunction<String, McfGraph>() {
                  @Override
                  public McfGraph apply(String input) {
                    return GraphUtils.convertToGraph(input);
                  }
                }));
    return mcf;
  }

  public static PCollectionTuple splitGraph(PCollection<McfGraph> graph) {
    return graph.apply(
        "SplitGraph",
        ParDo.of(
                new DoFn<McfGraph, McfGraph>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    McfGraph g = c.element();
                    if (!g.getNodesMap().isEmpty()) {
                      // Access the single element in the map.
                      PropertyValues pvs = g.getNodesMap().entrySet().iterator().next().getValue();
                      if (GraphUtils.isObservation(pvs)) {
                        c.output(OBSERVATION_NODES_TAG, g);
                      } else {
                        c.output(SCHEMA_NODES_TAG, g);
                      }
                    }
                  }
                })
            .withOutputTags(OBSERVATION_NODES_TAG, TupleTagList.of(SCHEMA_NODES_TAG)));
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
                "ExtractObs",
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
                          if (GraphUtils.isObservation(pv)) {
                            McfStatVarObsSeries svoSeries =
                                GraphUtils.convertMcfGraphToMcfStatVarObsSeries(pv);
                            receiver.output(KV.of(svoSeries.getKey(), svoSeries.getSvObsList(0)));
                          }
                        }
                      }
                    }))
            .apply(GroupByKey.create())
            .apply(
                "BuildOptimizedGraph",
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

                        Iterable<McfStatVarObsSeries.StatVarObs> observations =
                            StreamSupport.stream(element.getValue().spliterator(), false)
                                .sorted(
                                    Comparator.comparing(McfStatVarObsSeries.StatVarObs::getDate))
                                .collect(Collectors.toList());
                        for (McfStatVarObsSeries.StatVarObs svo : observations) {
                          svObsSeries.addSvObsList(svo);
                        }
                        McfOptimizedGraph.Builder res =
                            McfOptimizedGraph.newBuilder().setSvObsSeries(svObsSeries);
                        receiver.output(res.build());
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
                  }
                }));
    return combinedGraph;
  }

  /**
   * Returns whether the value for the given predicate should be stored as bytes, false otherwise.
   *
   * @param predicate The predicate.
   * @return True if the value should be stored as bytes, false otherwise.
   */
  public static boolean storeValueAsBytes(String predicate) {
    return STORE_VALUE_AS_BYTES_PREDICATES.contains(predicate);
  }

  /**
   * Generates a key for an object value.
   *
   * @param input The input object value.
   * @return The corresponding key.
   */
  public static String generateObjectValueKey(String input) {
    String hash = Encode.generateSha256(input);
    String prefix = input.substring(0, Math.min(input.length(), OBJECT_VALUE_PREFIX));
    return prefix + ":" + hash;
  }

  /**
   * Compresses input with GZIP.
   *
   * @param data The input to compress.
   * @return The compressed string.
   */
  public static byte[] compressString(String data) {
    try {
      var out = new ByteArrayOutputStream();
      try (GZIPOutputStream gout = new GZIPOutputStream(out)) {
        // Default charset can differ across platforms. Using UTF-8 here.
        gout.write(data.getBytes(StandardCharsets.UTF_8));
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new IllegalArgumentException("Error serializing string: " + data, e);
    }
  }
}
