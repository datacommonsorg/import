package org.datacommons.pipeline.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.extensions.protobuf.ProtoCoder;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Combine;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PipelineUtilsTest {

  PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void testBuildOptimizedMcfGraph() {

    p.getCoderRegistry()
        .registerCoderForClass(
            McfStatVarObsSeries.Key.class, ProtoCoder.of(McfStatVarObsSeries.Key.class));

    // Create two McfGraph objects representing observations for the same stat var.
    McfGraph.Builder graph1 = McfGraph.newBuilder();
    PropertyValues.Builder pv1 = PropertyValues.newBuilder();
    pv1.putPvs(
        "typeOf",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("StatVarObservation"))
            .build());
    pv1.putPvs(
        "variableMeasured",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("count_person"))
            .build());
    pv1.putPvs(
        "observationAbout",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("country/USA"))
            .build());
    pv1.putPvs(
        "observationDate",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("2020")).build());
    pv1.putPvs(
        "value",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("32.0")).build());
    pv1.putPvs(
        "dcid",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("obs1")).build());
    graph1.putNodes("obs1", pv1.build());

    McfGraph.Builder graph2 = McfGraph.newBuilder();
    PropertyValues.Builder pv2 = PropertyValues.newBuilder();
    pv2.putPvs(
        "typeOf",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("StatVarObservation"))
            .build());
    pv2.putPvs(
        "variableMeasured",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("count_person"))
            .build());
    pv2.putPvs(
        "observationAbout",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("country/USA"))
            .build());
    pv2.putPvs(
        "observationDate",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("2021")).build());
    pv2.putPvs(
        "value",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("33.0")).build());
    pv2.putPvs(
        "dcid",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("obs2")).build());
    graph2.putNodes("obs2", pv2.build());

    PropertyValues.Builder pv3 = PropertyValues.newBuilder();
    pv3.putPvs(
        "typeOf",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("StatVarObservation"))
            .build());
    pv3.putPvs(
        "variableMeasured",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("count_person"))
            .build());
    pv3.putPvs(
        "observationAbout",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("country/USA"))
            .build());
    pv3.putPvs(
        "observationDate",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("2022")).build());
    pv3.putPvs(
        "value",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("34.0")).build());
    pv3.putPvs(
        "dcid",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("obs3")).build());
    graph2.putNodes("obs3", pv3.build());

    PropertyValues.Builder pv4 = PropertyValues.newBuilder();
    pv4.putPvs(
        "typeOf",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("StatVarObservation"))
            .build());
    pv4.putPvs(
        "variableMeasured",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("count_person"))
            .build());
    pv4.putPvs(
        "observationAbout",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("country/India"))
            .build());
    pv4.putPvs(
        "observationDate",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("2022")).build());
    pv4.putPvs(
        "value",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("36.0")).build());
    pv4.putPvs(
        "dcid",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue("obs4")).build());
    graph2.putNodes("obs4", pv4.build());
    List<McfGraph> graphs = Arrays.asList(graph1.build(), graph2.build());
    PCollection<McfGraph> input = p.apply(Create.of(graphs));

    PCollection<McfOptimizedGraph> result = PipelineUtils.buildOptimizedMcfGraph(input);

    // Expected output
    McfStatVarObsSeries.Key.Builder keyBuilder = McfStatVarObsSeries.Key.newBuilder();
    keyBuilder.setObservationAbout("country/USA");
    keyBuilder.setVariableMeasured("count_person");

    McfStatVarObsSeries.Key.Builder keyBuilder2 = McfStatVarObsSeries.Key.newBuilder();
    keyBuilder2.setObservationAbout("country/India");
    keyBuilder2.setVariableMeasured("count_person");

    McfStatVarObsSeries.StatVarObs.Builder svObs1 = McfStatVarObsSeries.StatVarObs.newBuilder();
    svObs1.setDate("2020");
    svObs1.setNumber(32.0);
    svObs1.setDcid("obs1");
    svObs1.setPvs(PropertyValues.newBuilder().build());

    McfStatVarObsSeries.StatVarObs.Builder svObs2 = McfStatVarObsSeries.StatVarObs.newBuilder();
    svObs2.setDate("2021");
    svObs2.setNumber(33.0);
    svObs2.setDcid("obs2");
    svObs2.setPvs(PropertyValues.newBuilder().build());

    McfStatVarObsSeries.StatVarObs.Builder svObs3 = McfStatVarObsSeries.StatVarObs.newBuilder();
    svObs3.setDate("2022");
    svObs3.setNumber(34.0);
    svObs3.setDcid("obs3");
    svObs3.setPvs(PropertyValues.newBuilder().build());

    McfStatVarObsSeries.StatVarObs.Builder svObs4 = McfStatVarObsSeries.StatVarObs.newBuilder();
    svObs4.setDate("2022");
    svObs4.setNumber(36.0);
    svObs4.setDcid("obs4");
    svObs4.setPvs(PropertyValues.newBuilder().build());

    List<McfStatVarObsSeries.StatVarObs> sortedSvObs =
        Arrays.asList(svObs1.build(), svObs2.build(), svObs3.build()).stream()
            .sorted(Comparator.comparing(McfStatVarObsSeries.StatVarObs::getDate))
            .collect(Collectors.toList());

    McfStatVarObsSeries.Builder seriesBuilder = McfStatVarObsSeries.newBuilder();
    seriesBuilder.setKey(keyBuilder.build());
    seriesBuilder.addAllSvObsList(sortedSvObs);

    McfStatVarObsSeries.Builder seriesBuilder2 = McfStatVarObsSeries.newBuilder();
    seriesBuilder2.setKey(keyBuilder2.build());
    seriesBuilder2.addAllSvObsList(List.of(svObs4.build()));

    McfOptimizedGraph.Builder expectedGraph = McfOptimizedGraph.newBuilder();
    expectedGraph.setSvObsSeries(seriesBuilder.build());

    McfOptimizedGraph.Builder expectedGraph2 = McfOptimizedGraph.newBuilder();
    expectedGraph2.setSvObsSeries(seriesBuilder2.build());

    PAssert.that(result).containsInAnyOrder(expectedGraph.build(), expectedGraph2.build());
    p.run().waitUntilFinish();
  }

  @Test
  public void testCombineGraphNodes() {
    // Input Graph 1
    McfGraph graph1 =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propA",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("val1"))
                            .build())
                    .putPvs(
                        "propB",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valB1"))
                            .build())
                    .build())
            .putNodes(
                "node2",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propC",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valC1"))
                            .build())
                    .build())
            .build();

    // Input Graph 2
    McfGraph graph2 =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propA",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("val1"))
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("val2"))
                            .build())
                    .putPvs(
                        "propD",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valD1"))
                            .build())
                    .build())
            .putNodes(
                "node3",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propE",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valE1"))
                            .build())
                    .build())
            .build();

    // Expected Combined Graph
    McfGraph expectedCombinedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propA",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("val1"))
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("val2"))
                            .build())
                    .putPvs(
                        "propB",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valB1"))
                            .build())
                    .putPvs(
                        "propD",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valD1"))
                            .build())
                    .build())
            .putNodes(
                "node2",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propC",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valC1"))
                            .build())
                    .build())
            .putNodes(
                "node3",
                PropertyValues.newBuilder()
                    .putPvs(
                        "propE",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setType(ValueType.TEXT).setValue("valE1"))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> input = p.apply("CreateInput", Create.of(graph1, graph2));
    PCollection<McfGraph> output = PipelineUtils.combineGraphNodes(input);

    // The combineGraphNodes method returns a PCollection where each element is an McfGraph
    // containing a single combined node. To compare against a single expected graph,
    // we need to merge these single-node graphs back into one.
    PCollection<McfGraph> mergedOutput =
        output.apply(
            "MergeOutputGraphs", Combine.globally(new MergeMcfGraphsCombineFn()).withoutDefaults());

    PAssert.thatSingleton(mergedOutput).isEqualTo(expectedCombinedGraph);

    p.run().waitUntilFinish();
  }

  // A CombineFn to merge multiple McfGraph objects (each containing a single node) into a single
  // McfGraph containing all nodes.
  static class MergeMcfGraphsCombineFn
      extends Combine.CombineFn<McfGraph, Map<String, PropertyValues>, McfGraph> {
    @Override
    public Map<String, PropertyValues> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<String, PropertyValues> addInput(
        Map<String, PropertyValues> accumulator, McfGraph input) {
      // Each input McfGraph is expected to contain exactly one node.
      accumulator.putAll(input.getNodesMap());
      return accumulator;
    }

    @Override
    public Map<String, PropertyValues> mergeAccumulators(
        Iterable<Map<String, PropertyValues>> accumulators) {
      Map<String, PropertyValues> merged = new HashMap<>();
      for (Map<String, PropertyValues> acc : accumulators) {
        merged.putAll(acc);
      }
      return merged;
    }

    @Override
    public McfGraph extractOutput(Map<String, PropertyValues> accumulator) {
      return McfGraph.newBuilder().putAllNodes(accumulator).build();
    }
  }
}
