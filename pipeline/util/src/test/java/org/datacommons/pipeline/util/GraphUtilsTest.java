package org.datacommons.pipeline.util;

import java.util.HashMap;
import java.util.Map;
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
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GraphUtilsTest {

  PipelineOptions options = PipelineOptionsFactory.create();

  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

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
    PCollection<McfGraph> output =
        org.datacommons.pipeline.util.GraphUtils.combineGraphNodes(input);

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
