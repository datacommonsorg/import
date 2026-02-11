package org.datacommons.ingestion.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.PipelineResult;
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
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PipelineUtilsTest {

  static PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

  private McfGraph createStatVarObservationGraph(
      String obsId, String statVar, String location, String date, String value) {
    McfGraph.Builder graph = McfGraph.newBuilder();
    PropertyValues.Builder pv = PropertyValues.newBuilder();
    pv.putPvs(
        "typeOf",
        Values.newBuilder()
            .addTypedValues(TypedValue.newBuilder().setValue("StatVarObservation"))
            .build());
    pv.putPvs(
        "variableMeasured",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue(statVar)).build());
    pv.putPvs(
        "observationAbout",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue(location)).build());
    pv.putPvs(
        "observationDate",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue(date)).build());
    pv.putPvs(
        "value",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue(value)).build());
    pv.putPvs(
        "dcid",
        Values.newBuilder().addTypedValues(TypedValue.newBuilder().setValue(obsId)).build());
    graph.putNodes(obsId, pv.build());
    return graph.build();
  }

  private McfStatVarObsSeries.StatVarObs createStatVarObs(String date, double value, String dcid) {
    McfStatVarObsSeries.StatVarObs.Builder svObs = McfStatVarObsSeries.StatVarObs.newBuilder();
    svObs.setDate(date);
    svObs.setNumber(value);
    svObs.setDcid(dcid);
    svObs.setPvs(PropertyValues.newBuilder().build());
    return svObs.build();
  }

  private McfStatVarObsSeries createMcfStatVarObsSeries(
      String statVar, String location, List<McfStatVarObsSeries.StatVarObs> observations) {
    McfStatVarObsSeries.Key.Builder keyBuilder = McfStatVarObsSeries.Key.newBuilder();
    keyBuilder.setObservationAbout(location);
    keyBuilder.setVariableMeasured(statVar);

    List<McfStatVarObsSeries.StatVarObs> sortedSvObs =
        observations.stream()
            .sorted(Comparator.comparing(McfStatVarObsSeries.StatVarObs::getDate))
            .collect(Collectors.toList());

    McfStatVarObsSeries.Builder seriesBuilder = McfStatVarObsSeries.newBuilder();
    seriesBuilder.setKey(keyBuilder.build());
    seriesBuilder.addAllSvObsList(sortedSvObs);
    return seriesBuilder.build();
  }

  @Test
  public void testBuildOptimizedMcfGraph() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    p.getCoderRegistry()
        .registerCoderForClass(
            McfStatVarObsSeries.Key.class, ProtoCoder.of(McfStatVarObsSeries.Key.class));

    PCollection<McfGraph> input =
        p.apply(
            Create.of(
                createStatVarObservationGraph(
                    "obs1", "count_person", "country/USA", "2020", "32.0"),
                createStatVarObservationGraph(
                    "obs2", "count_person", "country/USA", "2021", "33.0"),
                createStatVarObservationGraph(
                    "obs4", "count_person", "country/India", "2022", "36.0")));

    PCollection<McfOptimizedGraph> result = PipelineUtils.buildOptimizedMcfGraph(input);

    McfOptimizedGraph expected1 =
        McfOptimizedGraph.newBuilder()
            .setSvObsSeries(
                createMcfStatVarObsSeries(
                    "count_person",
                    "country/USA",
                    Arrays.asList(
                        createStatVarObs("2020", 32.0, "obs1"),
                        createStatVarObs("2021", 33.0, "obs2"))))
            .build();
    McfOptimizedGraph expected2 =
        McfOptimizedGraph.newBuilder()
            .setSvObsSeries(
                createMcfStatVarObsSeries(
                    "count_person",
                    "country/India",
                    List.of(createStatVarObs("2022", 36.0, "obs4"))))
            .build();

    PAssert.that(result).containsInAnyOrder(expected1, expected2);
    PipelineResult.State state = p.run().waitUntilFinish();
    Assert.assertEquals(PipelineResult.State.DONE, state);
  }

  @Test
  public void testCombineGraphNodes() {
    // Input Graph 1
    McfGraph graph1 =
        createGraph(
            Map.of(
                "node1",
                Map.of(
                    "propA", List.of("val1"),
                    "propB", List.of("valB1")),
                "node2",
                Map.of("propC", List.of("valC1"))));

    // Input Graph 2
    McfGraph graph2 =
        createGraph(
            Map.of(
                "node1",
                Map.of(
                    "propA", List.of("val1", "val2"),
                    "propD", List.of("valD1")),
                "node3",
                Map.of("propE", List.of("valE1"))));

    // Expected Combined Graph
    McfGraph expectedCombinedGraph =
        createGraph(
            Map.of(
                "node1",
                Map.of(
                    "propA", List.of("val1", "val2"),
                    "propB", List.of("valB1"),
                    "propD", List.of("valD1")),
                "node2",
                Map.of("propC", List.of("valC1")),
                "node3",
                Map.of("propE", List.of("valE1"))));

    PCollection<McfGraph> input = p.apply("CreateInput", Create.of(graph1, graph2));
    PCollection<McfGraph> output = PipelineUtils.combineGraphNodes(input);

    PCollection<McfGraph> mergedOutput =
        output.apply(
            "MergeOutputGraphs", Combine.globally(new MergeMcfGraphsCombineFn()).withoutDefaults());
    PAssert.thatSingleton(mergedOutput).isEqualTo(expectedCombinedGraph);
    PipelineResult.State state = p.run().waitUntilFinish();
    Assert.assertEquals(PipelineResult.State.DONE, state);
  }

  private McfGraph createGraph(Map<String, Map<String, List<String>>> nodeData) {
    McfGraph.Builder graph = McfGraph.newBuilder();
    for (Map.Entry<String, Map<String, List<String>>> nodeEntry : nodeData.entrySet()) {
      String nodeName = nodeEntry.getKey();
      Map<String, List<String>> props = nodeEntry.getValue();
      PropertyValues.Builder pvs = PropertyValues.newBuilder();
      for (Map.Entry<String, List<String>> propEntry : props.entrySet()) {
        String propName = propEntry.getKey();
        List<String> values = propEntry.getValue();
        McfGraph.Values.Builder valuesBuilder = McfGraph.Values.newBuilder();
        for (String value : values) {
          valuesBuilder.addTypedValues(
              TypedValue.newBuilder().setType(ValueType.TEXT).setValue(value));
        }
        pvs.putPvs(propName, valuesBuilder.build());
      }
      graph.putNodes(nodeName, pvs.build());
    }
    return graph.build();
  }

  static class MergeMcfGraphsCombineFn
      extends Combine.CombineFn<McfGraph, Map<String, PropertyValues>, McfGraph> {
    @Override
    public Map<String, PropertyValues> createAccumulator() {
      return new HashMap<>();
    }

    @Override
    public Map<String, PropertyValues> addInput(
        Map<String, PropertyValues> accumulator, McfGraph input) {
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
