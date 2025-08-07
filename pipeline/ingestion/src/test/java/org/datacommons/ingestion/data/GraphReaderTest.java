package org.datacommons.ingestion.data;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.proto.Mcf.McfStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Mcf.McfType;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Storage.Observations;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphReaderTest {

  @Test
  public void testGraphToNodes() {
    Counter mockMcfNodesWithoutTypeCounter = Mockito.mock(Counter.class);
    McfGraph graph =
        McfGraph.newBuilder()
            .setType(McfType.INSTANCE_MCF)
            .putNodes(
                "dcid0",
                PropertyValues.newBuilder()
                    .putPvs(
                        "name",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue("Node Zero"))
                            .build())
                    .putPvs(
                        "typeOf",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("Class"))
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("Thing"))
                            .build())
                    .putPvs(
                        "geoJsonCoordinates",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue(
                                        "{   \"type\": \"Polygon\",   \"coordinates\": [     [       [9, 7],       [9, 6.5],       [9.5, 6.5],       [9.5, 7],       [9, 7]     ]   ] } "))
                            .build())
                    .build())
            .putNodes(
                "dcid1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "name",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue("Node One"))
                            .build())
                    .putPvs(
                        "typeOf",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("Property"))
                            .build())
                    .build())
            .putNodes(
                "dcid_obs", // This should be skipped as it's an observation
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("StatVarObservation"))
                            .build())
                    .build())
            .putNodes(
                "dcid2", // Node with no name or types
                PropertyValues.newBuilder().build())
            .build();

    List<Node> expectedNodes =
        Arrays.asList(
            Node.builder()
                .subjectId("dcid0")
                .value("dcid0")
                .name("Node Zero")
                .types(List.of("Class", "Thing"))
                .build(),
            Node.builder()
                .subjectId("dcid1")
                .value("dcid1")
                .name("Node One")
                .types(List.of("Property"))
                .build(),
            Node.builder().subjectId("dcid2").value("dcid2").types(List.of("Thing")).build(),
            Node.builder()
                .subjectId("kUyRupzrJkxe/HIOIctxlJX4woEGeOTtlVwqyXYnfDE=")
                .value("Node Zero")
                .build(),
            Node.builder()
                .subjectId("G8RZr2tV3+cSSDVRj8Q4KnMpxDhZyZr438T3Fvq1Zkk=")
                .bytes(
                    ByteArray.copyFrom(
                        PipelineUtils.compressString(
                            "{   \"type\": \"Polygon\",   \"coordinates\": [     [       [9, 7],       [9, 6.5],       [9.5, 6.5],       [9.5, 7],       [9, 7]     ]   ] } ")))
                .build(),
            Node.builder()
                .subjectId("J7we8EV8ssChRxBgWot6zDSbHl4xGY7I6mQosc89hFk=")
                .value("Node One")
                .build());

    List<Node> actualNodes = GraphReader.graphToNodes(graph, mockMcfNodesWithoutTypeCounter);

    // Sort both lists for consistent comparison, as map iteration order is not guaranteed.
    Comparator<Node> nodeComparator = Comparator.comparing(Node::getSubjectId);
    actualNodes.sort(nodeComparator);
    expectedNodes.sort(nodeComparator);

    assertEquals(expectedNodes.size(), actualNodes.size());
    for (int i = 0; i < expectedNodes.size(); i++) {
      Node expected = expectedNodes.get(i);
      Node actual = actualNodes.get(i);
      assertEquals(expected.getSubjectId(), actual.getSubjectId());
      assertEquals(expected.getValue(), actual.getValue());
      assertEquals(expected.getBytes(), actual.getBytes());
      assertEquals(expected.getName(), actual.getName());
      assertArrayEquals(expected.getTypes().toArray(), actual.getTypes().toArray());
    }

    Mockito.verify(mockMcfNodesWithoutTypeCounter, Mockito.times(1)).inc();
  }

  @Test
  public void testGraphToEdges() {
    McfGraph graph =
        McfGraph.newBuilder()
            .setType(McfType.INSTANCE_MCF)
            .putNodes(
                "dcid_subject",
                PropertyValues.newBuilder()
                    .putPvs(
                        "name",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue("Subject Node"))
                            .build())
                    .putPvs(
                        "typeOf",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("Class"))
                            .build())
                    .putPvs(
                        "containedInPlace",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("geoId/06"))
                            .build())
                    .putPvs(
                        "description",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue("A test description"))
                            .build())
                    .putPvs(
                        "geoJsonCoordinates",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.TEXT)
                                    .setValue(
                                        "{   \"type\": \"Polygon\",   \"coordinates\": [     [       [9, 7],       [9, 6.5],       [9.5, 6.5],       [9.5, 7],       [9, 7]     ]   ] } "))
                            .build())
                    .putPvs(
                        "provenance",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("dc/base/Test"))
                            .build())
                    .build())
            .putNodes(
                "dcid_obs", // This should be skipped as it's an observation
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        McfGraph.Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setType(ValueType.RESOLVED_REF)
                                    .setValue("StatVarObservation"))
                            .build())
                    .build())
            .build();

    List<Edge> expectedEdges =
        Arrays.asList(
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("name")
                .objectId("mccrOBZqQNkHnRh1HpDlRRCFk+0dKKdYJFwWqrIw71s=")
                .provenance("dc/base/Test")
                .build(),
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("typeOf")
                .objectId("Class")
                .provenance("dc/base/Test")
                .build(),
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("containedInPlace")
                .objectId("geoId/06")
                .provenance("dc/base/Test")
                .build(),
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("description")
                .objectId("Qa4HqNXvAF/E5uwL1wf1QtUS1qKwulUzF/F1HtAY6fk=")
                .provenance("dc/base/Test")
                .build(),
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("geoJsonCoordinates")
                .objectId("G8RZr2tV3+cSSDVRj8Q4KnMpxDhZyZr438T3Fvq1Zkk=")
                .provenance("dc/base/Test")
                .build(),
            Edge.builder()
                .subjectId("dcid_subject")
                .predicate("provenance")
                .objectId("dc/base/Test")
                .provenance("dc/base/Test")
                .build());

    List<Edge> actualEdges = GraphReader.graphToEdges(graph);

    // Sort both lists for consistent comparison
    Comparator<Edge> edgeComparator =
        Comparator.comparing(Edge::getSubjectId)
            .thenComparing(Edge::getPredicate)
            .thenComparing(Edge::getObjectId);
    actualEdges.sort(edgeComparator);
    expectedEdges.sort(edgeComparator);

    assertEquals(expectedEdges.size(), actualEdges.size());
    for (int i = 0; i < expectedEdges.size(); i++) {
      assertEquals(expectedEdges.get(i), actualEdges.get(i));
    }
  }

  @Test
  public void testGraphToObservations() {
    McfOptimizedGraph optimizedGraph =
        McfOptimizedGraph.newBuilder()
            .setSvObsSeries(
                McfStatVarObsSeries.newBuilder()
                    .setKey(
                        McfStatVarObsSeries.Key.newBuilder()
                            .setObservationAbout("geoId/testPlace")
                            .setVariableMeasured("testStatVar")
                            .setObservationPeriod("P1Y")
                            .setMeasurementMethod("dcAggregate/testMethod")
                            .setUnit("testUnit")
                            .setScalingFactor("100"))
                    .addSvObsList(
                        StatVarObs.newBuilder().setDcid("obs1").setDate("2020").setNumber(10.0))
                    .addSvObsList(
                        StatVarObs.newBuilder()
                            .setDcid("obs2")
                            .setDate("2021")
                            .setText("someText")))
            .build();

    Observations expectedObsValues =
        Observations.newBuilder().putValues("2020", "10.0").putValues("2021", "someText").build();

    Observation expectedObservation =
        Observation.builder()
            .observationAbout("geoId/testPlace")
            .variableMeasured("testStatVar")
            .measurementMethod("testMethod")
            .isDcAggregate(true)
            .observationPeriod("P1Y")
            .unit("testUnit")
            .scalingFactor("100")
            .observations(expectedObsValues)
            .build();

    Observation actualObservation = GraphReader.graphToObservations(optimizedGraph);

    assertEquals(expectedObservation, actualObservation);
  }
}
