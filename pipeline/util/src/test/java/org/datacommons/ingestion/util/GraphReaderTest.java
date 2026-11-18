package org.datacommons.ingestion.util;

import static java.util.stream.Collectors.toMap;
import static org.datacommons.ingestion.util.GraphTestUtils.PROVENANCE;
import static org.datacommons.ingestion.util.GraphTestUtils.bytesLeaf;
import static org.datacommons.ingestion.util.GraphTestUtils.edge;
import static org.datacommons.ingestion.util.GraphTestUtils.leaf;
import static org.datacommons.ingestion.util.GraphTestUtils.ref;
import static org.datacommons.ingestion.util.GraphTestUtils.subjectNode;
import static org.datacommons.ingestion.util.GraphTestUtils.text;
import static org.datacommons.ingestion.util.GraphTestUtils.values;
import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.ExpandedNode;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.proto.Mcf.McfStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

public class GraphReaderTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private Counter noTypeCounter;

  @Before
  public void setUp() {
    noTypeCounter = Mockito.mock(Counter.class);
  }

  @Test
  public void testToTimeSeries() {
    McfStatVarObsSeries.Key key =
        McfStatVarObsSeries.Key.newBuilder()
            .setObservationAbout("geoId/testPlace")
            .setVariableMeasured("testStatVar")
            .setObservationPeriod("P1Y")
            .setMeasurementMethod("dcAggregate/testMethod")
            .setUnit("testUnit")
            .setScalingFactor("100")
            .setProvenanceUrl("http://example.com")
            .build();

    TimeSeries expected1 =
        TimeSeries.builder()
            .entity1("geoId/testPlace")
            .variableMeasured("testStatVar")
            .measurementMethod("testMethod")
            .isDcAggregate(true)
            .importName("test_import")
            .observationPeriod("P1Y")
            .unit("testUnit")
            .scalingFactor("100")
            .provenanceUrl("http://example.com")
            .isBaseDc(true)
            .build();

    TimeSeries expected2 =
        TimeSeries.builder()
            .entity1("geoId/testPlace")
            .variableMeasured("testStatVar")
            .measurementMethod("testMethod")
            .isDcAggregate(true)
            .importName("test_import")
            .observationPeriod("P1Y")
            .unit("testUnit")
            .scalingFactor("100")
            .provenanceUrl("http://example.com")
            .isBaseDc(false)
            .build();

    TimeSeries actual1 = GraphReader.toTimeSeries(key, "test_import", true);
    assertEquals(expected1, actual1);

    TimeSeries actual2 = GraphReader.toTimeSeries(key, "test_import", false);
    assertEquals(expected2, actual2);
  }

  @Test
  public void testToTimeSeriesKey() {
    McfStatVarObsSeries.Key key =
        McfStatVarObsSeries.Key.newBuilder()
            .setObservationAbout("geoId/testPlace")
            .setVariableMeasured("testStatVar")
            .setObservationPeriod("P1Y")
            .setMeasurementMethod("dcAggregate/testMethod")
            .setUnit("testUnit")
            .setScalingFactor("100")
            .setProvenanceUrl("http://example.com")
            .build();

    String facetId =
        TimeSeries.calculateFacetId("test_import", "testMethod", "P1Y", "100", "testUnit", true);
    TimeSeriesKey expectedKey =
        new TimeSeriesKey(
            "testStatVar", "geoId/testPlace", "", "P1Y", "testMethod", "testUnit", "100", facetId);

    TimeSeriesKey actualKey = GraphReader.toTimeSeriesKey(key, "test_import");
    assertEquals(expectedKey, actualKey);
  }

  @Test
  public void testToObservation() {
    String facetId =
        TimeSeries.calculateFacetId("test_import", "testMethod", "P1Y", "100", "testUnit", true);
    TimeSeriesKey seriesKey =
        new TimeSeriesKey(
            "testStatVar", "geoId/testPlace", "", "P1Y", "testMethod", "testUnit", "100", facetId);

    StatVarObs obs1 =
        StatVarObs.newBuilder().setDcid("obs1").setDate("2020").setNumber(10.0).build();
    StatVarObs obs2 =
        StatVarObs.newBuilder().setDcid("obs2").setDate("2021").setText("someText").build();

    Observation expected1 =
        Observation.builder().seriesKey(seriesKey).date("2020").value("10.0").build();

    Observation expected2 =
        Observation.builder().seriesKey(seriesKey).date("2021").value("someText").build();

    Observation actual1 = GraphReader.toObservation(seriesKey, obs1);
    assertEquals(expected1, actual1);

    Observation actual2 = GraphReader.toObservation(seriesKey, obs2);
    assertEquals(expected2, actual2);
  }

  @Test
  public void testExtractTimeSeries_MultiEntity() {
    PropertyValues pv =
        PropertyValues.newBuilder()
            .putPvs(
                "typeOf",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("StatVarObservation"))
                    .build())
            .putPvs(
                "variableMeasured",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("dcs:FinancialTrade"))
                    .build())
            .putPvs(
                "observationDate",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.TEXT).setValue("2024"))
                    .build())
            .putPvs(
                "value",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.NUMBER).setValue("100"))
                    .build())
            .putPvs(
                "dcs:sourceCountry",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("country/FRA"))
                    .build())
            .putPvs(
                "dcs:destinationCountry",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("country/USA"))
                    .build())
            .putPvs(
                "dcs:observationProperties",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("destinationCountry"))
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("sourceCountry"))
                    .build())
            .putPvs(
                "provenance",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("provenance/TestImport"))
                    .build())
            .build();

    TimeSeries expected =
        TimeSeries.builder()
            .entity1("country/USA")
            .extraEntities(List.of("country/FRA"))
            .variableMeasured("FinancialTrade")
            .importName("test_import")
            .isBaseDc(true)
            .build();

    TimeSeries actual = GraphReader.extractTimeSeries("nodeId", pv, "test_import", true);
    assertEquals(expected, actual);
  }

  @Test
  public void testExtractTimeSeriesKey_MultiEntity() {
    PropertyValues pv =
        PropertyValues.newBuilder()
            .putPvs(
                "typeOf",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("StatVarObservation"))
                    .build())
            .putPvs(
                "variableMeasured",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("dcs:FinancialTrade"))
                    .build())
            .putPvs(
                "observationDate",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.TEXT).setValue("2024"))
                    .build())
            .putPvs(
                "value",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.NUMBER).setValue("100"))
                    .build())
            .putPvs(
                "dcs:sourceCountry",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("country/FRA"))
                    .build())
            .putPvs(
                "dcs:destinationCountry",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("country/USA"))
                    .build())
            .putPvs(
                "dcs:observationProperties",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("destinationCountry"))
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("sourceCountry"))
                    .build())
            .build();

    String facetId = TimeSeries.calculateFacetId("test_import", "", "", "", "", false);
    TimeSeriesKey expected =
        new TimeSeriesKey("FinancialTrade", "country/USA", "country/FRA", "", "", "", "", facetId);

    TimeSeriesKey actual = GraphReader.extractTimeSeriesKey("nodeId", pv, "test_import");
    assertEquals(expected, actual);
  }

  @Test(expected = RuntimeException.class)
  public void testExtractTimeSeries_MissingEntity() {
    PropertyValues pv =
        PropertyValues.newBuilder()
            .putPvs(
                "typeOf",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("StatVarObservation"))
                    .build())
            .putPvs(
                "variableMeasured",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("dcs:FinancialTrade"))
                    .build())
            .putPvs(
                "observationDate",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.TEXT).setValue("2024"))
                    .build())
            .putPvs(
                "value",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.NUMBER).setValue("100"))
                    .build())
            .putPvs(
                "dcs:destinationCountry",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("country/USA"))
                    .build())
            .putPvs(
                "dcs:observationProperties",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("destinationCountry"))
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("sourceCountry"))
                    .build())
            .build();

    // This should throw RuntimeException because sourceCountry is missing
    GraphReader.extractTimeSeries("nodeId", pv, "test_import", true);
  }

  @Test
  public void testExtractTimeSeriesFn_BundleDeduplication() throws Exception {
    GraphReader.ExtractTimeSeriesFn fn = new GraphReader.ExtractTimeSeriesFn("test_import", true);
    fn.startBundle();

    PropertyValues pv =
        PropertyValues.newBuilder()
            .putPvs(
                "typeOf",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("StatVarObservation"))
                    .build())
            .putPvs(
                "variableMeasured",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("Count_Person"))
                    .build())
            .putPvs(
                "observationAbout",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder()
                            .setType(ValueType.RESOLVED_REF)
                            .setValue("geoId/06"))
                    .build())
            .putPvs(
                "observationDate",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.TEXT).setValue("2020"))
                    .build())
            .putPvs(
                "value",
                McfGraph.Values.newBuilder()
                    .addTypedValues(
                        TypedValue.newBuilder().setType(ValueType.NUMBER).setValue("100"))
                    .build())
            .build();

    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes("obs1", pv)
            .putNodes("obs2", pv) // Duplicate observation of same series in same bundle
            .build();

    DoFn.ProcessContext mockContext = Mockito.mock(DoFn.ProcessContext.class);
    Mockito.when(mockContext.element()).thenReturn(graph);

    fn.processElement(mockContext);

    // Should only output 1 TimeSeries since obs1 and obs2 belong to the same TimeSeries
    Mockito.verify(mockContext, Mockito.times(1)).output(Mockito.any(TimeSeries.class));
  }

  private Map<Node, Set<Edge>> extract(McfGraph graph) {
    return GraphReader.extractExpandedNodesFromGraph(graph, PROVENANCE, noTypeCounter).stream()
        .collect(toMap(ExpandedNode::getNode, n -> ImmutableSet.copyOf(n.getEdges())));
  }

  private static long getCounterValue(PipelineResult result, String counterName) {
    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.named(GraphReaderTest.class, counterName))
                    .build());
    List<MetricResult<Long>> counters = Lists.newArrayList(metrics.getCounters());
    assertEquals(1, counters.size());
    return counters.get(0).getCommitted();
  }

  @Test
  public void extractExpandedNodesFromGraph_skipsObservations() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "obs1",
                PropertyValues.newBuilder()
                    .putPvs("typeOf", values(ref("StatVarObservation")))
                    .build())
            .build();

    assertEquals(Map.of(), extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_textValue_createsLeafNodeAndEdge() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "city1",
                PropertyValues.newBuilder()
                    .putPvs("typeOf", values(ref("City")))
                    .putPvs("name", values(text("Springfield")))
                    .build())
            .build();

    assertEquals(
        Map.of(
            subjectNode("city1", "Springfield", "City"),
            Set.of(
                edge("city1", "typeOf", "City"),
                edge("city1", "name", leaf("Springfield").getSubjectId())),
            leaf("Springfield"),
            Set.of()),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_bytesPredicate_createsCompressedLeafNode() {
    String geoJson = "{\"type\": \"Polygon\", \"coordinates\": [[[9, 7], [9, 6.5], [9, 7]]]}";
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "place1",
                PropertyValues.newBuilder()
                    .putPvs("typeOf", values(ref("Place")))
                    .putPvs("geoJsonCoordinates", values(text(geoJson)))
                    .build())
            .build();

    assertEquals(
        Map.of(
            subjectNode("place1", "place1", "Place"),
            Set.of(
                edge("place1", "typeOf", "Place"),
                edge("place1", "geoJsonCoordinates", bytesLeaf(geoJson).getSubjectId())),
            bytesLeaf(geoJson),
            Set.of()),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_referenceValue_createsEdgeWithoutLeafNode() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "city1",
                PropertyValues.newBuilder()
                    .putPvs("typeOf", values(ref("City")))
                    .putPvs("containedInPlace", values(ref("dcid:geoId/06")))
                    .build())
            .build();

    assertEquals(
        Map.of(
            subjectNode("city1", "city1", "City"),
            Set.of(edge("city1", "typeOf", "City"), edge("city1", "containedInPlace", "geoId/06"))),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_excludesDcidProperty() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "l:nodeKey",
                PropertyValues.newBuilder()
                    .putPvs("dcid", values(text("dcid0")))
                    .putPvs("typeOf", values(ref("Class")))
                    .build())
            .build();

    assertEquals(
        Map.of(subjectNode("dcid0", "dcid0", "Class"), Set.of(edge("dcid0", "typeOf", "Class"))),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_nodeWithoutType_defaultsToThingAndIncrementsCounter() {
    McfGraph graph =
        McfGraph.newBuilder().putNodes("dcid2", PropertyValues.newBuilder().build()).build();

    assertEquals(Map.of(subjectNode("dcid2", "dcid2", "Thing"), Set.of()), extract(graph));
    Mockito.verify(noTypeCounter, Mockito.times(1)).inc();
  }

  @Test
  public void extractExpandedNodesFromGraph_nodeWithoutDcidOrName_fallsBackToKey() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "dcid:node1",
                PropertyValues.newBuilder().putPvs("typeOf", values(ref("Class"))).build())
            .build();

    assertEquals(
        Map.of(subjectNode("node1", "node1", "Class"), Set.of(edge("node1", "typeOf", "Class"))),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_oversizedValue_dropsLeafNodeAndEdge() {
    String oversized = "a".repeat(SpannerClient.MAX_SPANNER_STRING_COLUMN_SIZE + 1);
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "dcid0",
                PropertyValues.newBuilder()
                    .putPvs("typeOf", values(ref("Class")))
                    .putPvs("name", values(text("Node Zero")))
                    .putPvs("description", values(text(oversized)))
                    .build())
            .build();

    assertEquals(
        Map.of(
            subjectNode("dcid0", "Node Zero", "Class"),
            Set.of(
                edge("dcid0", "typeOf", "Class"),
                edge("dcid0", "name", leaf("Node Zero").getSubjectId())),
            leaf("Node Zero"),
            Set.of()),
        extract(graph));
  }

  @Test
  public void extractExpandedNodesFromGraph_pipelineAndMutations() {
    McfGraph graph =
        McfGraph.newBuilder()
            .putNodes(
                "dcid0",
                PropertyValues.newBuilder()
                    .putPvs("name", values(text("Node Zero")))
                    .putPvs("typeOf", values(ref("Class")))
                    .build())
            .build();

    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .build();
    Counter invalidTypeCounter = Metrics.counter(GraphReaderTest.class, "invalid_type");
    Counter nodeCounter = Metrics.counter(GraphReaderTest.class, "nodes");
    Counter edgeCounter = Metrics.counter(GraphReaderTest.class, "edges");

    PCollection<McfGraph> inputGraph = pipeline.apply("CreateGraph", Create.of(graph));
    PCollection<ExpandedNode> expandedNodes =
        GraphReader.extractExpandedNodesFromGraph(
            "test", inputGraph, PROVENANCE, invalidTypeCounter);
    PCollection<Mutation> nodeMutations =
        GraphReader.nodesToMutations("test", expandedNodes, spannerClient, nodeCounter);
    PCollection<Mutation> edgeMutations =
        GraphReader.edgesToMutations("test", expandedNodes, spannerClient, edgeCounter);

    PAssert.that(nodeMutations)
        .containsInAnyOrder(
            spannerClient.toNodeMutation(subjectNode("dcid0", "Node Zero", "Class")),
            spannerClient.toNodeMutation(leaf("Node Zero")));
    PAssert.that(edgeMutations)
        .containsInAnyOrder(
            spannerClient.toEdgeMutation(edge("dcid0", "name", leaf("Node Zero").getSubjectId())),
            spannerClient.toEdgeMutation(edge("dcid0", "typeOf", "Class")));

    PipelineResult result = pipeline.run();
    result.waitUntilFinish();

    assertEquals(2L, getCounterValue(result, "nodes"));
    assertEquals(2L, getCounterValue(result, "edges"));
  }
}
