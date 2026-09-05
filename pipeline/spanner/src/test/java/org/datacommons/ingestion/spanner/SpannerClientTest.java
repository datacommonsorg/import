package org.datacommons.ingestion.spanner;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.List;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.junit.Before;
import org.junit.Test;

public class SpannerClientTest {

  private SpannerClient spannerClient;

  @Before
  public void setUp() {
    spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .build();
  }

  @Test
  public void testToNodeMutation_RegularNode() {
    Node node =
        Node.builder()
            .subjectId("dcid:123")
            .value("value123")
            .name("Node Name")
            .types(List.of("Type1", "Type2"))
            .build();

    Mutation mutation = spannerClient.toNodeMutation(node);
    assertNotNull(mutation);
    assertEquals(mutation.getTable(), "Node");

    var mutationMap = mutation.asMap();
    assertEquals(mutationMap.get("subject_id").getString(), "dcid:123");
    assertEquals(mutationMap.get("value").getString(), "value123");
    assertEquals(mutationMap.get("name").getString(), "Node Name");
    assertEquals(mutationMap.get("types").getStringArray(), List.of("Type1", "Type2"));
    assertEquals(mutationMap.get("last_update_timestamp").toString(), "spanner.commit_timestamp()");
  }

  @Test
  public void testToNodeMutation_ProvisionalNode() {
    Node node = Node.builder().subjectId("dcid:456").types(List.of("ProvisionalNode")).build();

    Mutation mutation = spannerClient.toNodeMutation(node);
    assertNotNull(mutation);
    assertEquals(mutation.getTable(), "Node");

    var mutationMap = mutation.asMap();
    assertEquals(mutationMap.get("subject_id").getString(), "dcid:456");
    assertFalse(mutationMap.containsKey("value"));
    assertEquals(mutationMap.get("last_update_timestamp").toString(), "spanner.commit_timestamp()");
  }

  @Test
  public void testToEdgeMutation() {
    Edge edge =
        Edge.builder()
            .subjectId("dcid:subject")
            .predicate("dcid:predicate")
            .objectId("dcid:object")
            .provenance("dcid:provenance")
            .build();

    Mutation mutation = spannerClient.toEdgeMutation(edge);

    assertEquals(mutation.getTable(), "Edge");
    var mutationMap = mutation.asMap();
    assertEquals(mutationMap.get("subject_id").getString(), "dcid:subject");
    assertEquals(mutationMap.get("predicate").getString(), "dcid:predicate");
    assertEquals(mutationMap.get("object_id").getString(), "dcid:object");
    assertEquals(mutationMap.get("provenance").getString(), "dcid:provenance");
    assertEquals(mutationMap.get("last_update_timestamp").toString(), "spanner.commit_timestamp()");
  }

  @Test
  public void testToTimeSeriesMutation_skipEmptyValues() {
    TimeSeries obs =
        TimeSeries.builder()
            .variableMeasured("testStatVar")
            .entity1("geoId/testPlace")
            .importName("test_import")
            .isBaseDc(true)
            .isDcAggregate(false)
            .measurementMethod("   ") // Whitespace only
            .observationPeriod("") // Empty
            .build();

    Mutation mutation = spannerClient.toTimeSeriesMutation(obs);
    assertNotNull(mutation);
    assertEquals("TimeSeries", mutation.getTable());

    var mutationMap = mutation.asMap();
    assertEquals("testStatVar", mutationMap.get("variable_measured").getString());
    assertEquals("", mutationMap.get("extra_entities_id").getString());

    // Verify facet JSON
    String facetJsonStr = mutationMap.get("facet").getJson();
    assertNotNull(facetJsonStr);

    // Parse the JSON to verify content
    JsonObject facetJson = JsonParser.parseString(facetJsonStr).getAsJsonObject();
    assertEquals("dc/base/test_import", facetJson.get("provenance").getAsString());
    assertEquals(false, facetJson.get("isDcAggregate").getAsBoolean());

    // Verify that empty/whitespace fields are NOT present
    assertFalse(facetJson.has("measurementMethod"));
    assertFalse(facetJson.has("observationPeriod"));
    assertFalse(facetJson.has("scalingFactor"));
    assertFalse(facetJson.has("unit"));
  }

  @Test
  public void testToObservationMutation() {
    TimeSeriesKey key =
        new TimeSeriesKey(
            "Count_Person", "geoId/06", "extra1", "P1Y", "CensusMethod", "Person", "1", "facet123");
    Observation obs = Observation.builder().seriesKey(key).date("2020").value("100").build();

    Mutation mutation = spannerClient.toObservationMutation(obs);
    assertNotNull(mutation);
    assertEquals("Observation", mutation.getTable());

    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("geoId/06", map.get("entity1").getString());
    assertEquals("extra1", map.get("extra_entities_id").getString());
    assertEquals("facet123", map.get("facet_id").getString());
    assertEquals("2020", map.get("date").getString());
    assertEquals("100", map.get("value").getString());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
  }

  @Test
  public void testFormatPartitionQuery_production_preservesCleanSql() {
    SpannerClient prodClient =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .build();

    String query =
        prodClient.formatPartitionQuery(
            "SELECT * FROM TimeSeries WHERE provenance = '%s'", "dc/base/Test");
    assertEquals("SELECT * FROM TimeSeries WHERE provenance = 'dc/base/Test'", query);
    assertFalse(query.contains("spanner_emulator"));
  }

  @Test
  public void testFormatPartitionQuery_emulator_prependsHint() {
    SpannerClient emulatorClient =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .emulatorHost("localhost:9010")
            .build();

    String query =
        emulatorClient.formatPartitionQuery(
            "SELECT * FROM TimeSeries WHERE provenance = '%s'", "dc/base/Test");
    assertEquals(
        "@{spanner_emulator.disable_query_partitionability_check=true} SELECT * FROM TimeSeries WHERE provenance = 'dc/base/Test'",
        query);
    assertTrue(query.startsWith("@{spanner_emulator.disable_query_partitionability_check=true}"));
  }
}
