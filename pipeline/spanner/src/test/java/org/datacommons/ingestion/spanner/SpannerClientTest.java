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
import org.datacommons.ingestion.spanner.model.EdgeRecord;
import org.datacommons.ingestion.spanner.model.KeyValueStoreRecord;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.datacommons.ingestion.spanner.model.ObservationRecord;
import org.datacommons.ingestion.spanner.model.TimeSeriesRecord;
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
  public void testToNodeRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("value")
            .to("California")
            .set("bytes")
            .to((com.google.cloud.ByteArray) null)
            .set("name")
            .to("State of California")
            .set("types")
            .toStringArray(List.of("State", "Place"))
            .set("last_update_timestamp")
            .to(com.google.cloud.Timestamp.now())
            .build();

    Mutation mutation = NodeRecord.from(struct).toMutation("Node");
    assertEquals("Node", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("California", map.get("value").getString());
    assertTrue(map.get("bytes").isNull());
    assertEquals("State of California", map.get("name").getString());
    assertEquals(List.of("State", "Place"), map.get("types").getStringArray());
  }

  @Test
  public void testToEdgeRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("predicate")
            .to("containedInPlace")
            .set("object_id")
            .to("country/USA")
            .set("provenance")
            .to("dc/base/Test")
            .set("last_update_timestamp")
            .to(com.google.cloud.Timestamp.now())
            .build();

    Mutation mutation = EdgeRecord.from(struct).toMutation("Edge");
    assertEquals("Edge", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("containedInPlace", map.get("predicate").getString());
    assertEquals("country/USA", map.get("object_id").getString());
    assertEquals("dc/base/Test", map.get("provenance").getString());
  }

  @Test
  public void testToTimeSeriesRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("variable_measured")
            .to("Count_Person")
            .set("extra_entities_id")
            .to("extra1")
            .set("facet_id")
            .to("facet1")
            .set("entities")
            .to(com.google.cloud.spanner.Value.json("{\"entity1\":\"geoId/06\"}"))
            .set("facet")
            .to(com.google.cloud.spanner.Value.json("{\"provenance\":\"test\"}"))
            .set("last_update_timestamp")
            .to(com.google.cloud.Timestamp.now())
            .build();

    Mutation mutation = TimeSeriesRecord.from(struct).toMutation("TimeSeries");
    assertEquals("TimeSeries", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("{\"entity1\":\"geoId/06\"}", map.get("entities").getJson());
    assertEquals("{\"provenance\":\"test\"}", map.get("facet").getJson());
  }

  @Test
  public void testToObservationRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("variable_measured")
            .to("Count_Person")
            .set("entity1")
            .to("geoId/06")
            .set("extra_entities_id")
            .to("extra1")
            .set("facet_id")
            .to("facet1")
            .set("date")
            .to("2020")
            .set("value")
            .to("100")
            .set("last_update_timestamp")
            .to(com.google.cloud.Timestamp.now())
            .build();

    Mutation mutation = ObservationRecord.from(struct).toMutation("Observation");
    assertEquals("Observation", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("geoId/06", map.get("entity1").getString());
    assertEquals("2020", map.get("date").getString());
    assertEquals("100", map.get("value").getString());
  }

  @Test
  public void testToKeyValueStoreRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("type")
            .to("Config")
            .set("key")
            .to("k1")
            .set("provenance")
            .to("prov1")
            .set("value")
            .to(com.google.cloud.spanner.Value.json("{\"enabled\":true}"))
            .build();

    Mutation mutation = KeyValueStoreRecord.from(struct).toMutation("KeyValueStore");
    assertEquals("KeyValueStore", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Config", map.get("type").getString());
    assertEquals("k1", map.get("key").getString());
    assertEquals("prov1", map.get("provenance").getString());
    assertEquals("{\"enabled\":true}", map.get("value").getJson());
  }

  @Test
  public void testToNodeEmbeddingRestoreMutation() {
    com.google.cloud.spanner.Struct struct =
        com.google.cloud.spanner.Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("embedding_label")
            .to("label1")
            .set("embedding_content_key")
            .to("key1")
            .set("embedding_content")
            .to(com.google.cloud.spanner.Value.json("{\"text\":\"California\"}"))
            .set("node_types")
            .toStringArray(List.of("State"))
            .set("embeddings")
            .toFloat64Array(List.of(0.1, 0.2, 0.3))
            .build();

    Mutation mutation = NodeEmbeddingRecord.from(struct).toMutation("NodeEmbedding");
    assertEquals("NodeEmbedding", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("label1", map.get("embedding_label").getString());
    assertEquals(List.of("State"), map.get("node_types").getStringArray());
    assertEquals(List.of(0.1, 0.2, 0.3), map.get("embeddings").getFloat64Array());
  }
}
