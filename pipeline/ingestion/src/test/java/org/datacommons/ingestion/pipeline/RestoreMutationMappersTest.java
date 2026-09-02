// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.List;
import java.util.Set;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RestoreMutationMappersTest {

  @Test
  public void testMutationSymmetry_rollbackRestoresAllForwardIngestionColumns() {
    SpannerClient client =
        SpannerClient.builder()
            .gcpProjectId("test-project")
            .spannerInstanceId("test-instance")
            .spannerDatabaseId("test-db")
            .nodeTableName("Node")
            .edgeTableName("Edge")
            .timeSeriesTableName("TimeSeries")
            .observationTableName("Observation")
            .build();

    // 1. Node Symmetry
    Node sampleNode =
        Node.builder()
            .subjectId("geoId/06")
            .name("California")
            .types(List.of("State", "Place"))
            .value("Val")
            .bytes(ByteArray.copyFrom("bytes"))
            .build();

    Set<String> forwardNodeCols = client.toNodeMutation(sampleNode).asMap().keySet();
    assertEquals(
        "toNodeMutation columns must exactly match NODE_WRITABLE_COLUMNS",
        SpannerClient.NODE_WRITABLE_COLUMNS,
        forwardNodeCols);

    // 2. Edge Symmetry
    Edge sampleEdge =
        Edge.builder()
            .subjectId("geoId/06")
            .predicate("containedInPlace")
            .objectId("country/USA")
            .provenance("dc/base/Test")
            .build();

    Set<String> forwardEdgeCols = client.toEdgeMutation(sampleEdge).asMap().keySet();
    assertEquals(
        "toEdgeMutation columns must exactly match EDGE_WRITABLE_COLUMNS",
        SpannerClient.EDGE_WRITABLE_COLUMNS,
        forwardEdgeCols);

    // 3. Observation Symmetry
    TimeSeriesKey key =
        new TimeSeriesKey(
            "Count_Person", "geoId/06", "", "P1Y", "CensusMethod", "Person", "1", "facet123");
    Observation sampleObs = Observation.builder().seriesKey(key).date("2020").value("100").build();

    Set<String> forwardObsCols = client.toObservationMutation(sampleObs).asMap().keySet();
    assertEquals(
        "toObservationMutation columns must exactly match OBSERVATION_WRITABLE_COLUMNS",
        SpannerClient.OBSERVATION_WRITABLE_COLUMNS,
        forwardObsCols);

    // 4. TimeSeries Symmetry
    TimeSeries sampleTs =
        TimeSeries.builder()
            .variableMeasured("testStatVar")
            .entity1("geoId/testPlace")
            .importName("test_import")
            .isBaseDc(true)
            .isDcAggregate(false)
            .measurementMethod("method")
            .observationPeriod("P1Y")
            .build();

    Set<String> forwardTsCols = client.toTimeSeriesMutation(sampleTs).asMap().keySet();
    assertEquals(
        "toTimeSeriesMutation columns must exactly match TIME_SERIES_WRITABLE_COLUMNS",
        SpannerClient.TIME_SERIES_WRITABLE_COLUMNS,
        forwardTsCols);
  }

  @Test
  public void testToTimeSeriesRestoreMutation_omitsGeneratedColumns() {
    Struct struct =
        Struct.newBuilder()
            .set("variable_measured")
            .to("Count_Person")
            .set("extra_entities_id")
            .to("E123")
            .set("facet_id")
            .to("F456")
            .set("entities")
            .to(Value.json("{\"entity1\":\"geoId/06\"}"))
            .set("facet")
            .to(Value.json("{\"provenance\":\"dc/base/Census\"}"))
            .build();

    Mutation mutation = RestoreMutationMappers.toTimeSeriesRestoreMutation(struct, "TimeSeries");
    assertNotNull(mutation);
    assertEquals("TimeSeries", mutation.getTable());

    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("E123", map.get("extra_entities_id").getString());
    assertEquals("F456", map.get("facet_id").getString());
    assertEquals("{\"entity1\":\"geoId/06\"}", map.get("entities").getJson());
    assertEquals("{\"provenance\":\"dc/base/Census\"}", map.get("facet").getJson());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());

    // CRITICAL: entity1 and provenance MUST NOT be written to TimeSeries directly
    assertFalse(map.containsKey("entity1"));
    assertFalse(map.containsKey("provenance"));
  }

  @Test
  public void testToNodeRestoreMutation_handlesNullsSafely() {
    Struct struct =
        Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("name")
            .to("California")
            .set("types")
            .toStringArray(List.of("State", "Place"))
            .set("value")
            .to((String) null)
            .set("bytes")
            .to((ByteArray) null)
            .build();

    Mutation mutation = RestoreMutationMappers.toNodeRestoreMutation(struct, "Node");
    assertNotNull(mutation);
    assertEquals("Node", mutation.getTable());

    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("California", map.get("name").getString());
    assertEquals(List.of("State", "Place"), map.get("types").getStringArray());
    assertTrue(map.get("value").isNull());
    assertTrue(map.get("bytes").isNull());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
  }

  @Test
  public void testToKeyValueStoreRestoreMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("type")
            .to("ProvenanceSummary")
            .set("key")
            .to("Count_Person")
            .set("provenance")
            .to("dc/base/Census")
            .set("value")
            .to(Value.json("{\"import_name\":\"Census\"}"))
            .build();

    Mutation mutation =
        RestoreMutationMappers.toKeyValueStoreRestoreMutation(struct, "KeyValueStore");
    assertNotNull(mutation);
    assertEquals("KeyValueStore", mutation.getTable());

    var map = mutation.asMap();
    assertEquals("ProvenanceSummary", map.get("type").getString());
    assertEquals("Count_Person", map.get("key").getString());
    assertEquals("dc/base/Census", map.get("provenance").getString());
    assertEquals("{\"import_name\":\"Census\"}", map.get("value").getJson());
  }

  @Test
  public void testToNodeEmbeddingRestoreMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("subject_id")
            .to("dcid:Count_Person")
            .set("embedding_label")
            .to("label1")
            .set("embedding_content_key")
            .to("key1")
            .set("embedding_content")
            .to(Value.json("{\"content\":\"text\"}"))
            .set("node_types")
            .toStringArray(List.of("StatisticalVariable"))
            .set("embeddings")
            .toFloat64Array(List.of(0.1, 0.2, 0.3))
            .build();

    Mutation mutation =
        RestoreMutationMappers.toNodeEmbeddingRestoreMutation(struct, "NodeEmbedding");
    assertNotNull(mutation);
    assertEquals("NodeEmbedding", mutation.getTable());

    var map = mutation.asMap();
    assertEquals("dcid:Count_Person", map.get("subject_id").getString());
    assertEquals("label1", map.get("embedding_label").getString());
    assertEquals("key1", map.get("embedding_content_key").getString());
    assertEquals(List.of(0.1, 0.2, 0.3), map.get("embeddings").getFloat64Array());
  }
}
