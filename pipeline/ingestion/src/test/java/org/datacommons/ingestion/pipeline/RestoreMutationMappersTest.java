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

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class RestoreMutationMappersTest {

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
    assertFalse(map.containsKey("value"));
    assertFalse(map.containsKey("bytes"));
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
