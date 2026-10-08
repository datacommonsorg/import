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

package org.datacommons.ingestion.spanner.model;

import static org.junit.Assert.assertEquals;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NodeEmbeddingRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("embedding_label")
            .to("label1")
            .set("embedding_content_key")
            .to("key1")
            .set("embedding_content")
            .to(Value.json("{\"text\":\"California\"}"))
            .set("node_types")
            .toStringArray(List.of("State"))
            .set("embeddings")
            .toFloat64Array(List.of(0.1, 0.2, 0.3))
            .build();

    NodeEmbeddingRecord record = NodeEmbeddingRecord.from(struct);
    assertEquals("geoId/06", record.subjectId());
    assertEquals("label1", record.embeddingLabel());
    assertEquals("key1", record.embeddingContentKey());
    assertEquals("{\"text\":\"California\"}", record.embeddingContent());
    assertEquals(List.of("State"), record.nodeTypes());
    assertEquals(List.of(0.1, 0.2, 0.3), record.embeddings());

    Mutation mutation = record.toMutation("NodeEmbedding");
    assertEquals("NodeEmbedding", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("label1", map.get("embedding_label").getString());
    assertEquals("key1", map.get("embedding_content_key").getString());
    assertEquals(List.of("State"), map.get("node_types").getStringArray());
    assertEquals(List.of(0.1, 0.2, 0.3), map.get("embeddings").getFloat64Array());
    assertEquals(NodeEmbeddingRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testToMutationWithNulls() {
    NodeEmbeddingRecord record =
        new NodeEmbeddingRecord("geoId/06", "label1", "key1", null, null, null);
    Mutation mutation = record.toMutation("NodeEmbedding");
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals(NodeEmbeddingRecord.WRITABLE_COLUMNS, map.keySet());
  }
}
