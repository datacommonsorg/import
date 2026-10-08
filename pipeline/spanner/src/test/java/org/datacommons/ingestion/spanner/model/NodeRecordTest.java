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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.google.cloud.ByteArray;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import java.util.List;
import org.datacommons.ingestion.data.Node;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class NodeRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("value")
            .to("California")
            .set("bytes")
            .to((ByteArray) null)
            .set("name")
            .to("State of California")
            .set("types")
            .toStringArray(List.of("State", "Place"))
            .set("last_update_timestamp")
            .to(Timestamp.now())
            .build();

    NodeRecord record = NodeRecord.from(struct);
    assertEquals("geoId/06", record.subjectId());
    assertEquals("California", record.value());
    assertEquals("State of California", record.name());
    assertEquals(List.of("State", "Place"), record.types());

    Mutation mutation = record.toMutation("Node");
    assertEquals("Node", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("California", map.get("value").getString());
    assertTrue(map.get("bytes").isNull());
    assertEquals("State of California", map.get("name").getString());
    assertEquals(List.of("State", "Place"), map.get("types").getStringArray());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
    assertEquals(NodeRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testFromNodeAndToMutation() {
    Node node =
        Node.builder()
            .subjectId("dcid:123")
            .value("val123")
            .bytes(ByteArray.copyFrom("bytes123"))
            .name("Test Node")
            .types(List.of("TypeA"))
            .build();

    NodeRecord record = NodeRecord.from(node);
    assertEquals("dcid:123", record.subjectId());
    assertEquals("val123", record.value());
    assertEquals(ByteArray.copyFrom("bytes123"), record.bytes());
    assertEquals("Test Node", record.name());
    assertEquals(List.of("TypeA"), record.types());

    Mutation mutation = record.toMutation("Node");
    assertNotNull(mutation);
    var map = mutation.asMap();
    assertEquals("dcid:123", map.get("subject_id").getString());
    assertEquals(NodeRecord.WRITABLE_COLUMNS, map.keySet());
  }
}
