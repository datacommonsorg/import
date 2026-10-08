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

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import org.datacommons.ingestion.data.Edge;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class EdgeRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("subject_id")
            .to("geoId/06")
            .set("predicate")
            .to("containedInPlace")
            .set("object_id")
            .to("country/USA")
            .set("provenance")
            .to("dc/base/Test")
            .set("last_update_timestamp")
            .to(Timestamp.now())
            .build();

    EdgeRecord record = EdgeRecord.from(struct);
    assertEquals("geoId/06", record.subjectId());
    assertEquals("containedInPlace", record.predicate());
    assertEquals("country/USA", record.objectId());
    assertEquals("dc/base/Test", record.provenance());

    Mutation mutation = record.toMutation("Edge");
    assertEquals("Edge", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("geoId/06", map.get("subject_id").getString());
    assertEquals("containedInPlace", map.get("predicate").getString());
    assertEquals("country/USA", map.get("object_id").getString());
    assertEquals("dc/base/Test", map.get("provenance").getString());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
    assertEquals(EdgeRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testFromEdgeAndToMutation() {
    Edge edge =
        Edge.builder()
            .subjectId("dcid:subject")
            .predicate("dcid:predicate")
            .objectId("dcid:object")
            .provenance("dcid:provenance")
            .build();

    EdgeRecord record = EdgeRecord.from(edge);
    assertEquals("dcid:subject", record.subjectId());
    assertEquals("dcid:predicate", record.predicate());
    assertEquals("dcid:object", record.objectId());
    assertEquals("dcid:provenance", record.provenance());

    Mutation mutation = record.toMutation("Edge");
    assertNotNull(mutation);
    var map = mutation.asMap();
    assertEquals("dcid:subject", map.get("subject_id").getString());
    assertEquals(EdgeRecord.WRITABLE_COLUMNS, map.keySet());
  }
}
