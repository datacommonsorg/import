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
import com.google.cloud.spanner.Mutation.Op;
import com.google.cloud.spanner.Struct;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class LinkedEdgeRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("predicate")
            .to("containedInPlace")
            .set("ancestor")
            .to("country/USA")
            .set("child_type")
            .to("County")
            .set("child")
            .to("geoId/06085")
            .set("provenance")
            .to("dc/base/generated/Place")
            .build();

    LinkedEdgeRecord record = LinkedEdgeRecord.from(struct);
    assertEquals("containedInPlace", record.predicate());
    assertEquals("country/USA", record.ancestor());
    assertEquals("County", record.childType());
    assertEquals("geoId/06085", record.child());
    assertEquals("dc/base/generated/Place", record.provenance());

    Mutation mutation = record.toMutation("LinkedEdge");
    assertEquals("LinkedEdge", mutation.getTable());
    assertEquals(Op.INSERT_OR_UPDATE, mutation.getOperation());
    var map = mutation.asMap();
    assertEquals("containedInPlace", map.get("predicate").getString());
    assertEquals("country/USA", map.get("ancestor").getString());
    assertEquals("County", map.get("child_type").getString());
    assertEquals("geoId/06085", map.get("child").getString());
    assertEquals("dc/base/generated/Place", map.get("provenance").getString());
    assertEquals(LinkedEdgeRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testReadColumnsCoverEveryWritableColumn() {
    // The rollback restore selects READ_COLUMNS at T_pre and writes them back via toMutation, so
    // every column in the mutation must be read, or the restored rows would silently lose data.
    assertEquals(LinkedEdgeRecord.WRITABLE_COLUMNS, Set.copyOf(LinkedEdgeRecord.READ_COLUMNS));
  }
}
