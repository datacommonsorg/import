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
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class KeyValueStoreRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("type")
            .to("Config")
            .set("key")
            .to("k1")
            .set("provenance")
            .to("prov1")
            .set("value")
            .to(Value.json("{\"enabled\":true}"))
            .build();

    KeyValueStoreRecord record = KeyValueStoreRecord.from(struct);
    assertEquals("Config", record.type());
    assertEquals("k1", record.key());
    assertEquals("prov1", record.provenance());
    assertEquals("{\"enabled\":true}", record.value());

    Mutation mutation = record.toMutation("KeyValueStore");
    assertEquals("KeyValueStore", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Config", map.get("type").getString());
    assertEquals("k1", map.get("key").getString());
    assertEquals("prov1", map.get("provenance").getString());
    assertEquals("{\"enabled\":true}", map.get("value").getJson());
    assertEquals(KeyValueStoreRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testToMutationWithNullValue() {
    KeyValueStoreRecord record = new KeyValueStoreRecord("Config", "k2", "prov2", null);
    Mutation mutation = record.toMutation("KeyValueStore");
    var map = mutation.asMap();
    assertEquals("Config", map.get("type").getString());
    assertEquals("k2", map.get("key").getString());
    assertEquals("prov2", map.get("provenance").getString());
    assertEquals(Value.json(null), map.get("value"));
    assertEquals(KeyValueStoreRecord.WRITABLE_COLUMNS, map.keySet());
  }
}
