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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.util.List;
import org.datacommons.ingestion.data.TimeSeries;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TimeSeriesRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
            .set("variable_measured")
            .to("Count_Person")
            .set("extra_entities_id")
            .to("extra1")
            .set("facet_id")
            .to("facet1")
            .set("entities")
            .to(Value.json("{\"entity1\":\"geoId/06\"}"))
            .set("facet")
            .to(Value.json("{\"provenance\":\"test\"}"))
            .set("last_update_timestamp")
            .to(Timestamp.now())
            .build();

    TimeSeriesRecord record = TimeSeriesRecord.from(struct);
    assertEquals("Count_Person", record.variableMeasured());
    assertEquals("extra1", record.extraEntitiesId());
    assertEquals("facet1", record.facetId());
    assertEquals("{\"entity1\":\"geoId/06\"}", record.entities());
    assertEquals("{\"provenance\":\"test\"}", record.facet());

    Mutation mutation = record.toMutation("TimeSeries");
    assertEquals("TimeSeries", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("{\"entity1\":\"geoId/06\"}", map.get("entities").getJson());
    assertEquals("{\"provenance\":\"test\"}", map.get("facet").getJson());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
    assertEquals(TimeSeriesRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testFromTimeSeries_skipEmptyValues() {
    TimeSeries obs =
        TimeSeries.builder()
            .variableMeasured("testStatVar")
            .entity1("geoId/testPlace")
            .extraEntities(List.of("geoId/extraPlace"))
            .importName("test_import")
            .isBaseDc(true)
            .isDcAggregate(false)
            .measurementMethod("   ") // Whitespace only
            .observationPeriod("") // Empty
            .build();

    TimeSeriesRecord record = TimeSeriesRecord.from(obs);
    assertEquals("testStatVar", record.variableMeasured());

    Mutation mutation = record.toMutation("TimeSeries");
    assertNotNull(mutation);
    assertEquals("TimeSeries", mutation.getTable());

    var mutationMap = mutation.asMap();
    assertEquals("testStatVar", mutationMap.get("variable_measured").getString());
    assertEquals(TimeSeriesRecord.WRITABLE_COLUMNS, mutationMap.keySet());

    // Verify facet JSON skips empty/whitespace
    String facetJsonStr = mutationMap.get("facet").getJson();
    assertNotNull(facetJsonStr);
    JsonObject facetJson = JsonParser.parseString(facetJsonStr).getAsJsonObject();
    assertEquals("dc/base/test_import", facetJson.get("provenance").getAsString());
    assertEquals(false, facetJson.get("isDcAggregate").getAsBoolean());
    assertFalse(facetJson.has("measurementMethod"));
    assertFalse(facetJson.has("observationPeriod"));
    assertFalse(facetJson.has("scalingFactor"));
    assertFalse(facetJson.has("unit"));

    // Verify entities JSON contains entity1 and entity2
    String entitiesJsonStr = mutationMap.get("entities").getJson();
    assertNotNull(entitiesJsonStr);
    JsonObject entitiesJson = JsonParser.parseString(entitiesJsonStr).getAsJsonObject();
    assertEquals("geoId/testPlace", entitiesJson.get("entity1").getAsString());
    assertEquals("geoId/extraPlace", entitiesJson.get("entity2").getAsString());
  }
}
