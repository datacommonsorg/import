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
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeriesKey;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class ObservationRecordTest {

  @Test
  public void testFromStructAndToMutation() {
    Struct struct =
        Struct.newBuilder()
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
            .to(Timestamp.now())
            .build();

    ObservationRecord record = ObservationRecord.from(struct);
    assertEquals("Count_Person", record.variableMeasured());
    assertEquals("geoId/06", record.entity1());
    assertEquals("extra1", record.extraEntitiesId());
    assertEquals("facet1", record.facetId());
    assertEquals("2020", record.date());
    assertEquals("100", record.value());

    Mutation mutation = record.toMutation("Observation");
    assertEquals("Observation", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals("geoId/06", map.get("entity1").getString());
    assertEquals("2020", map.get("date").getString());
    assertEquals("100", map.get("value").getString());
    assertEquals("spanner.commit_timestamp()", map.get("last_update_timestamp").toString());
    assertEquals(ObservationRecord.WRITABLE_COLUMNS, map.keySet());
  }

  @Test
  public void testFromObservationAndToMutation() {
    TimeSeriesKey key =
        new TimeSeriesKey(
            "Count_Person", "geoId/06", "extra1", "P1Y", "CensusMethod", "Person", "1", "facet123");
    Observation obs = Observation.builder().seriesKey(key).date("2020").value("100").build();

    ObservationRecord record = ObservationRecord.from(obs);
    assertEquals("Count_Person", record.variableMeasured());
    assertEquals("geoId/06", record.entity1());
    assertEquals("extra1", record.extraEntitiesId());
    assertEquals("facet123", record.facetId());
    assertEquals("2020", record.date());
    assertEquals("100", record.value());

    Mutation mutation = record.toMutation("Observation");
    assertNotNull(mutation);
    assertEquals("Observation", mutation.getTable());
    var map = mutation.asMap();
    assertEquals("Count_Person", map.get("variable_measured").getString());
    assertEquals(ObservationRecord.WRITABLE_COLUMNS, map.keySet());
  }
}
