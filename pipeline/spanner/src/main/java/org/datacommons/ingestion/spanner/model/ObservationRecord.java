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

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.Value;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.TimeSeriesKey;

/**
 * Immutable canonical record representing a single row in the Spanner Observation table.
 *
 * <p>Serves as the single contract between forward ingestion POJOs and Spanner historical snapshot
 * Structs, eliminating duplication across forward and rollback mutation builders.
 */
public record ObservationRecord(
    String variableMeasured,
    String entity1,
    String extraEntitiesId,
    String facetId,
    String date,
    String value)
    implements Serializable {

  public static final String COL_VARIABLE_MEASURED = "variable_measured";
  public static final String COL_ENTITY1 = "entity1";
  public static final String COL_EXTRA_ENTITIES_ID = "extra_entities_id";
  public static final String COL_FACET_ID = "facet_id";
  public static final String COL_DATE = "date";
  public static final String COL_VALUE = "value";
  public static final String COL_LAST_UPDATE_TIMESTAMP = "last_update_timestamp";

  public static final List<String> READ_COLUMNS =
      List.of(
          COL_VARIABLE_MEASURED,
          COL_ENTITY1,
          COL_EXTRA_ENTITIES_ID,
          COL_FACET_ID,
          COL_DATE,
          COL_VALUE);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(
          COL_VARIABLE_MEASURED,
          COL_ENTITY1,
          COL_EXTRA_ENTITIES_ID,
          COL_FACET_ID,
          COL_DATE,
          COL_VALUE,
          COL_LAST_UPDATE_TIMESTAMP);

  /** Adapts forward ingestion's domain {@link Observation} POJO. */
  public static ObservationRecord from(Observation obs) {
    TimeSeriesKey key = obs.getSeriesKey();
    return new ObservationRecord(
        key != null ? key.getVariableMeasured() : "",
        key != null ? key.getEntity1() : "",
        key != null ? key.getExtraEntitiesId() : "",
        key != null ? key.getFacetId() : "",
        obs.getDate(),
        obs.getValue());
  }

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static ObservationRecord from(Struct struct) {
    return new ObservationRecord(
        struct.getString(COL_VARIABLE_MEASURED),
        struct.getString(COL_ENTITY1),
        struct.getString(COL_EXTRA_ENTITIES_ID),
        struct.getString(COL_FACET_ID),
        struct.getString(COL_DATE),
        struct.getString(COL_VALUE));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set(COL_VARIABLE_MEASURED)
            .to(variableMeasured)
            .set(COL_ENTITY1)
            .to(entity1)
            .set(COL_EXTRA_ENTITIES_ID)
            .to(extraEntitiesId)
            .set(COL_FACET_ID)
            .to(facetId)
            .set(COL_DATE)
            .to(date)
            .set(COL_VALUE)
            .to(value)
            .set(COL_LAST_UPDATE_TIMESTAMP)
            .to(Value.COMMIT_TIMESTAMP)
            .build();
    if (!WRITABLE_COLUMNS.equals(mutation.asMap().keySet())) {
      throw new IllegalStateException(
          "Mutation columns "
              + mutation.asMap().keySet()
              + " do not match WRITABLE_COLUMNS "
              + WRITABLE_COLUMNS);
    }
    return mutation;
  }
}
