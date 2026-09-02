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
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.data.TimeSeries;

/**
 * Immutable canonical record representing a single row in the Spanner TimeSeries table.
 *
 * <p>Serves as the single contract between forward ingestion POJOs and Spanner historical snapshot
 * Structs, eliminating duplication across forward and rollback mutation builders.
 */
public record TimeSeriesRecord(
    String variableMeasured, String extraEntitiesId, String facetId, String entities, String facet)
    implements Serializable {

  private static final Gson GSON = new Gson();

  public static final String COL_VARIABLE_MEASURED = "variable_measured";
  public static final String COL_EXTRA_ENTITIES_ID = "extra_entities_id";
  public static final String COL_FACET_ID = "facet_id";
  public static final String COL_ENTITIES = "entities";
  public static final String COL_FACET = "facet";
  public static final String COL_LAST_UPDATE_TIMESTAMP = "last_update_timestamp";
  public static final String COL_ENTITY1 = "entity1";
  public static final String COL_PROVENANCE = "provenance";

  public static final List<String> READ_COLUMNS =
      List.of(COL_VARIABLE_MEASURED, COL_EXTRA_ENTITIES_ID, COL_FACET_ID, COL_ENTITIES, COL_FACET);

  public static final Set<String> WRITABLE_COLUMNS =
      Set.of(
          COL_VARIABLE_MEASURED,
          COL_EXTRA_ENTITIES_ID,
          COL_FACET_ID,
          COL_ENTITIES,
          COL_FACET,
          COL_LAST_UPDATE_TIMESTAMP);

  /** Adapts forward ingestion's domain {@link TimeSeries} POJO. */
  public static TimeSeriesRecord from(TimeSeries obs) {
    String variableMeasured = obs.getVariableMeasured();
    String entity1 = obs.getEntity1();
    String extraEntitiesId = obs.getExtraEntitiesId();
    String facetId = obs.getFacetId();

    // Create entities JSON
    JsonObject entitiesJson = new JsonObject();
    entitiesJson.addProperty(COL_ENTITY1, entity1);
    List<String> extra = obs.getExtraEntities();
    if (extra != null) {
      if (extra.size() > 0) {
        addPropertyIfNotEmpty(entitiesJson, "entity2", extra.get(0));
      }
      if (extra.size() > 1) {
        addPropertyIfNotEmpty(entitiesJson, "entity3", extra.get(1));
      }
    }

    // Create facet JSON
    JsonObject facetJson = new JsonObject();
    facetJson.addProperty(
        COL_PROVENANCE, ProvenanceUtils.getProvenanceDcid(obs.getImportName(), obs.getIsBaseDc()));
    addPropertyIfNotEmpty(facetJson, "measurementMethod", obs.getMeasurementMethod());
    addPropertyIfNotEmpty(facetJson, "observationPeriod", obs.getObservationPeriod());
    addPropertyIfNotEmpty(facetJson, "scalingFactor", obs.getScalingFactor());
    addPropertyIfNotEmpty(facetJson, "unit", obs.getUnit());
    facetJson.addProperty("isDcAggregate", obs.getIsDcAggregate());

    return new TimeSeriesRecord(
        variableMeasured,
        extraEntitiesId,
        facetId,
        GSON.toJson(entitiesJson),
        GSON.toJson(facetJson));
  }

  /** Adapts a historical Spanner {@link Struct} row from a snapshot read. */
  public static TimeSeriesRecord from(Struct struct) {
    return new TimeSeriesRecord(
        struct.getString(COL_VARIABLE_MEASURED),
        struct.getString(COL_EXTRA_ENTITIES_ID),
        struct.getString(COL_FACET_ID),
        struct.isNull(COL_ENTITIES) ? null : struct.getJson(COL_ENTITIES),
        struct.isNull(COL_FACET) ? null : struct.getJson(COL_FACET));
  }

  /** Builds the canonical Spanner Mutation for this record. */
  public Mutation toMutation(String tableName) {
    Mutation mutation =
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set(COL_VARIABLE_MEASURED)
            .to(variableMeasured)
            // entity1 is a STORED generated column in TimeSeries, DO NOT write to it directly!
            .set(COL_EXTRA_ENTITIES_ID)
            .to(extraEntitiesId)
            .set(COL_FACET_ID)
            .to(facetId)
            .set(COL_ENTITIES)
            .to(entities != null ? Value.json(entities) : Value.json(null))
            .set(COL_FACET)
            .to(facet != null ? Value.json(facet) : Value.json(null))
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

  private static void addPropertyIfNotEmpty(JsonObject json, String property, String value) {
    if (value != null && !value.trim().isEmpty()) {
      json.addProperty(property, value);
    }
  }
}
