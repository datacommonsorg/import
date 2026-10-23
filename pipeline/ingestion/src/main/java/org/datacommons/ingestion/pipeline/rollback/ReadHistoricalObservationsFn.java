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

package org.datacommons.ingestion.pipeline.rollback;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerException;
import com.google.cloud.spanner.TimestampBound;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.ObservationRecord;

/**
 * Reads historical Observation rows at T_pre by keyed prefix lookup against their parent TimeSeries
 * primary keys.
 *
 * <p>Observation is {@code INTERLEAVE IN PARENT TimeSeries ON DELETE CASCADE}, so every Observation
 * row shares its parent's four-column key prefix and is stored physically adjacent to it. A prefix
 * read over a batch of parent keys therefore retrieves exactly that set's children, and parallelism
 * comes from the upstream {@code PCollection} of parent keys rather than from query partitioning.
 *
 * <p>Prefer this to the equivalent {@code Observation JOIN TimeSeries} query. That join plans as a
 * {@code Distributed Cross Apply} at the root, and Spanner can only partition a query whose plan is
 * rooted at a {@code Distributed Union}. Reading it through {@code SpannerIO} would mean disabling
 * {@link org.apache.beam.sdk.io.gcp.spanner.SpannerIO.Read#withBatching}, which falls back to a
 * single unsplittable {@code executeQuery} pinned to one worker -- unworkable at graph scale.
 */
public class ReadHistoricalObservationsFn extends DoFn<List<List<String>>, Mutation> {

  /**
   * Number of columns in the parent TimeSeries primary key: {@code (variable_measured, entity1,
   * extra_entities_id, facet_id)}.
   */
  public static final int PARENT_KEY_COLUMN_COUNT = 4;

  private final SpannerClient spannerClient;
  private final com.google.cloud.Timestamp tPre;
  private final String observationTable;
  private final Counter restoredObservationsCounter =
      Metrics.counter(ReadHistoricalObservationsFn.class, "rollback_restored_observations");
  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public ReadHistoricalObservationsFn(
      SpannerClient spannerClient, com.google.cloud.Timestamp tPre, String observationTable) {
    this.spannerClient = spannerClient;
    this.tPre = tPre;
    this.observationTable = observationTable;
  }

  @Setup
  public void setup() {
    this.spanner = spannerClient.createSpanner();
    this.dbClient = spannerClient.getDatabaseClient(spanner);
  }

  @Teardown
  public void teardown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @ProcessElement
  public void processElement(@Element List<List<String>> batch, OutputReceiver<Mutation> receiver) {
    if (batch == null || batch.isEmpty()) {
      return;
    }

    try (ResultSet rs =
        dbClient
            .singleUse(TimestampBound.ofReadTimestamp(tPre))
            .read(observationTable, toParentPrefixKeySet(batch), ObservationRecord.READ_COLUMNS)) {
      while (rs.next()) {
        restoredObservationsCounter.inc();
        receiver.output(
            ObservationRecord.from(rs.getCurrentRowAsStruct()).toMutation(observationTable));
      }
    } catch (SpannerException e) {
      throw new IllegalStateException(
          String.format(
              "Failed historical read on '%s' at T_pre (%s). "
                  + "Verify that T_pre is within Spanner's version retention period.",
              observationTable, tPre),
          e);
    }
  }

  /**
   * Converts a batch of parent TimeSeries primary keys into a {@link KeySet} of prefix ranges, each
   * range covering every interleaved Observation child row of that parent.
   */
  public static KeySet toParentPrefixKeySet(List<List<String>> parentKeys) {
    KeySet.Builder builder = KeySet.newBuilder();
    for (List<String> key : parentKeys) {
      if (key == null || key.size() != PARENT_KEY_COLUMN_COUNT) {
        throw new IllegalArgumentException(
            String.format(
                "Expected a TimeSeries primary key of %d columns but got: %s",
                PARENT_KEY_COLUMN_COUNT, key));
      }
      // Fully qualified: the inherited DoFn.Key annotation shadows the simple name here.
      builder.addRange(
          KeyRange.prefix(
              com.google.cloud.spanner.Key.of(key.get(0), key.get(1), key.get(2), key.get(3))));
    }
    return builder.build();
  }
}
