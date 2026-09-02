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
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeEmbeddingRecord;

/** Reconciles vector embeddings for restored nodes against the historical snapshot at T_pre. */
public class ReconcileNodeEmbeddingsFn extends DoFn<List<String>, Mutation> {
  public static final String NODE_EMBEDDING_TABLE = "NodeEmbedding";

  private final SpannerClient spannerClient;
  private final com.google.cloud.Timestamp tPre;
  private final Counter restoredEmbeddingsCounter =
      Metrics.counter(ReconcileNodeEmbeddingsFn.class, "rollback_restored_embeddings");
  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public ReconcileNodeEmbeddingsFn(SpannerClient spannerClient, com.google.cloud.Timestamp tPre) {
    this.spannerClient = spannerClient;
    this.tPre = tPre;
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
  public void processElement(@Element List<String> batch, OutputReceiver<Mutation> receiver) {
    if (batch == null || batch.isEmpty()) {
      return;
    }

    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String subjectId : batch) {
      keySetBuilder.addRange(KeyRange.prefix(com.google.cloud.spanner.Key.of(subjectId)));
    }

    try (ResultSet rs =
        dbClient
            .singleUse(TimestampBound.ofReadTimestamp(tPre))
            .read(NODE_EMBEDDING_TABLE, keySetBuilder.build(), NodeEmbeddingRecord.READ_COLUMNS)) {
      while (rs.next()) {
        Struct row = rs.getCurrentRowAsStruct();
        restoredEmbeddingsCounter.inc();
        receiver.output(NodeEmbeddingRecord.from(row).toMutation(NODE_EMBEDDING_TABLE));
      }
    }
  }
}
