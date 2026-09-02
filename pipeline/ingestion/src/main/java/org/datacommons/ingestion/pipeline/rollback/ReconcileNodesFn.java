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

import com.google.cloud.NoCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Struct;
import com.google.cloud.spanner.TimestampBound;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.TupleTag;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.spanner.model.NodeRecord;

/**
 * Reconciles batches of modified subject IDs against the historical snapshot at T_pre. Emits
 * restore mutations for nodes that existed at T_pre, and delete mutations for new nodes.
 */
public class ReconcileNodesFn extends DoFn<List<String>, Mutation> {
  public static final TupleTag<Mutation> RESTORE_NODES_TAG = new TupleTag<Mutation>() {};
  public static final TupleTag<Mutation> DELETE_NODES_TAG = new TupleTag<Mutation>() {};

  private final SpannerClient spannerClient;
  private final com.google.cloud.Timestamp tPre;
  private final String emulatorHost;
  private final Counter restoredNodesCounter =
      Metrics.counter(ReconcileNodesFn.class, "rollback_restored_nodes");
  private final Counter deletedNodesCounter =
      Metrics.counter(ReconcileNodesFn.class, "rollback_deleted_nodes");
  private transient Spanner spanner;
  private transient DatabaseClient dbClient;

  public ReconcileNodesFn(
      SpannerClient spannerClient, com.google.cloud.Timestamp tPre, String emulatorHost) {
    this.spannerClient = spannerClient;
    this.tPre = tPre;
    this.emulatorHost = emulatorHost;
  }

  @Setup
  public void setup() {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(spannerClient.getGcpProjectId());
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      builder.setEmulatorHost(emulatorHost.trim());
      builder.setCredentials(NoCredentials.getInstance());
    }
    this.spanner = builder.build().getService();
    this.dbClient =
        spanner.getDatabaseClient(
            DatabaseId.of(
                spannerClient.getGcpProjectId(),
                spannerClient.getSpannerInstanceId(),
                spannerClient.getSpannerDatabaseId()));
  }

  @Teardown
  public void teardown() {
    if (spanner != null) {
      spanner.close();
    }
  }

  @ProcessElement
  public void processElement(@Element List<String> batch, MultiOutputReceiver receiver) {
    if (batch == null || batch.isEmpty()) {
      return;
    }

    KeySet.Builder keySetBuilder = KeySet.newBuilder();
    for (String subjectId : batch) {
      keySetBuilder.addKey(com.google.cloud.spanner.Key.of(subjectId));
    }

    Set<String> restoredIds = new HashSet<>();
    try (ResultSet rs =
        dbClient
            .singleUse(TimestampBound.ofReadTimestamp(tPre))
            .read(
                spannerClient.getNodeTableName(), keySetBuilder.build(), NodeRecord.READ_COLUMNS)) {
      while (rs.next()) {
        Struct row = rs.getCurrentRowAsStruct();
        String id = row.getString(NodeRecord.COL_SUBJECT_ID);
        restoredIds.add(id);
        restoredNodesCounter.inc();
        receiver
            .get(RESTORE_NODES_TAG)
            .output(NodeRecord.from(row).toMutation(spannerClient.getNodeTableName()));
      }
    }

    for (String id : batch) {
      if (!restoredIds.contains(id)) {
        deletedNodesCounter.inc();
        receiver
            .get(DELETE_NODES_TAG)
            .output(
                Mutation.delete(
                    spannerClient.getNodeTableName(), com.google.cloud.spanner.Key.of(id)));
      }
    }
  }
}
