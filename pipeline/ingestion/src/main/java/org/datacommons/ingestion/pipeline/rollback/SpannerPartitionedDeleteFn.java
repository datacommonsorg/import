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
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Worker DoFn executing Partitioned DML updates on Cloud Spanner to delete rows for a target list
 * of column values (e.g. target provenances) at HEAD.
 */
public class SpannerPartitionedDeleteFn extends DoFn<List<String>, Void> {
  private static final Logger LOGGER = LoggerFactory.getLogger(SpannerPartitionedDeleteFn.class);

  private final SpannerClient spannerClient;
  private final String tableName;
  private final String columnName;
  private final String emulatorHost;

  public SpannerPartitionedDeleteFn(
      SpannerClient spannerClient, String tableName, String columnName, String emulatorHost) {
    this.spannerClient = spannerClient;
    this.tableName = tableName;
    this.columnName = columnName;
    this.emulatorHost = emulatorHost;
  }

  @ProcessElement
  public void processElement(@Element List<String> values, OutputReceiver<Void> receiver) {
    if (values == null || values.isEmpty()) {
      receiver.output(null);
      return;
    }
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(spannerClient.getGcpProjectId());
    if (emulatorHost != null && !emulatorHost.trim().isEmpty()) {
      builder.setEmulatorHost(emulatorHost.trim());
      builder.setCredentials(NoCredentials.getInstance());
    }
    try (Spanner spanner = builder.build().getService()) {
      DatabaseClient dbClient =
          spanner.getDatabaseClient(
              DatabaseId.of(
                  spannerClient.getGcpProjectId(),
                  spannerClient.getSpannerInstanceId(),
                  spannerClient.getSpannerDatabaseId()));
      String dml =
          String.format(
              "DELETE FROM %s WHERE %s IN UNNEST(@%s)", tableName, columnName, columnName);
      Statement statement =
          Statement.newBuilder(dml).bind(columnName).toStringArray(values).build();
      long rowCount = dbClient.executePartitionedUpdate(statement);
      LOGGER.info("Deleted {} rows from {} for {} IN {}", rowCount, tableName, columnName, values);
      receiver.output(null);
    }
  }
}
