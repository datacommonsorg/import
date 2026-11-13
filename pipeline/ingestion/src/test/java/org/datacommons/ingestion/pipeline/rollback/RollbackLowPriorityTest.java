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

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.KeySet;
import com.google.cloud.spanner.Options.ReadOption;
import com.google.cloud.spanner.Options.UpdateOption;
import com.google.cloud.spanner.ReadContext;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.TimestampBound;
import java.util.List;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;

/**
 * Verifies that the rollback's direct Spanner RPCs (those not issued through SpannerIO) run at
 * {@link SpannerClient#LOW_PRIORITY} so they yield CPU to online serving.
 */
@RunWith(JUnit4.class)
public class RollbackLowPriorityTest {
  private static final Timestamp T_PRE = Timestamp.parseTimestamp("2026-01-01T00:00:00Z");

  private DatabaseClient dbClient;
  private ReadContext readContext;
  private SpannerClient spannerClient;

  @Before
  public void setUp() {
    dbClient = mock(DatabaseClient.class);
    readContext = mock(ReadContext.class);
    when(dbClient.singleUse(any(TimestampBound.class))).thenReturn(readContext);
    // A mocked ResultSet is empty: next() returns false.
    when(readContext.read(anyString(), any(KeySet.class), any(), any()))
        .thenReturn(mock(ResultSet.class));
    spannerClient = new FakeSpannerClient(dbClient);
  }

  @Test
  public void testPartitionedDelete_usesLowPriority() {
    SpannerPartitionedDeleteFn fn =
        new SpannerPartitionedDeleteFn(spannerClient, "Edge", "provenance");
    fn.setup();

    fn.processElement(List.of("dc/base/TestImport"), outputReceiver());

    ArgumentCaptor<UpdateOption> options = ArgumentCaptor.forClass(UpdateOption.class);
    verify(dbClient).executePartitionedUpdate(any(Statement.class), options.capture());
    assertEquals(
        "Partitioned DML must pass only SpannerClient.LOW_PRIORITY",
        List.of(SpannerClient.LOW_PRIORITY),
        options.getAllValues());
  }

  @Test
  public void testHistoricalObservationRead_usesLowPriority() {
    ReadHistoricalObservationsFn fn =
        new ReadHistoricalObservationsFn(spannerClient, T_PRE, "Observation");
    fn.setup();

    fn.processElement(List.of(List.of("Count_Person", "geoId/06", "", "facet1")), outputReceiver());

    assertReadUsedOnlyLowPriority("Observation");
  }

  @Test
  public void testNodeReconcileRead_usesLowPriority() {
    ReconcileNodesFn fn = new ReconcileNodesFn(spannerClient, T_PRE);
    fn.setup();

    fn.processElement(List.of("geoId/06"), mock(MultiOutputReceiver.class, RETURNS_MOCKS));

    assertReadUsedOnlyLowPriority(SpannerRollbackPipeline.TABLE_NODE);
  }

  @Test
  public void testNodeEmbeddingReconcileRead_usesLowPriority() {
    ReconcileNodeEmbeddingsFn fn = new ReconcileNodeEmbeddingsFn(spannerClient, T_PRE);
    fn.setup();

    fn.processElement(List.of("geoId/06"), mock(MultiOutputReceiver.class, RETURNS_MOCKS));

    assertReadUsedOnlyLowPriority(ReconcileNodeEmbeddingsFn.NODE_EMBEDDING_TABLE);
  }

  private void assertReadUsedOnlyLowPriority(String table) {
    ArgumentCaptor<ReadOption> options = ArgumentCaptor.forClass(ReadOption.class);
    verify(readContext).read(eq(table), any(KeySet.class), any(), options.capture());
    assertEquals(
        "Read of " + table + " must pass only SpannerClient.LOW_PRIORITY",
        List.of(SpannerClient.LOW_PRIORITY),
        options.getAllValues());
  }

  @SuppressWarnings("unchecked")
  private static <T> OutputReceiver<T> outputReceiver() {
    return mock(OutputReceiver.class);
  }

  /** Hands the DoFns a mocked {@link DatabaseClient} instead of connecting to Spanner. */
  private static class FakeSpannerClient extends SpannerClient {
    private final transient DatabaseClient dbClient;

    FakeSpannerClient(DatabaseClient dbClient) {
      super(
          SpannerClient.builder()
              .gcpProjectId("test")
              .spannerInstanceId("test")
              .spannerDatabaseId("test"));
      this.dbClient = dbClient;
    }

    @Override
    public Spanner createSpanner() {
      return mock(Spanner.class);
    }

    @Override
    public DatabaseClient getDatabaseClient(Spanner spanner) {
      return dbClient;
    }
  }
}
