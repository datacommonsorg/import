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

package org.datacommons.ingestion.pipeline.embedding;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.pipeline.MockSpannerClient;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeleteExistingEmbeddingsFn}. */
@RunWith(JUnit4.class)
public class DeleteExistingEmbeddingsFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    MockSpannerClient.clearCapturedMutations();
  }

  @Test
  public void testBuildSubjectIdsByProvenanceStatement() {
    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId("test-proj")
            .spannerInstanceId("test-inst")
            .spannerDatabaseId("test-db")
            .build();

    Statement stmt =
        DeleteExistingEmbeddingsFn.buildSubjectIdsByProvenanceStatement(
            spannerClient, "dc/base/Schema");
    assertEquals(
        "SELECT DISTINCT subject_id FROM Edge WHERE provenance = @provenance", stmt.getSql());
    assertTrue(stmt.hasBinding("provenance"));
    assertEquals("dc/base/Schema", stmt.getParameters().get("provenance").getString());
  }

  @Test
  public void testBuildDeleteMutation() {
    Mutation mutation =
        DeleteExistingEmbeddingsFn.buildDeleteMutation(
            "NodeEmbedding", List.of("Count_Person", "", "dc/topic/1"));

    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    assertEquals("NodeEmbedding", mutation.getTable());
    List<KeyRange> ranges = Lists.newArrayList(mutation.getKeySet().getRanges());
    assertEquals(2, ranges.size());
    assertEquals(KeyRange.prefix(Key.of("Count_Person")), ranges.get(0));
    assertEquals(KeyRange.prefix(Key.of("dc/topic/1")), ranges.get(1));
  }

  @Test
  public void testBuildDeleteMutations_pipeline() {
    PCollection<String> subjectIds = pipeline.apply(Create.of("Count_Person"));

    PCollection<Mutation> mutations =
        DeleteExistingEmbeddingsFn.buildDeleteMutations("testImport", subjectIds, "NodeEmbedding");

    Mutation expected =
        DeleteExistingEmbeddingsFn.buildDeleteMutation("NodeEmbedding", List.of("Count_Person"));
    PAssert.that(mutations).containsInAnyOrder(expected);
    PipelineResult result = pipeline.run();

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            DeleteExistingEmbeddingsFn.class, "embedding_deleted_nodes"))
                    .build());
    List<MetricResult<Long>> counters = Lists.newArrayList(metrics.getCounters());
    assertEquals(1, counters.size());
    assertEquals(Long.valueOf(1L), counters.get(0).getCommitted());
  }

  @Test
  public void testProcessElement_dedupsIdsInSameBatch() {
    PCollection<List<String>> batchedIds =
        pipeline.apply(Create.of(List.of(List.of("Count_Person", "Count_Person", "dc/topic/1"))));

    PCollection<Mutation> mutations =
        batchedIds.apply(ParDo.of(new DeleteExistingEmbeddingsFn("NodeEmbedding")));

    Mutation expected =
        DeleteExistingEmbeddingsFn.buildDeleteMutation(
            "NodeEmbedding", List.of("Count_Person", "dc/topic/1"));
    PAssert.that(mutations).containsInAnyOrder(expected);
    PipelineResult result = pipeline.run();

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            DeleteExistingEmbeddingsFn.class, "embedding_deleted_nodes"))
                    .build());
    List<MetricResult<Long>> counters = Lists.newArrayList(metrics.getCounters());
    assertEquals(1, counters.size());
    assertEquals(Long.valueOf(2L), counters.get(0).getCommitted());
  }

  @Test
  public void testReadSubjectIdsByProvenance_usesReadTimestampSnapshot() {
    Timestamp readTimestamp = Timestamp.ofTimeSecondsAndNanos(1700000123L, 0);
    SpannerClient spannerClient =
        new MockSpannerClient(Map.of("dc/base/Schema", List.of("Count_Person", "Farm")));

    assertThrows(
        NullPointerException.class,
        () ->
            DeleteExistingEmbeddingsFn.readSubjectIdsByProvenance(
                pipeline, spannerClient, "dc/base/Schema", "schema", null));

    PCollection<String> subjectIds =
        DeleteExistingEmbeddingsFn.readSubjectIdsByProvenance(
            pipeline, spannerClient, "dc/base/Schema", "schema", readTimestamp);

    PAssert.that(subjectIds).containsInAnyOrder("Count_Person", "Farm");
    pipeline.run().waitUntilFinish();

    assertEquals(List.of("dc/base/Schema"), MockSpannerClient.getCapturedEdgeReads());
    assertEquals(List.of(readTimestamp), MockSpannerClient.getCapturedEdgeReadTimestamps());
  }
}
