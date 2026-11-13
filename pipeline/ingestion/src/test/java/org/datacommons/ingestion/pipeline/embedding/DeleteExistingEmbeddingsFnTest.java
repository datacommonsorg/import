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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Statement;
import com.google.common.collect.Lists;
import java.util.List;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DeleteExistingEmbeddingsFn}. */
@RunWith(JUnit4.class)
public class DeleteExistingEmbeddingsFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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
            "NodeEmbedding", "base_text_embedding", List.of("Count_Person", "", "dc/topic/1"));

    assertEquals(Mutation.Op.DELETE, mutation.getOperation());
    assertEquals("NodeEmbedding", mutation.getTable());
    List<KeyRange> ranges = Lists.newArrayList(mutation.getKeySet().getRanges());
    assertEquals(2, ranges.size());
    assertEquals(KeyRange.prefix(Key.of("Count_Person", "base_text_embedding")), ranges.get(0));
    assertEquals(KeyRange.prefix(Key.of("dc/topic/1", "base_text_embedding")), ranges.get(1));
  }

  @Test
  public void testBuildDeleteMutations_pipeline() {
    PCollection<String> subjectIds = pipeline.apply(Create.of("Count_Person", "Count_Person"));

    PCollection<Mutation> mutations =
        DeleteExistingEmbeddingsFn.buildDeleteMutations(
            subjectIds, "NodeEmbedding", "base_text_embedding");

    Mutation expected =
        DeleteExistingEmbeddingsFn.buildDeleteMutation(
            "NodeEmbedding", "base_text_embedding", List.of("Count_Person"));
    PAssert.that(mutations).containsInAnyOrder(expected);
    PipelineResult result = pipeline.run();

    MetricQueryResults metrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            DeleteExistingEmbeddingsFn.class,
                            "embedding_deleted_nodes:base_text_embedding"))
                    .build());
    List<MetricResult<Long>> counters = Lists.newArrayList(metrics.getCounters());
    assertEquals(1, counters.size());
    assertEquals(Long.valueOf(1L), counters.get(0).getCommitted());
  }
}
