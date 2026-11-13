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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenerateEmbeddingsFn}. */
@RunWith(JUnit4.class)
public class GenerateEmbeddingsFnTest implements Serializable {

  private static final EmbeddingSpec TEST_SPEC =
      new EmbeddingSpec(
          "base_text_embedding",
          "NodeEmbeddingModel",
          "text-embedding-005",
          "RETRIEVAL_QUERY",
          Map.of("StatisticalVariable", List.of("subject_id", "name")),
          NodeFilterType.NO_FILTER);

  private static final EmbeddingContent COUNT_PERSON_CONTENT =
      new EmbeddingContent(
          "Count_Person",
          "base_text_embedding",
          "123456789",
          "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
          "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
          List.of("StatisticalVariable"));

  public static final List<Mutation> CAPTURED_MUTATIONS =
      Collections.synchronizedList(new ArrayList<>());

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    CAPTURED_MUTATIONS.clear();
  }

  @Test
  public void testConstructorValidation() {
    VertexAiEmbeddingClient client = new VertexAiEmbeddingClient("test-project");
    assertThrows(
        NullPointerException.class,
        () -> new GenerateEmbeddingsFn(null, "NodeEmbedding", TEST_SPEC));
    assertThrows(
        NullPointerException.class, () -> new GenerateEmbeddingsFn(client, "NodeEmbedding", null));
    assertThrows(
        IllegalArgumentException.class, () -> new GenerateEmbeddingsFn(client, "  ", TEST_SPEC));
  }

  @Test
  public void testBuildMutationsForBatch_buildsMutationsAndValidatesVectorCount() {
    VertexAiEmbeddingClient fakeClient =
        new VertexAiEmbeddingClient("test-project") {
          @Override
          public List<List<Double>> predictEmbeddings(
              List<String> texts, String endpoint, String taskType) {
            assertEquals("text-embedding-005", endpoint);
            assertEquals("RETRIEVAL_QUERY", taskType);
            return texts.stream()
                .map(t -> (List<Double>) List.of(0.1, 0.2))
                .collect(ImmutableList.toImmutableList());
          }
        };

    GenerateEmbeddingsFn fn = new GenerateEmbeddingsFn(fakeClient, "NodeEmbedding", TEST_SPEC);

    ImmutableList<Mutation> mutations = fn.buildMutationsForBatch(List.of(COUNT_PERSON_CONTENT));
    assertEquals(1, mutations.size());
    assertEquals(
        COUNT_PERSON_CONTENT.toNodeEmbeddingRecord(List.of(0.1, 0.2)).toMutation("NodeEmbedding"),
        mutations.get(0));

    assertTrue(fn.buildMutationsForBatch(List.of()).isEmpty());

    // Vector count mismatch throws IllegalStateException
    VertexAiEmbeddingClient mismatchedClient =
        new VertexAiEmbeddingClient("test-project") {
          @Override
          public List<List<Double>> predictEmbeddings(
              List<String> texts, String endpoint, String taskType) {
            return List.of(List.of(0.1), List.of(0.2));
          }
        };
    GenerateEmbeddingsFn badFn =
        new GenerateEmbeddingsFn(mismatchedClient, "NodeEmbedding", TEST_SPEC);
    assertThrows(
        IllegalStateException.class,
        () -> badFn.buildMutationsForBatch(List.of(COUNT_PERSON_CONTENT)));
  }

  @Test
  public void testBuildEmbeddingMutations_pipelineAndMetrics() {
    VertexAiEmbeddingClient fakeClient = new FixedVectorEmbeddingClient(List.of(0.25, 0.75));

    PCollection<EmbeddingContent> contents = pipeline.apply(Create.of(COUNT_PERSON_CONTENT));

    PCollection<Mutation> mutations =
        GenerateEmbeddingsFn.buildEmbeddingMutations(
            contents, fakeClient, "NodeEmbedding", TEST_SPEC, 10);

    Mutation expected =
        COUNT_PERSON_CONTENT.toNodeEmbeddingRecord(List.of(0.25, 0.75)).toMutation("NodeEmbedding");
    PAssert.that(mutations).containsInAnyOrder(expected);
    PipelineResult result = pipeline.run();

    MetricQueryResults generatedMetrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            GenerateEmbeddingsFn.class,
                            "node_embeddings_generated:base_text_embedding"))
                    .build());
    List<MetricResult<Long>> generatedCounters = Lists.newArrayList(generatedMetrics.getCounters());
    assertEquals(1, generatedCounters.size());
    assertEquals(Long.valueOf(1L), generatedCounters.get(0).getCommitted());

    MetricQueryResults batchCallMetrics =
        result
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(
                        MetricNameFilter.named(
                            GenerateEmbeddingsFn.class,
                            "vertex_ai_batch_calls:base_text_embedding"))
                    .build());
    List<MetricResult<Long>> batchCallCounters = Lists.newArrayList(batchCallMetrics.getCounters());
    assertEquals(1, batchCallCounters.size());
    assertEquals(Long.valueOf(1L), batchCallCounters.get(0).getCommitted());
  }

  @Test
  public void testGenerateAndWriteEmbeddings_waitsOnDeleteAndWritesToSpanner() {
    VertexAiEmbeddingClient fakeClient = new FixedVectorEmbeddingClient(List.of(0.5, -0.5));

    PCollection<EmbeddingContent> contents =
        pipeline.apply("CreateContents", Create.of(COUNT_PERSON_CONTENT));
    PCollection<Void> deleteWaitSignal =
        pipeline.apply("CreateDeleteSignal", Create.empty(TypeDescriptor.of(Void.class)));

    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteOutput", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);

    SpannerClient mockSpannerClient = new CapturingSpannerClient(mockWriteResult);

    GenerateEmbeddingsFn.generateAndWriteEmbeddings(
        pipeline,
        mockSpannerClient,
        contents,
        deleteWaitSignal,
        fakeClient,
        "NodeEmbedding",
        TEST_SPEC,
        10);

    pipeline.run().waitUntilFinish();

    Mutation expected =
        COUNT_PERSON_CONTENT.toNodeEmbeddingRecord(List.of(0.5, -0.5)).toMutation("NodeEmbedding");
    assertEquals(List.of(expected), CAPTURED_MUTATIONS);
  }

  private static class FixedVectorEmbeddingClient extends VertexAiEmbeddingClient {
    private final ImmutableList<Double> vector;

    FixedVectorEmbeddingClient(List<Double> vector) {
      super("test-project");
      this.vector = ImmutableList.copyOf(vector);
    }

    @Override
    public void setup() {
      // No-op in unit tests so TestPipeline does not initialize a live gRPC client.
    }

    @Override
    public List<List<Double>> predictEmbeddings(
        List<String> texts, String modelEndpoint, String taskType) {
      return texts.stream()
          .map(t -> (List<Double>) vector)
          .collect(ImmutableList.toImmutableList());
    }
  }

  private static class CapturingSpannerClient extends SpannerClient {
    private final transient SpannerWriteResult mockWriteResult;

    CapturingSpannerClient(SpannerWriteResult mockWriteResult) {
      super(
          SpannerClient.builder()
              .gcpProjectId("test")
              .spannerInstanceId("test")
              .spannerDatabaseId("test"));
      this.mockWriteResult = mockWriteResult;
    }

    @Override
    public SpannerWriteResult writeMutations(
        Pipeline pipeline, String name, PCollection<Mutation> mutations) {
      mutations.apply(
          "Capture_" + name,
          ParDo.of(
              new DoFn<Mutation, Void>() {
                @ProcessElement
                public void processElement(@Element Mutation m) {
                  CAPTURED_MUTATIONS.add(m);
                }
              }));
      return mockWriteResult;
    }
  }
}
