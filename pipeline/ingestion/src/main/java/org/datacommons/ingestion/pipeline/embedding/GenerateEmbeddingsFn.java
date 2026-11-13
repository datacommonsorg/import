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

import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.spanner.SpannerClient;

/**
 * Batches {@link EmbeddingContent} elements via {@link GroupIntoBatches}, invokes {@link
 * VertexAiEmbeddingClient#predictEmbeddings(List, String, String)} per batch using the model
 * endpoint and task type from {@link EmbeddingSpec}, and emits Spanner {@link Mutation}s for the
 * {@code NodeEmbedding} table.
 */
public class GenerateEmbeddingsFn extends DoFn<List<EmbeddingContent>, Mutation> {
  public static final int DEFAULT_BATCH_SIZE = 100;
  public static final int MAX_BATCH_SIZE = 250;
  private static final int BATCH_SHARDS = 128;

  private final VertexAiEmbeddingClient embeddingClient;
  private final String embeddingTableName;
  private final EmbeddingSpec spec;
  private final Counter embeddingsGeneratedCounter;
  private final Counter vertexBatchCallsCounter;

  public GenerateEmbeddingsFn(
      VertexAiEmbeddingClient embeddingClient, String embeddingTableName, EmbeddingSpec spec) {
    this.embeddingClient =
        Objects.requireNonNull(embeddingClient, "embeddingClient must not be null");
    this.spec = Objects.requireNonNull(spec, "spec must not be null");
    if (embeddingTableName == null || embeddingTableName.trim().isEmpty()) {
      throw new IllegalArgumentException("embeddingTableName must not be null or empty");
    }
    this.embeddingTableName = embeddingTableName.trim();
    String label = spec.embeddingLabel();
    this.embeddingsGeneratedCounter =
        Metrics.counter(GenerateEmbeddingsFn.class, "node_embeddings_generated:" + label);
    this.vertexBatchCallsCounter =
        Metrics.counter(GenerateEmbeddingsFn.class, "vertex_ai_batch_calls:" + label);
  }

  /**
   * Batches {@code contents}, predicts vector embeddings via {@code embeddingClient} for {@code
   * spec}, gates writes on {@code deleteWaitSignal} (when non-null), and writes the resulting
   * {@link Mutation}s to Spanner via {@link SpannerClient#writeMutations}.
   */
  public static PCollection<Void> generateAndWriteEmbeddings(
      Pipeline pipeline,
      SpannerClient spannerClient,
      PCollection<EmbeddingContent> contents,
      PCollection<Void> deleteWaitSignal,
      VertexAiEmbeddingClient embeddingClient,
      String embeddingTableName,
      EmbeddingSpec spec,
      int batchSize) {
    String label = spec.embeddingLabel();
    PCollection<Mutation> mutations =
        buildEmbeddingMutations(contents, embeddingClient, embeddingTableName, spec, batchSize);
    PCollection<Mutation> mutationsToWrite =
        deleteWaitSignal != null
            ? mutations.apply("WaitForDeletedEmbeddings_" + label, Wait.on(deleteWaitSignal))
            : mutations;
    SpannerWriteResult writeResult =
        spannerClient.writeMutations(pipeline, "WriteNodeEmbeddings_" + label, mutationsToWrite);
    return writeResult.getOutput();
  }

  /**
   * Keys and batches {@code contents} into chunks of {@code batchSize} and applies {@link
   * GenerateEmbeddingsFn} to produce Spanner {@link Mutation}s for {@code embeddingTableName}.
   */
  public static PCollection<Mutation> buildEmbeddingMutations(
      PCollection<EmbeddingContent> contents,
      VertexAiEmbeddingClient embeddingClient,
      String embeddingTableName,
      EmbeddingSpec spec,
      int batchSize) {
    String label = spec.embeddingLabel();
    int effectiveBatchSize =
        batchSize > 0 ? Math.min(batchSize, MAX_BATCH_SIZE) : DEFAULT_BATCH_SIZE;
    return contents
        .apply(
            "KeyContentsForEmbedding_" + label,
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.integers(), TypeDescriptor.of(EmbeddingContent.class)))
                .via(
                    c ->
                        KV.of(
                            Math.floorMod(
                                (c.subjectId() == null ? "" : c.subjectId()).hashCode(),
                                BATCH_SHARDS),
                            c)))
        .apply("BatchContentsForEmbedding_" + label, GroupIntoBatches.ofSize(effectiveBatchSize))
        .apply(
            "ExtractContentBatches_" + label,
            MapElements.into(TypeDescriptors.lists(TypeDescriptor.of(EmbeddingContent.class)))
                .via(kv -> Lists.newArrayList(kv.getValue())))
        .apply(
            "GenerateEmbeddings_" + label,
            ParDo.of(new GenerateEmbeddingsFn(embeddingClient, embeddingTableName, spec)));
  }

  @Setup
  public void setup() throws IOException {
    embeddingClient.setup();
  }

  @ProcessElement
  public void processElement(
      @Element List<EmbeddingContent> batch, OutputReceiver<Mutation> receiver) {
    ImmutableList<Mutation> mutations = buildMutationsForBatch(batch);
    if (!mutations.isEmpty()) {
      vertexBatchCallsCounter.inc();
      embeddingsGeneratedCounter.inc(mutations.size());
      mutations.forEach(receiver::output);
    }
  }

  ImmutableList<Mutation> buildMutationsForBatch(List<EmbeddingContent> batch) {
    if (batch == null || batch.isEmpty()) {
      return ImmutableList.of();
    }

    ImmutableList<String> texts =
        batch.stream().map(EmbeddingContent::textToEmbed).collect(ImmutableList.toImmutableList());

    List<List<Double>> vectors =
        embeddingClient.predictEmbeddings(texts, spec.modelEndpoint(), spec.taskType());
    if (vectors.size() != batch.size()) {
      throw new IllegalStateException(
          String.format(
              "Vertex AI returned %d vectors for a batch of %d EmbeddingContent items",
              vectors.size(), batch.size()));
    }

    return IntStream.range(0, batch.size())
        .mapToObj(
            i -> batch.get(i).toNodeEmbeddingRecord(vectors.get(i)).toMutation(embeddingTableName))
        .collect(ImmutableList.toImmutableList());
  }

  @Teardown
  public void teardown() {
    embeddingClient.close();
  }
}
