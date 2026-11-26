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

package org.datacommons.ingestion.pipeline;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.datacommons.ingestion.pipeline.embedding.VertexAiEmbeddingClient;

/**
 * Shared test helper extending {@link VertexAiEmbeddingClient} that captures predicted batches in
 * memory and returns deterministic vector embeddings instead of calling Vertex AI.
 */
public class MockVertexAIClient extends VertexAiEmbeddingClient {
  private static final List<List<String>> CAPTURED_CALLS =
      Collections.synchronizedList(new ArrayList<>());

  private final List<Double> fixedVector;

  public MockVertexAIClient() {
    this("test-project", "us-central1", List.of(0.1, 0.2, 0.3));
  }

  public MockVertexAIClient(List<Double> fixedVector) {
    this("test-project", "us-central1", fixedVector);
  }

  public MockVertexAIClient(String projectId, String location, List<Double> fixedVector) {
    super(projectId, location);
    this.fixedVector = new ArrayList<>(fixedVector);
  }

  public static List<List<String>> getCapturedCalls() {
    return CAPTURED_CALLS;
  }

  public static void clearCapturedCalls() {
    CAPTURED_CALLS.clear();
  }

  @Override
  public void setup() {}

  @Override
  public List<List<Double>> predictEmbeddings(
      List<String> embeddingContents, String modelEndpoint, String taskType) {
    CAPTURED_CALLS.add(List.copyOf(embeddingContents));
    List<List<Double>> vectors = new ArrayList<>(embeddingContents.size());
    for (int i = 0; i < embeddingContents.size(); i++) {
      vectors.add(fixedVector);
    }
    return vectors;
  }

  @Override
  public void close() {}
}
