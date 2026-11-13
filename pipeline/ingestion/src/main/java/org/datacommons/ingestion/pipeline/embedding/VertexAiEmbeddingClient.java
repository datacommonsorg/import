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

import com.google.cloud.aiplatform.v1.EndpointName;
import com.google.cloud.aiplatform.v1.PredictRequest;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.PredictionServiceSettings;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Serializable client for generating vector embeddings from text via Vertex AI {@link
 * PredictionServiceClient} (e.g., {@code text-embedding-005}).
 */
public class VertexAiEmbeddingClient implements Serializable, AutoCloseable {
  public static final String DEFAULT_LOCATION = "us-central1";
  private static final String DEFAULT_PUBLISHER = "google";
  private static final String ENDPOINT_SUFFIX = "-aiplatform.googleapis.com:443";

  private final String projectId;
  private final String location;
  private final String endpoint;
  private transient PredictionServiceClient predictionServiceClient;

  public VertexAiEmbeddingClient(String projectId) {
    this(projectId, DEFAULT_LOCATION);
  }

  public VertexAiEmbeddingClient(String projectId, String location) {
    if (projectId == null || projectId.trim().isEmpty()) {
      throw new IllegalArgumentException("projectId must not be null or empty");
    }
    this.projectId = projectId.trim();
    this.location =
        (location == null || location.trim().isEmpty()) ? DEFAULT_LOCATION : location.trim();
    this.endpoint = this.location + ENDPOINT_SUFFIX;
  }

  public String getProjectId() {
    return projectId;
  }

  public String getLocation() {
    return location;
  }

  public String getEndpoint() {
    return endpoint;
  }

  /**
   * Initializes the underlying {@link PredictionServiceClient}. Should be called once per worker
   * (e.g., in a Beam {@code @Setup} method) before calling {@link #predictEmbeddings}.
   */
  public void setup() throws IOException {
    if (predictionServiceClient == null) {
      predictionServiceClient = createPredictionServiceClient();
    }
  }

  /**
   * Predicts vector embeddings for a batch of input text strings using the specified Vertex AI
   * model endpoint and task type.
   */
  public List<List<Double>> predictEmbeddings(
      List<String> texts, String modelEndpoint, String taskType) {
    if (texts == null || texts.isEmpty()) {
      return ImmutableList.of();
    }
    if (predictionServiceClient == null) {
      throw new IllegalStateException(
          "VertexAiEmbeddingClient is not initialized; call setup() before predictEmbeddings()");
    }
    PredictRequest request = buildPredictRequest(modelEndpoint, texts, taskType);
    PredictResponse response = predictionServiceClient.predict(request);
    return parsePredictResponse(response, texts.size());
  }

  PredictionServiceClient createPredictionServiceClient() throws IOException {
    PredictionServiceSettings settings =
        PredictionServiceSettings.newBuilder().setEndpoint(endpoint).build();
    return PredictionServiceClient.create(settings);
  }

  @Override
  public void close() {
    if (predictionServiceClient != null) {
      predictionServiceClient.close();
      predictionServiceClient = null;
    }
  }

  PredictRequest buildPredictRequest(String modelEndpoint, List<String> texts, String taskType) {
    if (modelEndpoint == null || modelEndpoint.trim().isEmpty()) {
      throw new IllegalArgumentException("modelEndpoint must not be null or empty");
    }
    EndpointName endpointName =
        EndpointName.ofProjectLocationPublisherModelName(
            projectId, location, DEFAULT_PUBLISHER, modelEndpoint.trim());
    ImmutableList<Value> instances =
        texts.stream()
            .map(text -> buildInstanceValue(text, taskType))
            .collect(ImmutableList.toImmutableList());
    return PredictRequest.newBuilder()
        .setEndpoint(endpointName.toString())
        .addAllInstances(instances)
        .build();
  }

  static Value buildInstanceValue(String text, String taskType) {
    Struct.Builder structBuilder =
        Struct.newBuilder().putFields("content", valueOf(Objects.requireNonNullElse(text, "")));
    if (taskType != null && !taskType.trim().isEmpty()) {
      structBuilder.putFields("task_type", valueOf(taskType.trim()));
    }
    return Value.newBuilder().setStructValue(structBuilder.build()).build();
  }

  static ImmutableList<List<Double>> parsePredictResponse(
      PredictResponse response, int expectedCount) {
    if (response == null || response.getPredictionsCount() != expectedCount) {
      int actualCount = response == null ? 0 : response.getPredictionsCount();
      throw new IllegalStateException(
          String.format(
              "Expected %d embedding predictions from Vertex AI, but got %d",
              expectedCount, actualCount));
    }
    return response.getPredictionsList().stream()
        .map(VertexAiEmbeddingClient::extractVector)
        .collect(ImmutableList.toImmutableList());
  }

  static ImmutableList<Double> extractVector(Value prediction) {
    Value embeddings = prediction.getStructValue().getFieldsOrThrow("embeddings");
    Value values = embeddings.getStructValue().getFieldsOrThrow("values");
    return values.getListValue().getValuesList().stream()
        .map(Value::getNumberValue)
        .collect(ImmutableList.toImmutableList());
  }

  private static Value valueOf(String s) {
    return Value.newBuilder().setStringValue(s).build();
  }
}
