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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.gax.rpc.UnaryCallable;
import com.google.cloud.aiplatform.v1.PredictRequest;
import com.google.cloud.aiplatform.v1.PredictResponse;
import com.google.cloud.aiplatform.v1.PredictionServiceClient;
import com.google.cloud.aiplatform.v1.stub.PredictionServiceStub;
import com.google.protobuf.ListValue;
import com.google.protobuf.Struct;
import com.google.protobuf.Value;
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link VertexAiEmbeddingClient}. */
@RunWith(JUnit4.class)
public class VertexAiEmbeddingClientTest {

  @Test
  public void testConstructorResolution_success() {
    VertexAiEmbeddingClient defaultClient =
        new VertexAiEmbeddingClient("my-gcp-project", "us-east1");

    assertEquals(defaultClient.getProjectId(), "my-gcp-project");
    assertEquals(defaultClient.getLocation(), "us-east1");
    assertEquals(defaultClient.getEndpoint(), "us-east1-aiplatform.googleapis.com:443");
  }

  @Test
  public void testConstructorResolution_fail() {
    assertThrows(
        IllegalArgumentException.class, () -> new VertexAiEmbeddingClient(null, "us-central1"));
    assertThrows(
        IllegalArgumentException.class, () -> new VertexAiEmbeddingClient("  ", "us-central1"));
    assertThrows(
        IllegalArgumentException.class, () -> new VertexAiEmbeddingClient("my-gcp-project", null));
    assertThrows(
        IllegalArgumentException.class, () -> new VertexAiEmbeddingClient("my-gcp-project", "   "));
  }

  @Test
  public void testBuildPredictRequest_withTaskType_success() {
    VertexAiEmbeddingClient client = new VertexAiEmbeddingClient("my-gcp-project", "us-central1");

    PredictRequest request =
        client.buildPredictRequest(
            "text-embedding-005", List.of("Total Population", "Median Income"), "RETRIEVAL_QUERY");

    assertEquals(
        request.getEndpoint(),
        "projects/my-gcp-project/locations/us-central1/publishers/google/models/text-embedding-005");
    assertEquals(request.getInstancesCount(), 2);

    Struct firstInstance = request.getInstances(0).getStructValue();
    assertEquals(firstInstance.getFieldsOrThrow("content").getStringValue(), "Total Population");
    assertEquals(firstInstance.getFieldsOrThrow("task_type").getStringValue(), "RETRIEVAL_QUERY");
  }

  @Test
  public void testBuildPredictRequest_withoutTaskType_omitsTaskType() {
    VertexAiEmbeddingClient client = new VertexAiEmbeddingClient("my-gcp-project", "us-central1");

    PredictRequest requestNoTaskType =
        client.buildPredictRequest("text-embedding-005", List.of("Hello"), null);

    assertFalse(
        requestNoTaskType.getInstances(0).getStructValue().getFieldsMap().containsKey("task_type"));
  }

  @Test
  public void testParsePredictResponse_success() {
    PredictResponse response =
        PredictResponse.newBuilder()
            .addPredictions(buildPredictionValue(List.of(0.1, -0.25, 0.5)))
            .addPredictions(buildPredictionValue(List.of(0.75, 0.0, -0.5)))
            .build();

    List<List<Double>> vectors = VertexAiEmbeddingClient.parsePredictResponse(response, 2);

    assertEquals(vectors.size(), 2);
    assertEquals(vectors.get(0), List.of(0.1, -0.25, 0.5));
    assertEquals(vectors.get(1), List.of(0.75, 0.0, -0.5));
  }

  @Test
  public void testParsePredictResponse_nullResponse_throwsException() {
    assertThrows(
        IllegalStateException.class, () -> VertexAiEmbeddingClient.parsePredictResponse(null, 2));
  }

  @Test
  public void testParsePredictResponse_countMismatch_throwsException() {
    PredictResponse response =
        PredictResponse.newBuilder()
            .addPredictions(buildPredictionValue(List.of(0.1, -0.25, 0.5)))
            .addPredictions(buildPredictionValue(List.of(0.75, 0.0, -0.5)))
            .build();

    assertThrows(
        IllegalStateException.class,
        () -> VertexAiEmbeddingClient.parsePredictResponse(response, 1));
  }

  @Test
  public void testPredictEmbeddings_beforeSetup_throwsException() {
    VertexAiEmbeddingClient client = new VertexAiEmbeddingClient("my-gcp-project", "us-central1");

    assertThrows(
        IllegalStateException.class,
        () ->
            client.predictEmbeddings(
                List.of("Count_Person"), "text-embedding-005", "RETRIEVAL_QUERY"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPredictEmbeddings_setupAndTeardownLifecycle_success() throws Exception {
    PredictionServiceStub mockStub = mock(PredictionServiceStub.class);
    UnaryCallable<PredictRequest, PredictResponse> mockCallable = mock(UnaryCallable.class);
    PredictResponse fakeResponse =
        PredictResponse.newBuilder()
            .addPredictions(buildPredictionValue(List.of(0.2, 0.8)))
            .build();
    when(mockStub.predictCallable()).thenReturn(mockCallable);
    when(mockCallable.call(any(PredictRequest.class))).thenReturn(fakeResponse);

    int[] createCount = {0};
    VertexAiEmbeddingClient client =
        new VertexAiEmbeddingClient("my-gcp-project", "us-central1") {
          @Override
          public void setup() {
            if (predictionServiceClient == null) {
              createCount[0]++;
              predictionServiceClient = PredictionServiceClient.create(mockStub);
            }
          }
        };

    client.setup();
    assertEquals(createCount[0], 1);

    List<List<Double>> firstBatch =
        client.predictEmbeddings(List.of("Count_Person"), "text-embedding-005", "RETRIEVAL_QUERY");
    List<List<Double>> secondBatch =
        client.predictEmbeddings(
            List.of("Median_Income_Person"), "text-embedding-005", "RETRIEVAL_QUERY");

    assertEquals(firstBatch, List.of(List.of(0.2, 0.8)));
    assertEquals(secondBatch, List.of(List.of(0.2, 0.8)));
    assertEquals(createCount[0], 1);

    assertTrue(
        client.predictEmbeddings(List.of(), "text-embedding-005", "RETRIEVAL_QUERY").isEmpty());

    client.close();
    verify(mockStub).close();
  }

  private static Value buildPredictionValue(List<Double> numbers) {
    ListValue.Builder listBuilder = ListValue.newBuilder();
    for (Double n : numbers) {
      listBuilder.addValues(Value.newBuilder().setNumberValue(n).build());
    }
    Value valuesField = Value.newBuilder().setListValue(listBuilder.build()).build();
    Value embeddingsField =
        Value.newBuilder()
            .setStructValue(Struct.newBuilder().putFields("values", valuesField).build())
            .build();
    return Value.newBuilder()
        .setStructValue(Struct.newBuilder().putFields("embeddings", embeddingsField).build())
        .build();
  }
}
