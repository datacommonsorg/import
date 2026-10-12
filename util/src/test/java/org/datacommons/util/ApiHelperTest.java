package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import java.io.ByteArrayOutputStream;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class ApiHelperTest {

  @Test
  public void buildPropertyValuesRequestDefaultsToProd() {
    HttpRequest request =
        ApiHelper.buildPropertyValuesRequest(
            "{}", new DcApiConfig("https://api.datacommons.org", "prod-key"));

    assertEquals("https://api.datacommons.org/v2/node", request.uri().toString());
    assertEquals("prod-key", request.headers().firstValue("x-api-key").orElse(""));
  }

  @Test
  public void buildPropertyValuesRequestUsesExplicitRoot() {
    HttpRequest request =
        ApiHelper.buildPropertyValuesRequest(
            "{}", new DcApiConfig("https://custom.api.datacommons.org/", "key"));

    assertEquals("https://custom.api.datacommons.org/v2/node", request.uri().toString());
    assertEquals("key", request.headers().firstValue("x-api-key").orElse(""));
  }

  @Test
  public void buildPropertyValuesRequestOmitsMissingKey() {
    HttpRequest request =
        ApiHelper.buildPropertyValuesRequest(
            "{}", new DcApiConfig("https://api.datacommons.org", ""));

    assertEquals("https://api.datacommons.org/v2/node", request.uri().toString());
    assertTrue(request.headers().firstValue("x-api-key").isEmpty());
  }

  @Test
  public void fetchPropertyValuesMergesAllPages() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    HttpResponse<String> firstResponse = mock(HttpResponse.class);
    HttpResponse<String> secondResponse = mock(HttpResponse.class);
    when(firstResponse.statusCode()).thenReturn(200);
    when(secondResponse.statusCode()).thenReturn(200);
    when(firstResponse.body())
        .thenReturn(
            "{\"data\":{"
                + "\"nodeA\":{\"arcs\":{\"typeOf\":{\"nodes\":[{\"dcid\":\"Place\"}]}}},"
                + "\"nodeB\":{}},"
                + "\"nextToken\":\"next-page-token\"}");
    when(secondResponse.body())
        .thenReturn(
            "{\"data\":{"
                + "\"nodeA\":{\"arcs\":{\"typeOf\":{\"nodes\":[{\"dcid\":\"City\"}]}}},"
                + "\"nodeB\":{\"arcs\":{\"typeOf\":{\"nodes\":[{\"dcid\":\"Place\"}]}}}}}");
    doReturn(firstResponse, secondResponse).when(mockHttp).send(any(), any());

    JsonObject result =
        ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA", "nodeB"), "typeOf");

    ArgumentCaptor<HttpRequest> requestCaptor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(mockHttp, org.mockito.Mockito.times(2)).send(requestCaptor.capture(), any());
    JsonObject firstRequest = requestBody(requestCaptor.getAllValues().get(0));
    JsonObject secondRequest = requestBody(requestCaptor.getAllValues().get(1));
    assertFalse(firstRequest.has("nextToken"));
    assertEquals("next-page-token", secondRequest.get("nextToken").getAsString());
    assertEquals(firstRequest.get("nodes"), secondRequest.get("nodes"));
    assertEquals("->typeOf", secondRequest.get("property").getAsString());

    JsonArray nodeA = result.getAsJsonObject("nodeA").getAsJsonArray("out");
    JsonArray nodeB = result.getAsJsonObject("nodeB").getAsJsonArray("out");
    assertEquals(2, nodeA.size());
    assertEquals("Place", nodeA.get(0).getAsJsonObject().get("dcid").getAsString());
    assertEquals("City", nodeA.get(1).getAsJsonObject().get("dcid").getAsString());
    assertEquals(1, nodeB.size());
    assertEquals("Place", nodeB.get(0).getAsJsonObject().get("dcid").getAsString());
  }

  @Test
  public void fetchPropertyValuesDoesNotReturnPartialData() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    HttpResponse<String> firstResponse = mock(HttpResponse.class);
    HttpResponse<String> invalidResponse = mock(HttpResponse.class);
    when(firstResponse.statusCode()).thenReturn(200);
    when(invalidResponse.statusCode()).thenReturn(200);
    when(firstResponse.body())
        .thenReturn(
            "{\"data\":{\"nodeA\":{\"arcs\":{\"typeOf\":{\"nodes\":[{\"dcid\":\"Place\"}]}}}},"
                + "\"nextToken\":\"next-page-token\"}");
    when(invalidResponse.body()).thenReturn("{}");
    doReturn(firstResponse, invalidResponse).when(mockHttp).send(any(), any());

    JsonObject result = ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf");

    assertNull(result);
    verify(mockHttp, org.mockito.Mockito.times(2)).send(any(), any());
  }

  @Test(expected = JsonParseException.class)
  public void fetchPropertyValuesPropagatesMalformedResponse() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    HttpResponse<String> malformedResponse = mock(HttpResponse.class);
    when(malformedResponse.statusCode()).thenReturn(200);
    when(malformedResponse.body()).thenReturn("{invalid-json");
    doReturn(malformedResponse).when(mockHttp).send(any(), any());

    ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf");
  }

  @Test
  public void fetchPropertyValuesRetriesTransientHttpErrors() throws Exception {
    for (int status : List.of(408, 429, 500, 502, 503, 504)) {
      HttpClient mockHttp = mock(HttpClient.class);
      doReturn(apiResponse(status, "<html>Unavailable</html>"), apiResponse(200, "{\"data\":{}}"))
          .when(mockHttp)
          .send(any(), any());

      JsonObject result = ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf");

      assertTrue(result.has("nodeA"));
      verify(mockHttp, org.mockito.Mockito.times(2)).send(any(), any());
    }
  }

  @Test
  public void fetchPropertyValuesStopsAfterFourAttempts() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    doReturn(apiResponse(503, "<html>Unavailable</html>")).when(mockHttp).send(any(), any());

    assertNull(ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf"));
    verify(mockHttp, org.mockito.Mockito.times(4)).send(any(), any());
  }

  @Test
  public void fetchPropertyValuesRejectsNonretryableNon200Responses() throws Exception {
    for (int status : List.of(201, 204, 302, 400, 401, 403, 404, 422, 501, 505)) {
      HttpClient mockHttp = mock(HttpClient.class);
      doReturn(apiResponse(status, "{\"data\":{}}")).when(mockHttp).send(any(), any());

      assertNull(ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf"));
      verify(mockHttp).send(any(), any());
    }
  }

  @Test
  public void fetchPropertyValuesStillRetriesClientTimeouts() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    HttpResponse<String> success = apiResponse(200, "{\"data\":{}}");
    doThrow(new HttpTimeoutException("Timed out"))
        .doReturn(success)
        .when(mockHttp)
        .send(any(), any());

    assertTrue(ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf").has("nodeA"));
    verify(mockHttp, org.mockito.Mockito.times(2)).send(any(), any());
  }

  @Test
  public void fetchPropertyValuesPropagatesTimeoutAfterFourAttempts() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    HttpTimeoutException timeout = new HttpTimeoutException("Timed out");
    doThrow(timeout).when(mockHttp).send(any(), any());

    try {
      ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf");
      fail("Expected timeout to propagate after retries");
    } catch (HttpTimeoutException e) {
      assertSame(timeout, e);
    }
    verify(mockHttp, org.mockito.Mockito.times(4)).send(any(), any());
  }

  @Test
  public void fetchPropertyValuesPropagatesInterruptionWithoutRetrying() throws Exception {
    HttpClient mockHttp = mock(HttpClient.class);
    InterruptedException interruption = new InterruptedException("Interrupted");
    HttpResponse<String> success = apiResponse(200, "{\"data\":{}}");
    doThrow(interruption).doReturn(success).when(mockHttp).send(any(), any());

    try {
      ApiHelper.fetchPropertyValues(mockHttp, List.of("nodeA"), "typeOf");
      fail("Expected interruption to propagate without retrying");
    } catch (InterruptedException e) {
      assertSame(interruption, e);
    } finally {
      Thread.interrupted();
    }
  }

  private static HttpResponse<String> apiResponse(int status, String body) {
    HttpResponse<String> response = mock(HttpResponse.class);
    when(response.statusCode()).thenReturn(status);
    when(response.body()).thenReturn(body);
    return response;
  }

  @Test
  public void convertsNodesWithDcid() throws Exception {
    Map<String, List<V2NodeResponse.NodeInfo>> propertyValuesByNode =
        Map.of("geoId/06", List.of(nodeWithDcid("Class")));

    JsonObject legacy = ApiHelper.convertToLegacyFormat(propertyValuesByNode, List.of("geoId/06"));

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertEquals(1, out.size());
    assertEquals("Class", out.get(0).getAsJsonObject().get("dcid").getAsString());
  }

  @Test
  public void convertsNodesWithValue() throws Exception {
    Map<String, List<V2NodeResponse.NodeInfo>> propertyValuesByNode =
        Map.of("geoId/06", List.of(nodeWithValue("California")));

    JsonObject legacy = ApiHelper.convertToLegacyFormat(propertyValuesByNode, List.of("geoId/06"));

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertEquals(1, out.size());
    assertEquals("California", out.get(0).getAsJsonObject().get("value").getAsString());
  }

  @Test
  public void populatesPlaceholdersWhenNoDataReturned() throws Exception {
    JsonObject legacy = ApiHelper.convertToLegacyFormat(Map.of(), List.of("geoId/06"));

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertTrue(out.isEmpty());
  }

  private static V2NodeResponse.NodeInfo nodeWithDcid(String dcid) {
    V2NodeResponse.NodeInfo nodeInfo = new V2NodeResponse.NodeInfo();
    nodeInfo.dcid = dcid;
    return nodeInfo;
  }

  private static V2NodeResponse.NodeInfo nodeWithValue(String value) {
    V2NodeResponse.NodeInfo nodeInfo = new V2NodeResponse.NodeInfo();
    nodeInfo.value = value;
    return nodeInfo;
  }

  private static JsonObject requestBody(HttpRequest request) throws Exception {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    CompletableFuture<JsonObject> body = new CompletableFuture<>();
    request
        .bodyPublisher()
        .orElseThrow()
        .subscribe(
            new Flow.Subscriber<>() {
              @Override
              public void onSubscribe(Flow.Subscription subscription) {
                subscription.request(Long.MAX_VALUE);
              }

              @Override
              public void onNext(ByteBuffer bytes) {
                byte[] chunk = new byte[bytes.remaining()];
                bytes.get(chunk);
                output.write(chunk, 0, chunk.length);
              }

              @Override
              public void onError(Throwable error) {
                body.completeExceptionally(error);
              }

              @Override
              public void onComplete() {
                body.complete(
                    new Gson().fromJson(output.toString(StandardCharsets.UTF_8), JsonObject.class));
              }
            });
    return body.get();
  }
}
