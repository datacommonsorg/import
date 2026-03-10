package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.http.HttpRequest;
import java.util.Map;
import org.datacommons.proto.Resolve.ResolveRequest;
import org.junit.Test;

public class ReconClientRequestTest {
  private static final String API_URL = "https://api.datacommons.org/v2/resolve";

  @Test
  public void buildRequestAddsApiKeyHeaderWhenAvailable() {
    HttpRequest request =
        ReconClient.buildRequest(API_URL, newRequest(), Map.of("DC_API_KEY", "prod-key"));

    assertEquals(API_URL, request.uri().toString());
    assertEquals("prod-key", request.headers().firstValue("x-api-key").orElse(""));
  }

  @Test
  public void buildRequestOmitsApiKeyHeaderWhenMissing() {
    HttpRequest request = ReconClient.buildRequest(API_URL, newRequest(), Map.of());

    assertEquals(API_URL, request.uri().toString());
    assertTrue(request.headers().firstValue("x-api-key").isEmpty());
  }

  private static ResolveRequest newRequest() {
    return ResolveRequest.newBuilder().addNodes("37.77493#-122.41942").setProperty("name").build();
  }
}
