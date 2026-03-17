package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.http.HttpRequest;
import org.datacommons.proto.Resolve.ResolveRequest;
import org.junit.Test;

public class ReconClientRequestTest {
  @Test
  public void buildRequestDefaultsToProd() {
    HttpRequest request =
        ReconClient.buildRequest(
            newRequest(), new DcApiConfig("https://api.datacommons.org", "prod-key"));

    assertEquals("https://api.datacommons.org/v2/resolve", request.uri().toString());
    assertEquals("prod-key", request.headers().firstValue("x-api-key").orElse(""));
  }

  @Test
  public void buildRequestUsesExplicitRoot() {
    HttpRequest request =
        ReconClient.buildRequest(
            newRequest(), new DcApiConfig("https://custom.api.datacommons.org/", "key"));

    assertEquals("https://custom.api.datacommons.org/v2/resolve", request.uri().toString());
    assertEquals("key", request.headers().firstValue("x-api-key").orElse(""));
  }

  @Test
  public void buildRequestOmitsApiKeyHeaderWhenMissing() {
    HttpRequest request =
        ReconClient.buildRequest(newRequest(), new DcApiConfig("https://api.datacommons.org", ""));

    assertEquals("https://api.datacommons.org/v2/resolve", request.uri().toString());
    assertTrue(request.headers().firstValue("x-api-key").isEmpty());
  }

  private static ResolveRequest newRequest() {
    return ResolveRequest.newBuilder().addNodes("37.77493#-122.41942").setProperty("name").build();
  }
}
