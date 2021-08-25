package org.datacommons.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.Test;
import org.mockito.Mockito;

public class ExistenceCheckerTest {
  private static final String NONEXISTING_LAT = "{ \"payload\": \"{\\\"latitude\\\":{}}\" }";
  private static final String EXISTING_GENDER =
      "{ \"payload\": \"{\\\"gender\\\":{\\\"out"
          + "\\\":[{\\\"dcid\\\":\\\"Person\\\",\\\"name\\\":\\\"Person\\\","
          + "\\\"provenanceId\\\":\\\"dc/5l5zxr1\\\",\\\"types\\\":[\\\"Class\\\"]}]}}\" }";
  private static final String LOCAL_KG_NODE =
      "Node: dcid:latitude\n"
          + "typeOf: schema:Property\n"
          + "name: \"latitude\"\n"
          + "rangeIncludes: schema:Place\n";

  @Test
  public void testNode() throws IOException, InterruptedException {
    var mockHttp = Mockito.mock(HttpClient.class);
    var mockResp = Mockito.mock(HttpResponse.class);
    when(mockHttp.send(any(), any())).thenReturn(mockResp);

    var checker = new ExistenceChecker(mockHttp, TestUtil.newLogCtx("inmem"));

    // Caching for miss.
    when(mockResp.body()).thenReturn(NONEXISTING_LAT);
    assertFalse(checker.checkNode("latitude"));
    verify(mockHttp, times(1)).send(any(), any());
    assertFalse(checker.checkNode("latitude"));
    verify(mockHttp, times(1)).send(any(), any());

    // Caching for hit.
    when(mockResp.body()).thenReturn(EXISTING_GENDER);
    assertTrue(checker.checkNode("gender"));
    verify(mockHttp, times(2)).send(any(), any());
    assertTrue(checker.checkNode("gender"));
    verify(mockHttp, times(2)).send(any(), any());

    // Local KG hit.
    checker.addLocalGraph(TestUtil.graphFromMcf(LOCAL_KG_NODE));
    assertTrue(checker.checkNode("latitude"));
  }

  @Test
  public void testTriple() throws IOException, InterruptedException {
    var mockHttp = Mockito.mock(HttpClient.class);
    var mockResp = Mockito.mock(HttpResponse.class);
    when(mockHttp.send(any(), any())).thenReturn(mockResp);

    var checker = new ExistenceChecker(mockHttp, TestUtil.newLogCtx("inmem"));

    when(mockResp.body()).thenReturn(NONEXISTING_LAT);
    assertFalse(checker.checkTriple("latitude", "rangeIncludes", "Place"));
    assertFalse(checker.checkTriple("latitude", "rangeIncludes", "Place"));
    verify(mockHttp, times(1)).send(any(), any());

    when(mockResp.body()).thenReturn(EXISTING_GENDER);
    assertTrue(checker.checkTriple("gender", "domainIncludes", "Person"));
    assertTrue(checker.checkTriple("gender", "domainIncludes", "Person"));
    verify(mockHttp, times(2)).send(any(), any());

    // Local KG hit.
    checker.addLocalGraph(TestUtil.graphFromMcf(LOCAL_KG_NODE));
    assertTrue(checker.checkTriple("latitude", "rangeIncludes", "Place"));
  }

  @Test
  public void endToEnd() throws IOException, InterruptedException {
    var checker = new ExistenceChecker(HttpClient.newHttpClient(), TestUtil.newLogCtx("inmem"));
    assertTrue(checker.checkNode("gender"));
    assertFalse(checker.checkNode("Nope"));
    assertTrue(checker.checkTriple("gender", "domainIncludes", "Person"));
    assertTrue(checker.checkTriple("gender", "rangeIncludes", "GenderType"));
    assertTrue(checker.checkNode("Count_Person"));
    assertTrue(checker.checkNode("minimumWage"));
    assertFalse(checker.checkNode("UnemploymentRate")); // must be unemploymentRate
  }
}
