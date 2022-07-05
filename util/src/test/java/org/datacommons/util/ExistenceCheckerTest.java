package org.datacommons.util;

import static org.datacommons.util.LogCb.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
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

    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExistenceChecker.DC_CALL_BATCH_LIMIT = 1;

    var checker = new ExistenceChecker(mockHttp, false, lw);

    assertFalse(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 1));

    // Non-existing node, response from RPC.
    when(mockResp.body()).thenReturn(NONEXISTING_LAT);
    checker.submitNodeCheck("latitude", newLogCb(lw, PREF_KEY, "latitude1"));
    verify(mockHttp, times(1)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 1));

    // Non-existing node must be cached. No further RPCs.
    checker.submitNodeCheck("latitude", newLogCb(lw, PREF_KEY, "latitude2"));
    verify(mockHttp, times(1)).send(any(), any());
    // Two missing-ref counts.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 2));

    // Existing node, response from RPC.
    when(mockResp.body()).thenReturn(EXISTING_GENDER);
    checker.submitNodeCheck("gender", newLogCb(lw, PREF_KEY, "gender1"));
    verify(mockHttp, times(2)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 2));

    // Existing node must be cached. No further RPCs.
    checker.submitNodeCheck("gender", newLogCb(lw, PREF_KEY, "gender2"));
    verify(mockHttp, times(2)).send(any(), any());
    // Same missing-ref count as before.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 2));

    // Local KG hit. No further RPCs.
    checker.addLocalGraph(TestUtil.graphFromMcf(LOCAL_KG_NODE));
    checker.submitNodeCheck("latitude", newLogCb(lw, PREF_KEY, "latitude3"));
    verify(mockHttp, times(2)).send(any(), any());
    // Same missing-ref count as before.
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 2));
  }

  @Test
  public void testTriple() throws IOException, InterruptedException {
    var mockHttp = Mockito.mock(HttpClient.class);
    var mockResp = Mockito.mock(HttpResponse.class);
    when(mockHttp.send(any(), any())).thenReturn(mockResp);

    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExistenceChecker.DC_CALL_BATCH_LIMIT = 1;

    var checker = new ExistenceChecker(mockHttp, false, lw);

    assertFalse(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 0));

    // Non-existing triple, response from RPC.
    when(mockResp.body()).thenReturn(NONEXISTING_LAT);
    checker.submitTripleCheck(
        "latitude", "rangeIncludes", "Place", newLogCb(lw, SUB_KEY, "latitude"));
    verify(mockHttp, times(1)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 1));

    // Non-existing triple response must be cached. No further RPCs.
    checker.submitTripleCheck(
        "latitude", "rangeIncludes", "Place", newLogCb(lw, SUB_KEY, "latitude"));
    verify(mockHttp, times(1)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 2));

    // Existing triple, response from RPC.
    when(mockResp.body()).thenReturn(EXISTING_GENDER);
    checker.submitTripleCheck(
        "gender", "domainIncludes", "Person", newLogCb(lw, SUB_KEY, "gender"));
    verify(mockHttp, times(2)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 2));

    // Existing triple again, must be cached.
    when(mockResp.body()).thenReturn(EXISTING_GENDER);
    checker.submitTripleCheck(
        "gender", "domainIncludes", "Person", newLogCb(lw, SUB_KEY, "gender"));
    verify(mockHttp, times(2)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 2));

    // Local KG hit. Must be cached.
    checker.addLocalGraph(TestUtil.graphFromMcf(LOCAL_KG_NODE));
    checker.submitTripleCheck(
        "latitude", "rangeIncludes", "Place", newLogCb(lw, SUB_KEY, "latitude"));
    verify(mockHttp, times(2)).send(any(), any());
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 2));
  }

  @Test
  public void endToEnd() throws IOException, InterruptedException {
    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    ExistenceChecker.DC_CALL_BATCH_LIMIT = 5;

    var checker = new ExistenceChecker(HttpClient.newHttpClient(), false, lw);

    // Ref 1. Exists
    checker.submitNodeCheck("gender", newLogCb(lw, VREF_KEY, "gender"));
    // Ref 2. Missing
    checker.submitNodeCheck("Nope", newLogCb(lw, VREF_KEY, "Nope"));
    // Tripe. Exists
    checker.submitTripleCheck(
        "gender", "domainIncludes", "Person", newLogCb(lw, PRED_KEY, "domainIncludes"));
    // Triple. Exists
    checker.submitTripleCheck(
        "gender", "rangeIncludes", "GenderType", newLogCb(lw, PRED_KEY, "rangeIncludes"));
    // Triple. Missing
    checker.submitTripleCheck(
        "gender", "subPropertyOf", "notReally", newLogCb(lw, PRED_KEY, "subPropertyOf"));
    // Ref 3. Exists
    checker.submitNodeCheck("Count_Person", newLogCb(lw, VREF_KEY, "Count_Person"));
    // Ref 4. Exists
    checker.submitNodeCheck("minimumWage", newLogCb(lw, VREF_KEY, "minimumWage"));

    // Up till this point no call should have been made.
    assertFalse(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 1));
    assertFalse(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 1));

    // Ref 5. Missing
    checker.submitNodeCheck("UnemploymentRate", newLogCb(lw, VREF_KEY, "UnemploymentRate"));

    // Since we have issued 5 ref-checks (all, typeOf), we should now see 2 missing refs.
    assertFalse(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 1));
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingReference", 2));

    // After we drain all, we should see the one missing triple.
    checker.drainRemoteCalls();
    assertTrue(TestUtil.checkCounter(lw.getLog(), "Existence_MissingTriple", 1));

    // Lets also confirm the exact missing ones.
    assertTrue(TestUtil.checkLog(lw.getLog(), "Existence_MissingReference", "Nope"));
    assertTrue(TestUtil.checkLog(lw.getLog(), "Existence_MissingReference", "UnemploymentRate"));
    assertTrue(TestUtil.checkLog(lw.getLog(), "Existence_MissingTriple", "subPropertyOf"));
  }

  private static LogCb newLogCb(LogWrapper lw, String key, String value) {
    var dummyNode = Mcf.McfGraph.PropertyValues.newBuilder().build();
    return new LogCb(lw, Debug.Log.Level.LEVEL_WARNING, dummyNode).setDetail(key, value);
  }
}
