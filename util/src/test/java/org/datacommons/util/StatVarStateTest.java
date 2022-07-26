package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import org.datacommons.proto.Debug;
import org.junit.Test;
import org.mockito.Mockito;

public class StatVarStateTest {
  @Test
  // Also exercises the private function fetchStatTypeFromApi(), used underneath
  // getStatType for SVs that we don't know about yet
  public void funcGetStatType() throws IOException, InterruptedException {
    String TEST_SV_MEASRES_DCID = "SinanYumurtaci_DCIntern2022";
    String TEST_SV_MEASRES_HTTP_RESP =
        "{\"payload\": \"{\\\""
            + TEST_SV_MEASRES_DCID
            + "\\\": {\\\"out\\\":[{\\\"value\\\":\\\""
            + Vocabulary.MEASUREMENT_RESULT
            + "\\\"}]}}\" }";

    // Common setup
    var mockHttp = Mockito.mock(HttpClient.class);
    var mockResp = Mockito.mock(HttpResponse.class);
    when(mockHttp.send(any(), any())).thenReturn(mockResp);

    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));

    var svs = new StatVarState(mockHttp, lw);
    int expected_http_calls = 0;

    // No http calls before calling any .getStatType()
    verify(mockHttp, times(expected_http_calls)).send(any(), any());

    // Calling with unknown DCID triggers call to HttpClient
    when(mockResp.body()).thenReturn(TEST_SV_MEASRES_HTTP_RESP);
    String result = svs.getStatType(TEST_SV_MEASRES_DCID);
    expected_http_calls++;
    verify(mockHttp, times(expected_http_calls)).send(any(), any());

    // Response is parsed correctly
    assertEquals(Vocabulary.MEASUREMENT_RESULT, result);

    // Call again to same DCID does not trigger another http call
    String result_repeat = svs.getStatType(TEST_SV_MEASRES_DCID);
    verify(mockHttp, times(expected_http_calls)).send(any(), any());

    // And the responses from the two calls are equivalent
    assertEquals(result, result_repeat);
  }

  @Test
  public void funcCheckDuplicateDcid() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("."));
    StatVarState svs = new StatVarState(lw);
    check(
        svs,
        List.of(
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n",
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    gender: schema:Male        \n"
                + "    statType: dcs:measuredValue\n"));
    assertTrue(
        TestUtil.checkLog(lw.getLog(), "Sanity_SameDcidForDifferentStatVars", "Count_Person"));
  }

  @Test
  public void funcCheckDuplicateSVContent() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("."));
    StatVarState svs = new StatVarState(lw);
    check(
        svs,
        List.of(
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n",
            "    Node: dcid:Count_Person_Male\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n"));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Sanity_DifferentDcidsForSameStatVar",
            "dcid1: 'Count_Person', dcid2: 'Count_Person_Male'"));
  }

  public static void check(StatVarState svs, List<String> nodeStrings) throws IOException {
    for (var nodeString : nodeStrings) {
      var g = TestUtil.graphFromMcf(nodeString);
      for (var kv : g.getNodesMap().entrySet()) {
        svs.check(kv.getKey(), kv.getValue());
      }
    }
  }
}
