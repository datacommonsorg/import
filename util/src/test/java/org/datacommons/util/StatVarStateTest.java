package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf.McfGraph;
import org.junit.Test;
import org.mockito.Mockito;

public class StatVarStateTest {
  String TEST_SV_MEASRES_DCID = "SinanYumurtaci_DCIntern2022";
  String TEST_SV_MEASRES_HTTP_RESP =
      String.format(
          "{\"payload\": \"{\\\"%s\\\": {\\\"out\\\":[{\\\"name\\\":\\\"%s\\\"}]}}\" }",
          TEST_SV_MEASRES_DCID, Vocabulary.MEASUREMENT_RESULT);

  @Test
  public void getStatTypeHttpCalls() throws IOException, InterruptedException {
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

  // Test that querying SV described in local MCF returns the correct statType
  // with no HTTP calls.
  //
  // Online behavior is tested in getStatTypeHttpCalls.
  @Test
  public void testAddLocalGraph() throws IOException, InterruptedException {
    var mockHttp = Mockito.mock(HttpClient.class);

    Debug.Log.Builder lb = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(lb, Path.of("InMemory"));
    StatVarState svs = new StatVarState(mockHttp, lw);

    int expected_http_calls = 0;
    String localSvDcid = "Acre_MeasurementResult_StatVar";
    String localSvStatType = "measurementResult";

    String localMcf =
        String.format(
            "Node: dcid:%s\n"
                + "populationType: dcid:Place\n"
                + "statType: dcid:%s\n"
                + "measuredProperty: dcid:area\n"
                + "typeOf: dcid:StatisticalVariable\n",
            localSvDcid, localSvStatType);

    // Add node to local MCF
    McfGraph graph = McfParser.parseInstanceMcfString(localMcf, true, lw);
    svs.addLocalGraph(graph);

    // Query the statType of the node we just asked about
    // Expect the return to be measurementResult, and that no additional http
    // calls were made.
    String resultLocal = svs.getStatType(localSvDcid);
    verify(mockHttp, times(expected_http_calls)).send(any(), any());
    assertEquals(localSvStatType, resultLocal);
  }

  // Test many things that parseApiStatTypeResponse should return null for
  // (i.e. incorrect/invalid inputs)
  @Test
  public void funcParseApiStatTypeResponseNulls() {

    String returnValue;

    // Null inputs, or empty dcid string
    returnValue = StatVarState.parseApiStatTypeResponse(null, "someDcid");
    assertEquals(null, returnValue);

    returnValue = StatVarState.parseApiStatTypeResponse(new JsonObject(), null);
    assertEquals(null, returnValue);

    returnValue = StatVarState.parseApiStatTypeResponse(new JsonObject(), "");
    assertEquals(null, returnValue);

    // Various bad JSON
    String svDcid = "Test_SV";
    List<String> badJsonStrings =
        List.of(
            "{}",
            // dcid is incorrect
            "{\"IncorrectSVDcid\":{\"irrelevant value\": 0}}",
            // direction should be "out"
            String.format("{\"%s\":{\"in\": [{\"irrelevant value\": 0}]}}", svDcid),
            // array should have only 1 element (only one statType per SV)
            String.format("{\"%s\":{\"out\": [{\"name\":\"someValue\"}, {}]}}", svDcid),
            // object does not have "value" field
            String.format("{\"%s\":{\"out\": [{\"provenance\":\"datacommons.org\"}]}}", svDcid));
    JsonParser parser = new JsonParser();
    JsonObject badInput;
    String assertionFailedMessage;
    for (String badJsonString : badJsonStrings) {
      badInput = parser.parse(badJsonString).getAsJsonObject();

      returnValue = StatVarState.parseApiStatTypeResponse(badInput, svDcid);
      assertionFailedMessage =
          "parseApiStatTypeResponse response to JSON "
              + badJsonString
              + " was expected to be null but was "
              + returnValue;
      assertEquals(assertionFailedMessage, null, returnValue);
    }

    // Valid case; test that the JSON object in the string contained in TEST_SV_MEASRES_HTTP_RESP
    // is correctly parsed and Vocabulary.MEASUREMENT_RESULT is returned.
    String innerJsonString =
        parser.parse(TEST_SV_MEASRES_HTTP_RESP).getAsJsonObject().get("payload").getAsString();
    JsonObject test_sv_measres_api_payload = parser.parse(innerJsonString).getAsJsonObject();
    returnValue =
        StatVarState.parseApiStatTypeResponse(test_sv_measres_api_payload, TEST_SV_MEASRES_DCID);
    assertEquals(Vocabulary.MEASUREMENT_RESULT, returnValue);
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
