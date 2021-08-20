package org.datacommons.util;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datacommons.proto.Mcf;

// This class checks the existence of typically schema-related, nodes or (select types of)
// triples in the KG or local graph.
// TODO: Consider changing callers to batch calls.
public class ExistenceChecker {
  // Use the staging end-point to not impact prod.
  private static final String API_ROOT = "https://staging.api.datacommons.org/node/property-values";
  // For now we only need checks for certain Property/Class props.
  private static final Set<String> SCHEMA_PROPERTIES =
      Set.of(Vocabulary.DOMAIN_INCLUDES, Vocabulary.RANGE_INCLUDES, Vocabulary.SUB_CLASS_OF);
  // Useful for mocking.
  private HttpClient httpClient;

  // This is a combination of local KG data and prior cached checks.
  // Node is just the DCID. Triple is "s,p,o" and the property just includes CACHED_PROPERTIES.
  private Set<String> existingNodesOrTriples; // Existence cache
  private Set<String> missingNodesOrTriples; // Absence cache
  private LogWrapper logCtx;

  public ExistenceChecker(HttpClient httpClient, LogWrapper logCtx) {
    this.httpClient = httpClient;
    this.logCtx = logCtx;
    existingNodesOrTriples = new HashSet<>();
    missingNodesOrTriples = new HashSet<>();
  }

  public void addLocalGraph(Mcf.McfGraph graph) {
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      // Skip doing anything with StatVarObs.
      String typeOf = McfUtil.getPropVal(node.getValue(), Vocabulary.TYPE_OF);
      if (typeOf.equals(Vocabulary.STAT_VAR_OBSERVATION_TYPE)
          || typeOf.equals(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX)) {
        continue;
      }

      String dcid = McfUtil.getPropVal(node.getValue(), Vocabulary.DCID);
      if (dcid.isEmpty()) {
        continue;
      }

      existingNodesOrTriples.add(dcid);
      if (missingNodesOrTriples.contains(dcid)) {
        missingNodesOrTriples.remove(dcid);
      }

      if (!typeOf.equals(Vocabulary.CLASS_TYPE) && !typeOf.equals(Vocabulary.PROPERTY_TYPE)) {
        continue;
      }
      for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getValue().getPvsMap().entrySet()) {
        if (SCHEMA_PROPERTIES.contains(pv.getKey())) {
          for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
            var key = makeKey(dcid, pv.getKey(), tv.getValue());
            existingNodesOrTriples.add(key);
            if (missingNodesOrTriples.contains(key)) {
              missingNodesOrTriples.remove(key);
            }
          }
        }
      }
    }
  }

  public boolean checkNode(String node) throws IOException, InterruptedException {
    return checkCommon(node, Vocabulary.TYPE_OF, "", node);
  }

  public boolean checkTriple(String sub, String pred, String obj)
      throws IOException, InterruptedException {
    return checkCommon(sub, pred, obj, makeKey(sub, pred, obj));
  }

  public boolean checkCommon(String sub, String pred, String obj, String key)
      throws IOException, InterruptedException {
    logCtx.incrementCounterBy("Existence_NumChecks", 1);
    if (existingNodesOrTriples.contains(key)) return true;
    if (missingNodesOrTriples.contains(key)) return false;

    var dataJson = callDc(sub, pred);
    logCtx.incrementCounterBy("Existence_NumDcCalls", 1);
    if (dataJson == null) {
      throw new IOException("Calls to the Staging DC API endpoint are failing!");
    }

    var nodeJson = dataJson.getAsJsonObject(sub);
    if (nodeJson.has("out")) {
      if (obj.isEmpty()) {
        // Node existence check case.
        if (nodeJson.getAsJsonArray("out").size() > 0) {
          existingNodesOrTriples.add(key);
          return true;
        }
      } else {
        // Triple existence check case.
        for (var objVal : nodeJson.getAsJsonArray("out")) {
          if (objVal.getAsJsonObject().getAsJsonPrimitive("dcid").getAsString().equals(obj)) {
            existingNodesOrTriples.add(key);
            return true;
          }
        }
      }
    }
    missingNodesOrTriples.add(key);
    return false;
  }

  private JsonObject callDc(String node, String property) throws IOException, InterruptedException {
    List<String> args =
        List.of(
            "dcids=" + URLEncoder.encode(node, StandardCharsets.UTF_8),
            "property=" + URLEncoder.encode(property, StandardCharsets.UTF_8),
            "direction=out");
    var url = API_ROOT + "?" + String.join("&", args);
    var request =
        HttpRequest.newBuilder(URI.create(url)).header("accept", "application/json").build();
    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    var payloadJson = new JsonParser().parse(response.body()).getAsJsonObject();
    if (payloadJson == null) return null;
    return new JsonParser().parse(payloadJson.get("payload").getAsString()).getAsJsonObject();
  }

  private static String makeKey(String s, String p, String o) {
    return s + "," + p + "," + o;
  }
}
