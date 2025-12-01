package org.datacommons.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

// This class is a collection of static methods to help with communicating with
// the Data Commons API.
public class ApiHelper {
  private static final Logger logger = LogManager.getLogger(ApiHelper.class);

  // Use the autopush end-point so we get more recent schema additions that
  // haven't rolled out.
  private static final String API_ROOT =
      "https://autopush.api.datacommons.org/v2/node";
  private static final String API_KEY = System.getenv("DC_API_KEY");

  // Retry configuration
  private static boolean ENABLE_RETRIES = true;
  private static int MAX_RETRIES = 3;
  private static int INITIAL_RETRY_DELAY_SECONDS = 1;
  private static int MAX_RETRY_DELAY_SECONDS = 8;

  // calls the Data Commons API to get the given property for the given list of nodes
  // if the call is succesful, returns the contents of the "payload" field as a JsonObject
  // if the call fails, or the response does not have a "payload" field, returns null.
  // API documentation: https://docs.datacommons.org/api/rest/property_value.html
  public static JsonObject fetchPropertyValues(
      HttpClient httpClient, List<String> nodes, String property)
      throws IOException, InterruptedException {

    JsonArray dcids = new JsonArray();
    for (var node : nodes) {
      dcids.add(node);
    }

    JsonObject arg = new JsonObject();
    arg.add("nodes", dcids);
    // V2 uses -> for out-edges, which is equivalent to direction: "out" in V1
    arg.addProperty("property", "->" + property);

    var requestBuilder =
        HttpRequest.newBuilder(URI.create(API_ROOT))
            .version(HttpClient.Version.HTTP_1_1)
            .header("accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(arg.toString()));
    if (API_KEY != null && !API_KEY.isEmpty()) {
      requestBuilder.header("x-api-key", API_KEY);
    }
    var request = requestBuilder.build();

    // maxRetries = 0 means no retries (only initial attempt)
    // maxRetries = 3 means 4 total attempts (1 initial + 3 retries)
    int maxRetries = ENABLE_RETRIES ? MAX_RETRIES : 0;

    RetryPolicy<HttpResponse<String>> retryPolicy =
        RetryPolicy.<HttpResponse<String>>builder()
            .handle(IOException.class)
            .withMaxRetries(maxRetries)
            // Exponential backoff: 1s, 2s, 4s, 8s, 8s... (doubles each retry, capped at
            // MAX_RETRY_DELAY_SECONDS)
            .withBackoff(
                Duration.ofSeconds(INITIAL_RETRY_DELAY_SECONDS),
                Duration.ofSeconds(MAX_RETRY_DELAY_SECONDS))
            .onRetry(
                event -> {
                  logger.warn(
                      "API call failed (attempt "
                          + (event.getAttemptCount() + 1)
                          + "/"
                          + (maxRetries + 1)
                          + "), retrying: "
                          + event.getLastException().getMessage());
                })
            .build();

    var response =
        Failsafe.with(retryPolicy)
            .get(
                () -> {
                  return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
                });

    var payloadJson = new JsonParser().parse(response.body().trim()).getAsJsonObject();
    if (payloadJson == null || !payloadJson.has("data")) return null;
    // V2 Node API returns data directly in the "data" field, no need to parse a nested string.
    // We need to transform it back to the expected format for existing callers,
    // or update callers. Given the current structure, it's easier to transform it here
    // to match what the callers expect, or update the callers.
    // Old format expected by callers (from parseApiStatTypeResponse and ExistenceChecker):
    // { "dcid1": { "out": [ { "dcid": "val1" }, { "dcid": "val2" } ] } }
    // V2 format:
    // { "data": { "dcid1": { "arcs": { "prop": { "nodes": [ { "dcid": "val1" }, { "value": "val2" } ] } } } } }

    JsonObject legacyFormat = new JsonObject();
    JsonObject data = payloadJson.getAsJsonObject("data");
    for (String dcid : data.keySet()) {
      JsonObject nodeData = data.getAsJsonObject(dcid);
      if (nodeData.has("arcs")) {
        JsonObject arcs = nodeData.getAsJsonObject("arcs");
        if (arcs.has(property)) {
          JsonObject propData = arcs.getAsJsonObject(property);
          if (propData.has("nodes")) {
            JsonArray nodesArray = propData.getAsJsonArray("nodes");
            JsonArray outArray = new JsonArray();
            for (int i = 0; i < nodesArray.size(); i++) {
              JsonObject node = nodesArray.get(i).getAsJsonObject();
              JsonObject outObj = new JsonObject();
              if (node.has("dcid")) {
                outObj.addProperty("dcid", node.get("dcid").getAsString());
              } else if (node.has("value")) {
                // For property values, V1 used "value" or "dcid" depending on type.
                // V2 also uses "value" or "dcid".
                outObj.addProperty("value", node.get("value").getAsString());
                // Also add as dcid for compatibility if needed, but usually it's one or the other.
                // ExistenceChecker looks for "dcid" in checkOneResult for triples.
                // Let's check ExistenceChecker again.
              }
              outArray.add(outObj);
            }
            JsonObject outWrapper = new JsonObject();
            outWrapper.add("out", outArray);
            legacyFormat.add(dcid, outWrapper);
          }
        }
      }
    }
    return legacyFormat;
  }
}
