package org.datacommons.util;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
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
  private static final String API_ROOT = "https://autopush.api.datacommons.org/v2/node";
  private static final String API_KEY = System.getenv("AUTOPUSH_DC_API_KEY");

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

    V2NodeResponse v2Response = new Gson().fromJson(response.body().trim(), V2NodeResponse.class);
    if (v2Response == null || v2Response.data == null) return null;

    return convertToLegacyFormat(v2Response, nodes, property);
  }

  static JsonObject convertToLegacyFormat(
      V2NodeResponse v2Response, List<String> nodes, String property) {
    JsonObject legacyFormat = new JsonObject();
    // Iterate over requested nodes to ensure each has an entry in the response,
    // even if empty. This is required by callers like ExistenceChecker.
    for (String dcid : nodes) {
      JsonObject outWrapper = new JsonObject();
      JsonArray outArray = new JsonArray();
      outWrapper.add("out", outArray);
      legacyFormat.add(dcid, outWrapper);

      if (v2Response.data.containsKey(dcid)) {
        populateOutArray(v2Response.data.get(dcid), property, outArray);
      }
    }
    return legacyFormat;
  }

  private static void populateOutArray(
      V2NodeResponse.NodeData nodeData, String property, JsonArray outArray) {
    if (nodeData != null && nodeData.arcs != null && nodeData.arcs.containsKey(property)) {
      V2NodeResponse.ArcData arcData = nodeData.arcs.get(property);
      if (arcData != null && arcData.nodes != null) {
        for (V2NodeResponse.NodeInfo node : arcData.nodes) {
          outArray.add(createOutObject(node));
        }
      }
    }
  }

  private static JsonObject createOutObject(V2NodeResponse.NodeInfo node) {
    JsonObject outObj = new JsonObject();
    if (node.dcid != null) {
      outObj.addProperty("dcid", node.dcid);
    } else if (node.value != null) {
      outObj.addProperty("value", node.value);
    }
    return outObj;
  }
}
