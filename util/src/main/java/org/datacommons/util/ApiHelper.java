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
      "https://autopush.api.datacommons.org/node/property-values";

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
    arg.add("dcids", dcids);
    arg.addProperty("property", property);
    arg.addProperty("direction", "out");

    var request =
        HttpRequest.newBuilder(URI.create(API_ROOT))
            .version(HttpClient.Version.HTTP_1_1)
            .header("accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(arg.toString()))
            .build();

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
    if (payloadJson == null || !payloadJson.has("payload")) return null;
    // The API returns the actual response in a JSON-serialized string.
    // This string is in the "payload" field of the raw response.
    return new JsonParser().parse(payloadJson.get("payload").getAsString()).getAsJsonObject();
  }
}
