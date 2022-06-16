package org.datacommons.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.*;

public class ApiHelper {
  // Use the autopush end-point so we get more recent schema additions that
  // haven't rolled out.
  private static final String API_ROOT =
      "https://autopush.api.datacommons.org/node/property-values";

  public static JsonObject callDc(HttpClient httpClient, List<String> nodes, String property)
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

    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    var payloadJson = new JsonParser().parse(response.body().trim()).getAsJsonObject();
    if (payloadJson == null || !payloadJson.has("payload")) return null;
    return new JsonParser().parse(payloadJson.get("payload").getAsString()).getAsJsonObject();
  }
}
