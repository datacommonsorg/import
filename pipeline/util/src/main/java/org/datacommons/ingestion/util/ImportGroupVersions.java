package org.datacommons.ingestion.util;

import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;

/** Contains functions that returns import group versions. */
public class ImportGroupVersions {
  private static final Gson gson = new Gson();

  /**
   * Returns import group versions by calling the specified DC version endpoint like
   * https://autopush.api.datacommons.org/version
   */
  public static List<String> getImportGroupVersions(String versionEndpoint) {
    var request = HttpRequest.newBuilder().uri(URI.create(versionEndpoint)).GET().build();

    try {
      var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());

      if (response.statusCode() != HttpURLConnection.HTTP_OK) {
        throw new RuntimeException(
            String.format(
                "Versions request failed with status code: %d (%s)",
                response.statusCode(), versionEndpoint));
      }

      return gson.fromJson(response.body(), VersionResponse.class).getImportGroupVersions();

    } catch (Exception e) {
      throw new RuntimeException(
          String.format("Failed to get import group versions: %s", versionEndpoint), e);
    }
  }

  public static String getImportGroupName(String importGroupVersion) {
    int versionSeparatorIndex = importGroupVersion.indexOf("_");
    return versionSeparatorIndex == -1
        ? importGroupVersion
        : importGroupVersion.substring(0, versionSeparatorIndex);
  }

  /**
   * Represents the response from DC version endpoints like
   * https://autopush.api.datacommons.org/version
   */
  static class VersionResponse {
    // The tables array in the response encodes all import group versions served by that endpoint.
    @SerializedName("tables")
    private List<String> importGroupVersions;

    public List<String> getImportGroupVersions() {
      return importGroupVersions;
    }

    public void setImportGroupVersions(List<String> importGroupVersions) {
      this.importGroupVersions = importGroupVersions;
    }
  }
}
