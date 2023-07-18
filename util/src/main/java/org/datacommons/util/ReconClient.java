package org.datacommons.util;

import static java.net.http.HttpClient.Version.HTTP_1_1;

import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;

/**
 * Client to the DC resolution APIs.
 *
 * <p>Currently it only resolves coordinates.
 */
public class ReconClient {
  private static final String RESOLVE_COORDINATES_API_URL =
      "https://api.datacommons.org/v1/recon/resolve/coordinate";

  private final HttpClient httpClient;

  public ReconClient(HttpClient httpClient) {
    this.httpClient = httpClient;
  }

  public ResolveCoordinatesResponse resolveCoordinates(ResolveCoordinatesRequest request)
      throws IOException, InterruptedException {
    return callApi(
        RESOLVE_COORDINATES_API_URL, request, ResolveCoordinatesResponse.getDefaultInstance());
  }

  private <T extends Message> T callApi(
      String apiUrl, Message requestMessage, T responseDefaultInstance)
      throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder(URI.create(apiUrl))
            .version(HTTP_1_1)
            .header("accept", "application/json")
            .POST(BodyPublishers.ofString(StringUtil.msgToJson(requestMessage)))
            .build();
    var response = httpClient.send(request, BodyHandlers.ofString());
    var responseMessageBuilder = responseDefaultInstance.newBuilderForType();
    JsonFormat.parser().merge(response.body().trim(), responseMessageBuilder);
    return (T) responseMessageBuilder.build();
  }
}
