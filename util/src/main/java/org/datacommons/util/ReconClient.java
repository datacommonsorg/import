package org.datacommons.util;

import static com.google.common.collect.Lists.partition;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.util.stream.Collectors.toList;
import static org.datacommons.util.FuturesUtil.toFutureOfList;
import static org.datacommons.util.StringUtil.toJson;

import com.google.protobuf.Message;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.concurrent.CompletableFuture;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;
import org.datacommons.proto.Recon.ResolveEntitiesRequest;
import org.datacommons.proto.Recon.ResolveEntitiesResponse;

/**
 * Client to the DC resolution APIs.
 *
 * <p>Currently it only resolves coordinates.
 */
public class ReconClient {
  private static final String RESOLVE_COORDINATES_API_URL =
      "https://api.datacommons.org/v1/recon/resolve/coordinate";

  private static final String RESOLVE_ENTITIES_API_URL =
      "https://api.datacommons.org/v1/recon/entity/resolve";

  static final String NUM_API_CALLS_COUNTER = "ReconClient_NumApiCalls";

  private static final int DEFAULT_CHUNK_SIZE = 500;

  private final int chunkSize;

  private final HttpClient httpClient;

  private final LogWrapper logWrapper;

  public ReconClient(HttpClient httpClient, LogWrapper logWrapper) {
    this(httpClient, logWrapper, DEFAULT_CHUNK_SIZE);
  }

  public ReconClient(HttpClient httpClient, LogWrapper logWrapper, int chunkSize) {
    this.httpClient = httpClient;
    this.logWrapper = logWrapper;
    this.chunkSize = chunkSize;
  }

  public CompletableFuture<ResolveCoordinatesResponse> resolveCoordinates(
      ResolveCoordinatesRequest request) {
    ResolveCoordinatesResponse defaultResponse = ResolveCoordinatesResponse.getDefaultInstance();
    if (request.getCoordinatesCount() < 1) {
      return CompletableFuture.completedFuture(defaultResponse);
    }

    return toFutureOfList(
            partition(request.getCoordinatesList(), chunkSize).stream()
                .map(
                    chunk ->
                        request.toBuilder().clearCoordinates().addAllCoordinates(chunk).build())
                .map(
                    chunkedRequest ->
                        callApi(RESOLVE_COORDINATES_API_URL, chunkedRequest, defaultResponse))
                .collect(toList()))
        .thenApply(
            chunkedResponses ->
                ResolveCoordinatesResponse.newBuilder()
                    .addAllPlaceCoordinates(
                        chunkedResponses.stream()
                            .flatMap(
                                chunkedResponse ->
                                    chunkedResponse.getPlaceCoordinatesList().stream())
                            .collect(toList()))
                    .build());
  }

  public CompletableFuture<ResolveEntitiesResponse> resolveEntities(
      ResolveEntitiesRequest request) {
    ResolveEntitiesResponse defaultResponse = ResolveEntitiesResponse.getDefaultInstance();
    if (request.getEntitiesCount() < 1) {
      return CompletableFuture.completedFuture(defaultResponse);
    }

    return toFutureOfList(
            partition(request.getEntitiesList(), chunkSize).stream()
                .map(chunk -> request.toBuilder().clearEntities().addAllEntities(chunk).build())
                .map(
                    chunkedRequest ->
                        callApi(RESOLVE_ENTITIES_API_URL, chunkedRequest, defaultResponse))
                .collect(toList()))
        .thenApply(
            chunkedResponses ->
                ResolveEntitiesResponse.newBuilder()
                    .addAllResolvedEntities(
                        chunkedResponses.stream()
                            .flatMap(
                                chunkedResponse ->
                                    chunkedResponse.getResolvedEntitiesList().stream())
                            .collect(toList()))
                    .build());
  }

  private <T extends Message> CompletableFuture<T> callApi(
      String apiUrl, Message requestMessage, T responseDefaultInstance) {
    logWrapper.incrementInfoCounterBy(NUM_API_CALLS_COUNTER, 1);
    HttpRequest request =
        HttpRequest.newBuilder(URI.create(apiUrl))
            .version(HTTP_1_1)
            .header("accept", "application/json")
            .POST(BodyPublishers.ofString(toJson(requestMessage)))
            .build();
    return httpClient
        .sendAsync(request, BodyHandlers.ofString())
        .thenApply(
            response -> {
              Message.Builder responseMessageBuilder = responseDefaultInstance.newBuilderForType();
              StringUtil.fromJson(response.body().trim(), responseMessageBuilder);
              return (T) responseMessageBuilder.build();
            });
  }
}
