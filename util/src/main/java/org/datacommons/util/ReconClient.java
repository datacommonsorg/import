package org.datacommons.util;

import static com.google.common.collect.Lists.partition;
import static java.net.http.HttpClient.Version.HTTP_1_1;
import static java.util.stream.Collectors.toList;
import static org.datacommons.util.StringUtil.msgToJson;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;

/**
 * Client to the DC resolution APIs.
 *
 * <p>Currently it only resolves coordinates. If the number of coordinates to resolve are greater
 * than {@code chunkSize}, the API calls will be partitioned into max {@code chunkSize}d batches.
 */
public class ReconClient {
  private static final String RESOLVE_COORDINATES_API_URL =
      "https://api.datacommons.org/v1/recon/resolve/coordinate";

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

  public ResolveCoordinatesResponse resolveCoordinates(ResolveCoordinatesRequest request) {
    try {
      return resolveCoordinatesAsync(request).get();
    } catch (Exception e) {
      throw new RuntimeException("Error resolving candidates", e);
    }
  }

  public CompletableFuture<ResolveCoordinatesResponse> resolveCoordinatesAsync(
      ResolveCoordinatesRequest request) {
    ResolveCoordinatesResponse defaultResponse = ResolveCoordinatesResponse.getDefaultInstance();
    if (request.getCoordinatesCount() < 1) {
      return CompletableFuture.completedFuture(defaultResponse);
    }

    return toFutureOfList(
            // Partition request into chunkSize batches.
            // e.g. if chunkSize = 3 then Request(C1, C2, C3) will be chunked into [Request(C1, C2),
            // Request(C3)]
            partition(request.getCoordinatesList(), chunkSize).stream()
                .map(
                    chunk ->
                        request.toBuilder().clearCoordinates().addAllCoordinates(chunk).build())
                .map(
                    // Call API for each chunked request.
                    chunkedRequest ->
                        callApi(RESOLVE_COORDINATES_API_URL, chunkedRequest, defaultResponse))
                .collect(toList()))
        .thenApply(
            // Aggregate chunked responses.
            // e.g. [Response(P1, P2), Response(P3)] will be aggregated into Response(P1, P2, P3)
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
              fromJson(response.body().trim(), responseMessageBuilder);
              return (T) responseMessageBuilder.build();
            });
  }

  private static <T> CompletableFuture<List<T>> toFutureOfList(List<CompletableFuture<T>> futures) {
    return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
        .thenApply(v -> futures.stream().map(CompletableFuture::join).collect(Collectors.toList()));
  }

  private static String toJson(Message message) {
    try {
      return msgToJson(message);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  private static void fromJson(String json, Message.Builder builder) {
    try {
      JsonFormat.parser().merge(json, builder);
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }
}
