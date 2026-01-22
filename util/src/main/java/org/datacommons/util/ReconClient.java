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
import org.datacommons.proto.Resolve.ResolveRequest;
import org.datacommons.proto.Resolve.ResolveResponse;

/**
 * Client to the DC resolution APIs.
 *
 * <p>Currently it only resolves coordinates. If the number of coordinates to resolve are greater
 * than {@code chunkSize}, the API calls will be partitioned into max {@code chunkSize}d batches.
 */
public class ReconClient {
  // TODO: Supply an API key for prod /v2/resolve
  private static final String V2_RESOLVE_API_URL = "https://api.datacommons.org/v2/resolve";

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

  public ResolveResponse resolve(ResolveRequest request) {
    try {
      return resolveAsync(request).get();
    } catch (Exception e) {
      throw new RuntimeException("Error resolving nodes.", e);
    }
  }

  public CompletableFuture<ResolveResponse> resolveAsync(ResolveRequest request) {
    ResolveResponse defaultResponse = ResolveResponse.getDefaultInstance();
    if (request.getNodesCount() < 1) {
      return CompletableFuture.completedFuture(defaultResponse);
    }

    // Partition request into chunkSize batches.
    // e.g. if chunkSize = 2 then:
    // Request(N1, N2, N3) will be chunked into [Request(N1, N2), Request(N3)]
    List<ResolveRequest> chunkedRequests =
        partition(request.getNodesList(), chunkSize).stream()
            .map(chunk -> request.toBuilder().clearNodes().addAllNodes(chunk).build())
            .collect(toList());

    // Call API for each chunked request in parallel.
    List<CompletableFuture<ResolveResponse>> chunkedResponseFutures =
        chunkedRequests.stream()
            .map(chunkedRequest -> callApi(V2_RESOLVE_API_URL, chunkedRequest, defaultResponse))
            .collect(toList());

    // Convert List of response futures to Future of list of responses
    CompletableFuture<List<ResolveResponse>> futureOfChunkedResponses =
        toFutureOfList(chunkedResponseFutures);

    // Aggregate chunked responses and return the aggregated response.
    // e.g. [Response(E1, E2), Response(E3)] will be aggregated into Response(E1, E2, E3)
    return futureOfChunkedResponses.thenApply(
        chunkedResponses ->
            ResolveResponse.newBuilder()
                .addAllEntities(
                    chunkedResponses.stream()
                        .flatMap(chunkedResponse -> chunkedResponse.getEntitiesList().stream())
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
      throw new RuntimeException(String.format("Unable to convert proto to json:\n%s", message), e);
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
