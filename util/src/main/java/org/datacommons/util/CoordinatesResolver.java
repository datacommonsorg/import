package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import com.google.common.collect.Lists;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest.Coordinate;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse.Place;

/**
 * Resolves nodes with lat-lngs by calling the DC coordinates resolution API.
 *
 * <p>The resolver should be called in 3 phases:
 * <li>1. submitNode - submit nodes to be resolved in this phase.
 * <li>2. resolve - nodes will be resolved by invoking the recon API in this phase.
 * <li>3. getResolvedNode - query the resolver to get the resolved DCID in this phase.
 */
public class CoordinatesResolver {
  private static final int DEFAULT_CHUNK_SIZE = 500;

  private final int chunkSize;
  private final AtomicBoolean resolved = new AtomicBoolean(false);

  private final Set<Coordinate> resolveCoordinates = ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap<Coordinate, Set<String>> resolvedCoordinates =
      new ConcurrentHashMap<>();

  private final ReconClient client;

  public CoordinatesResolver(ReconClient client, int chunkSize) {
    this.client = client;
    this.chunkSize = chunkSize;
  }

  public CoordinatesResolver(ReconClient client) {
    this(client, DEFAULT_CHUNK_SIZE);
  }

  public void submitNode(PropertyValues node) {
    if (resolved.get()) {
      throw new IllegalStateException("submitNode called after remote resolution.");
    }
    getCoordinate(node).ifPresent(resolveCoordinates::add);
  }

  // TODO: Pick the ID based on a preferred list.
  public String getResolvedNode(PropertyValues node) {
    return getCoordinate(node)
        .filter(resolvedCoordinates::containsKey)
        .flatMap(coordinate -> resolvedCoordinates.get(coordinate).stream().findFirst())
        .orElse("");
  }

  public CompletableFuture<Void> resolve() {
    if (resolved.getAndSet(true)) {
      throw new IllegalStateException("execute called after remote resolution.");
    }

    if (resolveCoordinates.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    List<List<Coordinate>> chunks = Lists.partition(new ArrayList<>(resolveCoordinates), chunkSize);
    return CompletableFuture.allOf(
        chunks.stream()
            .map(
                chunk -> {
                  try {
                    return client
                        .resolveCoordinates(
                            ResolveCoordinatesRequest.newBuilder().addAllCoordinates(chunk).build())
                        .thenApply(
                            response -> {
                              populateResolvedCandidates(response);
                              return null;
                            });
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(toList())
            .toArray(new CompletableFuture[0]));
  }

  boolean isResolved() {
    return resolved.get();
  }

  private void populateResolvedCandidates(ResolveCoordinatesResponse response) {
    response
        .getPlaceCoordinatesList()
        .forEach(
            placeCoordinate -> {
              if (placeCoordinate.getPlacesCount() > 0) {
                resolvedCoordinates.put(
                    Coordinate.newBuilder()
                        .setLatitude(placeCoordinate.getLatitude())
                        .setLongitude(placeCoordinate.getLongitude())
                        .build(),
                    new LinkedHashSet<>(
                        placeCoordinate.getPlacesList().stream()
                            .map(Place::getDcid)
                            .collect(toList())));
              }
            });
  }

  private static Optional<Coordinate> getCoordinate(PropertyValues node) {
    if (node.containsPvs(Vocabulary.LATITUDE) && node.containsPvs(Vocabulary.LONGITUDE)) {

      Optional<Double> optLat = getDoubleValue(node.getPvsMap().get(Vocabulary.LATITUDE));
      Optional<Double> optLng = getDoubleValue(node.getPvsMap().get(Vocabulary.LONGITUDE));

      if (optLat.isPresent() && optLng.isPresent()) {
        double lat = optLat.get();
        double lng = optLng.get();

        if (!Double.isNaN(lat) && !Double.isNaN(lng)) {
          return Optional.of(Coordinate.newBuilder().setLatitude(lat).setLongitude(lng).build());
        }
      }
    }

    return Optional.empty();
  }

  // TODO: Add support for other formats of values (e.g. 12.34N, 45.56W, etc.)
  private static Optional<Double> getDoubleValue(Values prop) {
    return prop.getTypedValuesList().stream()
        .filter(
            typedValue ->
                ValueType.NUMBER.equals(typedValue.getType())
                    || ValueType.TEXT.equals(typedValue.getType()))
        .findFirst()
        .map(typedValue -> Double.parseDouble(typedValue.getValue()));
  }
}
