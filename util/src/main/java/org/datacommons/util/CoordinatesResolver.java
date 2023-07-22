package org.datacommons.util;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest.Coordinate;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;

/** Resolves nodes with lat-lngs by calling the DC coordinates resolution API. */
public class CoordinatesResolver {
  private final AtomicBoolean resolved = new AtomicBoolean(false);

  private final Set<Coordinate> resolveCoordinates = ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap<Coordinate, Set<String>> resolvedCoordinates =
      new ConcurrentHashMap<>();

  private final ReconClient client;

  public CoordinatesResolver(ReconClient client) {
    this.client = client;
  }

  public void submitNode(PropertyValues node) {
    if (resolved.get()) {
      throw new IllegalStateException("submitNode called after remote resolution.");
    }
    getCoordinate(node).ifPresent(resolveCoordinates::add);
  }

  // TODO: Pick the ID based on a preferred list.
  public String resolveNode(PropertyValues node) {
    return getCoordinate(node)
        .filter(resolvedCoordinates::containsKey)
        .flatMap(coordinate -> resolvedCoordinates.get(coordinate).stream().findFirst())
        .orElse("");
  }

  // TODO: Support chunking in batches of max size 500.
  public void remoteResolve() throws IOException, InterruptedException {
    if (resolved.get()) {
      throw new IllegalStateException("remoteResolve called after remote resolution.");
    }
    resolved.set(true);

    if (resolveCoordinates.isEmpty()) {
      return;
    }

    ResolveCoordinatesRequest request =
        ResolveCoordinatesRequest.newBuilder().addAllCoordinates(resolveCoordinates).build();
    ResolveCoordinatesResponse response = client.resolveCoordinates(request);
    populateResolvedCandidates(response);
  }

  boolean isResolved() {
    return resolved.get();
  }

  private void populateResolvedCandidates(ResolveCoordinatesResponse response) {
    response
        .getPlaceCoordinatesList()
        .forEach(
            placeCoordinate -> {
              if (placeCoordinate.getPlaceDcidsCount() > 0) {
                resolvedCoordinates.put(
                    Coordinate.newBuilder()
                        .setLatitude(placeCoordinate.getLatitude())
                        .setLongitude(placeCoordinate.getLongitude())
                        .build(),
                    new LinkedHashSet<>(placeCoordinate.getPlaceDcidsList()));
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
