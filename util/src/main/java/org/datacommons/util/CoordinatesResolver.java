package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest;
import org.datacommons.proto.Recon.ResolveCoordinatesRequest.Coordinate;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse;
import org.datacommons.proto.Recon.ResolveCoordinatesResponse.Place;

/** Resolves nodes with lat-lngs by calling the DC coordinates resolution API. */
// TODO: Add counters for errors.
final class CoordinatesResolver {
  private final Set<Coordinate> resolveCoordinates = ConcurrentHashMap.newKeySet();

  private final ConcurrentHashMap<Coordinate, Set<String>> resolvedCoordinates =
      new ConcurrentHashMap<>();

  private final ReconClient client;

  CoordinatesResolver(ReconClient client) {
    this.client = client;
  }

  boolean submit(PropertyValues node) {
    Optional<Coordinate> optionalCoordinate = getCoordinate(node);
    if (optionalCoordinate.isPresent()) {
      resolveCoordinates.add(optionalCoordinate.get());
      return true;
    }
    return false;
  }

  void drain() {
    if (!resolveCoordinates.isEmpty()) {
      populateResolvedCandidates(
          client.resolveCoordinates(
              ResolveCoordinatesRequest.newBuilder()
                  .addAllCoordinates(resolveCoordinates)
                  .build()));
    }
  }

  Optional<String> resolve(PropertyValues node) {
    return getCoordinate(node)
        .filter(resolvedCoordinates::containsKey)
        .flatMap(coordinate -> resolvedCoordinates.get(coordinate).stream().findFirst());
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
