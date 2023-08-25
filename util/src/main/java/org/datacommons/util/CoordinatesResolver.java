package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Resolve.ResolveRequest;
import org.datacommons.proto.Resolve.ResolveResponse;
import org.datacommons.proto.Resolve.ResolveResponse.Entity.Candidate;

/** Resolves nodes with lat-lngs by calling the DC coordinates resolution API. */
// TODO: Add counters for errors.
final class CoordinatesResolver {
  // This DecimalFormat instance is used to round coordinates to 6 digits
  // since the V2Resolve API formats nodes this way.
  private static final DecimalFormat COORDINATE_NODE_FORMAT = newCoordinateNodeFormat();

  // Coordinates to be resolved.
  // The coordinates are maintained as strings in the following format: "<lat>#<lng>".
  // This format is used since it is the format used by the V2 resolve API as well.
  private final Set<String> resolveCoordinates = ConcurrentHashMap.newKeySet();

  // Coordinates that were resolved to DCIDs.
  // The Map maintains mappings from coordinate string to Set of candidate DCIDs.
  private final ConcurrentHashMap<String, Set<String>> resolvedCoordinates =
      new ConcurrentHashMap<>();

  private final ReconClient client;

  CoordinatesResolver(ReconClient client) {
    this.client = client;
  }

  boolean submit(PropertyValues node) {
    Optional<String> optionalCoordinate = getCoordinate(node);
    if (optionalCoordinate.isPresent()) {
      resolveCoordinates.add(optionalCoordinate.get());
      return true;
    }
    return false;
  }

  void drain() {
    if (!resolveCoordinates.isEmpty()) {
      populateResolvedCandidates(
          client.resolve(
              ResolveRequest.newBuilder()
                  .addAllNodes(resolveCoordinates)
                  .setProperty("<-geoCoordinate->dcid")
                  .build()));
    }
  }

  Optional<String> resolve(PropertyValues node) {
    return getCoordinate(node)
        .filter(resolvedCoordinates::containsKey)
        .flatMap(coordinate -> resolvedCoordinates.get(coordinate).stream().findFirst());
  }

  private void populateResolvedCandidates(ResolveResponse response) {
    response
        .getEntitiesList()
        .forEach(
            entity -> {
              if (entity.getCandidatesCount() > 0) {
                resolvedCoordinates.put(
                    entity.getNode(),
                    new LinkedHashSet<>(
                        entity.getCandidatesList().stream()
                            .map(Candidate::getDcid)
                            .collect(toList())));
              }
            });
  }

  private static Optional<String> getCoordinate(PropertyValues node) {
    if (node.containsPvs(Vocabulary.LATITUDE) && node.containsPvs(Vocabulary.LONGITUDE)) {

      Optional<Double> optLat = getDoubleValue(node.getPvsMap().get(Vocabulary.LATITUDE));
      Optional<Double> optLng = getDoubleValue(node.getPvsMap().get(Vocabulary.LONGITUDE));

      if (optLat.isPresent() && optLng.isPresent()) {
        double lat = optLat.get();
        double lng = optLng.get();

        if (!Double.isNaN(lat) && !Double.isNaN(lng)) {
          return Optional.of(String.format("%s#%s", formatCoordinate(lat), formatCoordinate(lng)));
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

  private static String formatCoordinate(double value) {
    return COORDINATE_NODE_FORMAT.format(value);
  }

  private static DecimalFormat newCoordinateNodeFormat() {
    DecimalFormat format = new DecimalFormat("0.000000");
    format.setRoundingMode(RoundingMode.CEILING);
    return format;
  }
}
