package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.util.LinkedHashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Resolve.ResolveRequest;
import org.datacommons.proto.Resolve.ResolveResponse;
import org.datacommons.proto.Resolve.ResolveResponse.Entity.Candidate;

/** Resolves nodes with names by calling the DC resolution API. */
final class NameResolver {
  // Property in the resolve request.
  private static final String DESCRIPTION_DCID = "<-description->dcid";
  // Names to be resolved.
  private final Set<String> resolveNames = ConcurrentHashMap.newKeySet();

  // Names that were resolved to DCIDs.
  // The Map maintains mappings from name to Set of candidate DCIDs.
  private final ConcurrentHashMap<String, Set<String>> resolvedNames = new ConcurrentHashMap<>();

  private final ReconClient client;

  NameResolver(ReconClient client) {
    this.client = client;
  }

  boolean submit(PropertyValues node) {
    Optional<String> optionalName = getName(node);
    if (optionalName.isPresent()) {
      resolveNames.add(optionalName.get());
      return true;
    }
    return false;
  }

  void drain() {
    if (!resolveNames.isEmpty()) {
      // TODO: Support resolving based on type.
      // See:
      // https://github.com/datacommonsorg/mixer/blob/master/internal/server/v2/resolve/golden/resolve_description_test.go#L50
      ResolveRequest request =
          ResolveRequest.newBuilder()
              .addAllNodes(resolveNames)
              .setProperty(DESCRIPTION_DCID)
              .build();
      ResolveResponse response = client.resolve(request);
      populateResolvedNames(response);
    }
  }

  // TODO: Instead of returning the first one always, allow client to optionally set the type of
  // candidate to return.
  Optional<String> resolve(PropertyValues node) {
    return getName(node)
        .filter(resolvedNames::containsKey)
        .flatMap(name -> resolvedNames.get(name).stream().findFirst());
  }

  private void populateResolvedNames(ResolveResponse response) {
    response
        .getEntitiesList()
        .forEach(
            entity -> {
              if (entity.getCandidatesCount() > 0) {
                resolvedNames.put(
                    entity.getNode(),
                    new LinkedHashSet<>(
                        entity.getCandidatesList().stream()
                            .map(Candidate::getDcid)
                            .collect(toList())));
              }
            });
  }

  private static Optional<String> getName(PropertyValues node) {
    if (node.containsPvs(Vocabulary.NAME)) {
      return getValue(node.getPvsMap().get(Vocabulary.NAME));
    }

    return Optional.empty();
  }

  private static Optional<String> getValue(Values prop) {
    return prop.getTypedValuesList().stream()
        .filter(
            typedValue ->
                ValueType.NUMBER.equals(typedValue.getType())
                    || ValueType.TEXT.equals(typedValue.getType()))
        .map(TypedValue::getValue)
        .findFirst();
  }
}
