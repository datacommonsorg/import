package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Resolve.ResolveRequest;
import org.datacommons.proto.Resolve.ResolveResponse;
import org.datacommons.proto.Resolve.ResolveResponse.Entity.Candidate;

/** Resolves nodes with properties by calling the DC resolution API. */
final class PropertyResolver {
  private static final String DCID_PROPERTY = "dcid";

  // Property in the resolve request.
  // We will support multiple properties, so this will be dynamic.
  
  // Properties and their values to be resolved.
  // Map from property to set of values.
  // Example: {"isoCode" -> ["IN", "US"], "wikidataId" -> ["Q62"]}
  private final Map<String, Set<String>> resolveProperties = new ConcurrentHashMap<>();

  // Resolved values.
  // Map from property to Map from value to Set of candidate DCIDs.
  // Example: {"isoCode" -> {"IN" -> ["country/IND"], "US" -> ["country/USA"]},
  //           "wikidataId" -> {"Q62" -> ["geoId/0667000"]}}
  private final Map<String, Map<String, Set<String>>> resolvedProperties = new ConcurrentHashMap<>();

  private final ReconClient client;

  PropertyResolver(ReconClient client) {
    this.client = client;
  }

  boolean submit(PropertyValues node) {
    Map<String, Set<String>> externalIds = getExternalIds(node);
    if (!externalIds.isEmpty()) {
      for (Map.Entry<String, Set<String>> entry : externalIds.entrySet()) {
        String prop = entry.getKey();
        Set<String> values = entry.getValue();
        resolveProperties.computeIfAbsent(prop, k -> ConcurrentHashMap.newKeySet()).addAll(values);
      }
      return true;
    }
    return false;
  }

  void drain() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : resolveProperties.entrySet()) {
      String prop = entry.getKey();
      Set<String> values = entry.getValue();
      if (!values.isEmpty()) {
        ResolveRequest request =
            ResolveRequest.newBuilder()
                .addAllNodes(values)
                .setProperty(getPropertyExpression(prop))
                .build();
        futures.add(
            client
                .resolveAsync(request)
                .thenAccept(response -> populateResolvedCandidates(prop, response)));
      }
    }
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
  }

  private void populateResolvedCandidates(String prop, ResolveResponse response) {
    Map<String, Set<String>> propResolved =
        resolvedProperties.computeIfAbsent(prop, k -> new ConcurrentHashMap<>());
    response
        .getEntitiesList()
        .forEach(
            entity -> {
              if (entity.getCandidatesCount() > 0) {
                propResolved.put(
                    entity.getNode(),
                    new LinkedHashSet<>(
                        entity.getCandidatesList().stream()
                            .map(Candidate::getDcid)
                            .collect(toList())));
              }
            });
  }

  Optional<String> resolve(PropertyValues node) {
    // To be implemented in Step 3
    return Optional.empty();
  }

  private static String getPropertyExpression(String prop) {
    return "<-" + prop + "->" + DCID_PROPERTY;
  }

  private static Map<String, Set<String>> getExternalIds(PropertyValues node) {
    Map<String, Set<String>> idMap = new HashMap<>();
    for (String id : Vocabulary.PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS) {
      if (node.containsPvs(id)) {
        Set<String> idVals = new HashSet<>();
        for (TypedValue val : node.getPvsOrThrow(id).getTypedValuesList()) {
          if (val.getType() == ValueType.TEXT || val.getType() == ValueType.NUMBER) {
            idVals.add(val.getValue());
          }
        }
        if (!idVals.isEmpty()) {
          idMap.put(id, idVals);
        }
      }
    }
    return idMap;
  }
}
