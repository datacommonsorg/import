package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
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

/** Resolves nodes with properties by calling the DC resolution API. */
final class PropertyResolver {
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
    // To be implemented in Step 2
  }

  Optional<String> resolve(PropertyValues node) {
    // To be implemented in Step 3
    return Optional.empty();
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
