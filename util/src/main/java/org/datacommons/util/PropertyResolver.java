package org.datacommons.util;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
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
  // Resolved values.
  // Map from property to Map from value to Set of candidate DCIDs.
  // Example: {"isoCode" -> {"IN" -> ["country/IND"], "US" -> ["country/USA"]},
  //           "wikidataId" -> {"Q62" -> ["geoId/0667000"]}}
  private final Map<String, Set<String>> unresolvedProperties = new ConcurrentHashMap<>();
  private final Map<String, Map<String, Set<String>>> resolvedProperties =
      new ConcurrentHashMap<>();

  private final ReconClient client;
  private final LogWrapper logCtx;

  PropertyResolver(ReconClient client, LogWrapper logCtx) {
    this.client = client;
    this.logCtx = logCtx;
  }

  boolean submit(PropertyValues node) {
    if (!McfUtil.isResolvableType(node)) return false;
    boolean submitted = false;
    for (Map.Entry<String, Set<String>> propVals : McfUtil.getExternalIds(node).entrySet()) {
      String prop = propVals.getKey();
      Set<String> vals = propVals.getValue();
      Map<String, Set<String>> resolvedProp = resolvedProperties.get(prop);
      for (String val : vals) {
        if (resolvedProp == null || !resolvedProp.containsKey(val)) {
          unresolvedProperties.computeIfAbsent(prop, k -> ConcurrentHashMap.newKeySet()).add(val);
          submitted = true;
        }
      }
    }
    return submitted;
  }

  void drain() {
    List<CompletableFuture<Void>> futures = new ArrayList<>();
    for (Map.Entry<String, Set<String>> entry : unresolvedProperties.entrySet()) {
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

  Optional<String> resolve(String nodeId, PropertyValues node) {
    String foundDcid = null;
    String foundExternalProp = null;
    String foundExternalId = null;
    Map<String, Set<String>> externalIds = McfUtil.getExternalIds(node);
    for (Map.Entry<String, Set<String>> entry : externalIds.entrySet()) {
      String prop = entry.getKey();
      Set<String> values = entry.getValue();
      Map<String, Set<String>> propResolved = resolvedProperties.get(prop);
      for (String val : values) {
        if (checkAndLogUnresolved(nodeId, node, prop, val, propResolved)) {
          return Optional.empty();
        }
        String newDcid = propResolved.get(val).stream().findFirst().orElse(null);
        if (newDcid != null) {
          if (foundDcid != null && !foundDcid.equals(newDcid)) {
            logDivergingDcids(
                nodeId, node, foundExternalProp, foundExternalId, foundDcid, prop, val, newDcid);
            return Optional.empty();
          }
          foundDcid = newDcid;
          foundExternalProp = prop;
          foundExternalId = val;
        }
      }
    }
    return Optional.ofNullable(foundDcid);
  }

  void addResolvedId(String prop, String externalId, String dcid) {
    resolvedProperties
        .computeIfAbsent(prop, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(externalId, k -> ConcurrentHashMap.newKeySet())
        .add(dcid);
  }

  private boolean checkAndLogUnresolved(
      String nodeId,
      PropertyValues node,
      String prop,
      String id,
      Map<String, Set<String>> propResolved) {
    if (propResolved == null || !propResolved.containsKey(id)) {
      logUnresolvedId(nodeId, node, prop, id);
      return true;
    }
    return false;
  }

  private void logDivergingDcids(
      String nodeId,
      PropertyValues node,
      String prop1,
      String id1,
      String dcid1,
      String prop2,
      String id2,
      String dcid2) {
    boolean foundFirst = prop1.compareTo(prop2) < 0;
    logCtx.addEntry(
        Debug.Log.Level.LEVEL_ERROR,
        "Resolution_DivergingDcidsForExternalIds_"
            + (foundFirst ? prop1 : prop2)
            + "_"
            + (foundFirst ? prop2 : prop1),
        "Found diverging DCIDs for external IDs :: extId1: '"
            + (foundFirst ? id1 : id2)
            + "', "
            + "dcid1: '"
            + (foundFirst ? dcid1 : dcid2)
            + "', property1: '"
            + (foundFirst ? prop1 : prop2)
            + ", "
            + "extId2: '"
            + (foundFirst ? id2 : id1)
            + "', dcid2: '"
            + (foundFirst ? dcid2 : dcid1)
            + "', property2: '"
            + (foundFirst ? prop2 : prop1)
            + "', node: '"
            + nodeId
            + "'",
        node.getLocationsList());
  }

  private void logUnresolvedId(String nodeId, PropertyValues node, String prop, String id) {
    logCtx.addEntry(
        Debug.Log.Level.LEVEL_ERROR,
        "Resolution_UnresolvedExternalId_" + prop,
        "Unresolved external ID :: id: '" + id + "', property: '" + prop + "', node: '" + nodeId,
        node.getLocationsList());
  }

  private static String getPropertyExpression(String prop) {
    return "<-" + prop + "->" + DCID_PROPERTY;
  }
}
