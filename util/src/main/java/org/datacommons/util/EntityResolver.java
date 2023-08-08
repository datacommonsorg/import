package org.datacommons.util;

import static java.util.stream.Collectors.toList;
import static org.datacommons.util.McfUtil.getEntities;
import static org.datacommons.util.McfUtil.newIdWithProperty;

import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Recon.IdWithProperty;
import org.datacommons.proto.Recon.ResolveEntitiesRequest;
import org.datacommons.proto.Recon.ResolveEntitiesResponse;
import org.datacommons.proto.Recon.ResolveEntitiesResponse.ResolvedEntity;
import org.datacommons.proto.Recon.ResolveEntitiesResponse.ResolvedId;

/** Resolves nodes by calling the DC entity resolution API. */
final class EntityResolver extends Resolver {
  private final Set<IdWithProperty> resolveEntities = ConcurrentHashMap.newKeySet();
  // Map of Entity (represented as IdWithProperty) to DCID.
  private final ConcurrentHashMap<IdWithProperty, String> resolvedEntities =
      new ConcurrentHashMap<>();

  private final ReconClient client;

  private final LogWrapper logWrapper;

  public EntityResolver(ReconClient client, LogWrapper logWrapper) {
    this.client = client;
    this.logWrapper = logWrapper;
  }

  @Override
  protected boolean submit(PropertyValues node) {
    Set<IdWithProperty> entities = getEntities(node);
    if (!entities.isEmpty()) {
      resolveEntities.addAll(entities);
      return true;
    }
    return false;
  }

  @Override
  protected CompletableFuture<Void> resolve() {
    if (resolveEntities.isEmpty()) {
      return CompletableFuture.completedFuture(null);
    }

    return client
        .resolveEntities(
            ResolveEntitiesRequest.newBuilder()
                .addAllEntities(
                    resolveEntities.stream().map(McfUtil::newEntitySubGraph).collect(toList()))
                .build())
        .thenAccept(this::populateResolvedEntities);
  }

  @Override
  protected Optional<String> getResolved(PropertyValues node) {
    return McfUtil.getResolved(node, resolvedEntities, logWrapper);
  }

  private void populateResolvedEntities(ResolveEntitiesResponse response) {
    for (ResolvedEntity entity : response.getResolvedEntitiesList()) {
      fromResolvedEntity(entity)
          .ifPresent(entry -> resolvedEntities.put(entry.getKey(), entry.getValue()));
    }
  }

  private static Optional<Map.Entry<IdWithProperty, String>> fromResolvedEntity(
      ResolvedEntity entity) {
    String[] propVal = entity.getSourceId().split(":");
    if (propVal.length != 2) {
      return Optional.empty();
    }
    IdWithProperty key = newIdWithProperty(propVal[0], propVal[1]);

    List<ResolvedId> resolvedIds = entity.getResolvedIdsList();
    if (resolvedIds.isEmpty()) {
      return Optional.empty();
    }

    return resolvedIds.get(0).getIdsList().stream()
        .filter(idWithProperty -> idWithProperty.getProp().equals(Vocabulary.DCID))
        .findFirst()
        .map(IdWithProperty::getVal)
        .map(dcid -> new SimpleEntry<>(key, dcid));
  }
}
