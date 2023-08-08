package org.datacommons.util;

import static java.util.stream.Collectors.toList;
import static org.datacommons.util.McfUtil.newIdWithProperty;

import com.google.common.collect.ImmutableSet;
import java.util.*;
import java.util.AbstractMap.SimpleEntry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Debug.Log.Level;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.ValueType;
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
    Set<IdWithProperty> externalEntities = getEntities(node);
    Set<String> dcids =
        new LinkedHashSet<>(
            externalEntities.stream()
                .filter(resolvedEntities::containsKey)
                .map(resolvedEntities::get)
                .collect(toList()));

    if (dcids.isEmpty()) {
      return Optional.empty();
    }

    if (dcids.size() > 1) {
      logWrapper.addEntry(
          Level.LEVEL_ERROR,
          "Resolution_DivergingDcidsForExternalIds",
          String.format("Divergence found. dcids = %s, external ids = %s", dcids, externalEntities),
          node.getLocationsList());
      return Optional.empty();
    }

    return dcids.stream().findFirst();
  }

  private void populateResolvedEntities(ResolveEntitiesResponse response) {
    for (ResolvedEntity entity : response.getResolvedEntitiesList()) {
      fromResolvedEntity(entity)
          .ifPresent(entry -> resolvedEntities.put(entry.getKey(), entry.getValue()));
    }
  }

  private static Set<IdWithProperty> getEntities(PropertyValues node) {
    ImmutableSet.Builder<IdWithProperty> builder = ImmutableSet.builder();

    for (String prop : Vocabulary.PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS) {
      if (node.containsPvs(prop)) {
        for (TypedValue typedValue : node.getPvsMap().get(prop).getTypedValuesList()) {
          if (typedValue.getType() == ValueType.TEXT || typedValue.getType() == ValueType.NUMBER) {
            builder.add(newIdWithProperty(prop, typedValue.getValue()));
          }
        }
      }
    }

    return builder.build();
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
