package org.datacommons.util;

import static org.apache.commons.lang3.StringUtils.isEmpty;
import static org.datacommons.util.McfUtil.getEntities;
import static org.datacommons.util.McfUtil.getPropVal;

import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Recon.IdWithProperty;

/** Resolves nodes locally. */
final class LocalResolver extends Resolver {
  // Map of Entity (represented as IdWithProperty) to DCID.
  private final ConcurrentHashMap<IdWithProperty, String> resolvedEntities =
      new ConcurrentHashMap<>();

  private final LogWrapper logWrapper;

  public LocalResolver(LogWrapper logWrapper) {
    this.logWrapper = logWrapper;
  }

  @Override
  protected boolean submit(PropertyValues node) {
    Set<IdWithProperty> entities = getEntities(node);
    String dcid = getPropVal(node, Vocabulary.DCID);
    if (isEmpty(dcid)) {
      // Even if a DCID is not present in this node, but if one with the same entities was
      // previously added locally and can be already be resolved, then return true.
      return getResolved(node).map(unused -> true).orElse(false);
    }

    if (!entities.isEmpty()) {
      for (IdWithProperty entity : entities) {
        resolvedEntities.put(entity, dcid);
      }
      return true;
    }
    return false;
  }

  @Override
  protected CompletableFuture<Void> resolve() {
    // Nothing to resolve externally, so return immediately.
    return CompletableFuture.completedFuture(null);
  }

  @Override
  protected Optional<String> getResolved(PropertyValues node) {
    return McfUtil.getResolved(node, resolvedEntities, logWrapper);
  }
}
