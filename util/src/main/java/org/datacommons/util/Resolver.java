package org.datacommons.util;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;

/**
 * The base class for external resolvers.
 *
 * <p>The resolver should be called in 3 phases:
 *
 * <ol>
 *   <li>1. submitNode - submit nodes to be resolved in this phase.
 *   <li>2. resolveNodes - nodes will be resolved by invoking the recon API in this phase (no-op for
 *       local resolvers).
 *   <li>3. getResolvedNode - query the resolver to get the resolved DCID in this phase.
 * </ol>
 *
 * <p>Concrete implementations are required to override the protected methods corresponding to the
 * above. i.e. submit, resolve and getResolved respectively.
 */
abstract class Resolver {
  private final AtomicBoolean resolved = new AtomicBoolean(false);

  protected abstract boolean submit(PropertyValues node);

  protected abstract CompletableFuture<Void> resolve();

  protected abstract Optional<String> getResolved(PropertyValues node);

  boolean submitNode(PropertyValues node) {
    if (resolved.get()) {
      throw new IllegalStateException("submitNode called after resolution.");
    }
    return submit(node);
  }

  // TODO: Pick the ID based on a preferred list.
  Optional<String> getResolvedNode(PropertyValues node) {
    if (!resolved.get()) {
      throw new IllegalStateException("getResolvedNode called before resolution.");
    }
    return getResolved(node);
  }

  CompletableFuture<Void> resolveNodes() {
    if (resolved.getAndSet(true)) {
      throw new IllegalStateException("resolveNodes called after resolution.");
    }

    return resolve();
  }

  boolean isResolved() {
    return resolved.get();
  }
}
