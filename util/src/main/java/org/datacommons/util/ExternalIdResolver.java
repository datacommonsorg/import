package org.datacommons.util;

import static org.apache.commons.lang3.StringUtils.isEmpty;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Recon;

// Resolves nodes with external IDs by calling DC Resolution API.
//
// This class is used as follows:
// 1. Do N submitNode calls (which may batch up RPCs)
// 2. Call drainRemoteCalls() (to drain all RPCs)
// 3. Do M resolveNode calls (to resolve a dcid)
//
// If this order is not followed, errors will be thrown.
// This class is thread-safe.
public class ExternalIdResolver {
  private static final Logger logger = LogManager.getLogger(ExternalIdResolver.class);

  private static final String API_ROOT = "https://api.datacommons.org/v1/recon/entity/resolve";
  // Let tests modify it.
  static int MAX_RESOLUTION_BATCH_IDS = 500;

  private boolean drained = false;
  private final boolean verbose;
  private final LogWrapper logCtx;
  private final HttpClient httpClient;
  private final CoordinatesResolver coordinatesResolver;

  // IDs waiting to be mapped.
  // Key: ID property, Value: set of external IDs
  private Map<String, Set<String>> batchedIds = new HashMap<>();
  private int numBatchedIds = 0;

  // IDs mapped already.
  // Key: ID property, Value: Map(Key: external ID, Value: DCID)
  private Map<String, Map<String, String>> mappedIds = new HashMap<>();

  // First all the id maps are populated ("write" phase), and then used for resolution ("read"
  // phase), so we use RW locks.
  private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

  private final PropertyResolver propertyResolver;

  public ExternalIdResolver(
      HttpClient httpClient, boolean doCoordinatesResolution, boolean verbose, LogWrapper logCtx) {
    this.httpClient = httpClient;
    this.verbose = verbose;
    this.logCtx = logCtx;
    ReconClient reconClient = new ReconClient(httpClient, logCtx, MAX_RESOLUTION_BATCH_IDS);
    this.propertyResolver = new PropertyResolver(reconClient, logCtx);
    if (doCoordinatesResolution) {
      this.coordinatesResolver = new CoordinatesResolver(reconClient);
    } else {
      this.coordinatesResolver = null;
    }
  }

  public void submitNode(Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    rwlock.writeLock().lock();
    try {
      if (drained) {
        throw new UnexpectedException("Cannot call submitMcf after drainRemoteCalls!");
      }
      // Nothing to do if this is not a resolvable type.
      if (!isResolvableType(node)) return;

      propertyResolver.submit(node);

      if (coordinatesResolver != null) {
        coordinatesResolver.submit(node);
      }

    } finally {
      rwlock.writeLock().unlock();
    }
  }

  public void drainRemoteCalls() throws IOException, InterruptedException {
    rwlock.writeLock().lock();
    try {
      if (drained) {
        return;
      }
      drained = true;
      propertyResolver.drain();
      if (coordinatesResolver != null) {
        coordinatesResolver.drain();
      }
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  // Resolves the given node, if possible, and returns the node's DCID if resolved. If not,
  // returns empty string, and updates "logCtx" with appropriate error.
  //
  // REQUIRES: drainRemoteCalls() is called.
  public String resolveNode(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws UnexpectedException {
    rwlock.readLock().lock();
    try {
      if (!drained) {
        throw new UnexpectedException("Cannot call resolveNode before drainRemoteCalls!");
      }
      String foundDcid = new String();

      // Nothing to do if not resolvable.
      if (!isResolvableType(node)) return foundDcid;

      // 1. Try resolving using properties.
      Optional<String> dcid = propertyResolver.resolve(nodeId, node);
      if (dcid.isPresent()) {
        return dcid.get();
      }

      // 2. Try resolving using coordinates.
      if (coordinatesResolver != null) {
        return coordinatesResolver.resolve(node).orElse("");
      }
      return "";
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public synchronized void addLocalGraph(Mcf.McfGraph.PropertyValues node) {
    // Skip doing anything with unresolvable types.
    if (!isResolvableType(node)) {
      return;
    }

    String dcid = McfUtil.getPropVal(node, Vocabulary.DCID);
    if (dcid.isEmpty()) {
      return;
    }

    Map<String, Set<String>> externalIds = getExternalIds(node);
    for (Map.Entry<String, Set<String>> externalId : externalIds.entrySet()) {
      String externalIdProp = externalId.getKey();
      Set<String> externalIdValues = externalId.getValue();
      for (String externalIdValue : externalIdValues) {
        addToMappedIds(externalIdProp, externalIdValue, dcid);
      }
    }
  }

  public synchronized void addLocalGraph(Mcf.McfGraph graph) {
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> nodeEntry :
        graph.getNodesMap().entrySet()) {
      addLocalGraph(nodeEntry.getValue());
    }
  }

  // Returns true if this node is of a type that is resolvable by the ID mapper.
  private static boolean isResolvableType(Mcf.McfGraph.PropertyValues node) {
    for (var typeOf : McfUtil.getPropVals(node, Vocabulary.TYPE_OF)) {
      if (Vocabulary.PLACE_TYPES.contains(typeOf)) return true;
    }
    return false;
  }

  private void drainRemoteCallsInternal() throws IOException, InterruptedException {
    // Package a request with all the batched IDs.
    Recon.ResolveEntitiesRequest.Builder request = Recon.ResolveEntitiesRequest.newBuilder();
    request.addWantedIdProperties(Vocabulary.DCID);
    for (var propIds : batchedIds.entrySet()) {
      var prop = propIds.getKey();
      for (var id : propIds.getValue()) {
        var reqEntity = request.addEntitiesBuilder();
        var reqIds = reqEntity.getEntityIdsBuilder();
        var reqId = reqIds.addIdsBuilder();
        reqId.setProp(prop);
        reqId.setVal(id);
        reqEntity.setSourceId(prop + ":" + id);
      }
    }
    if (request.getEntitiesCount() == 0) {
      return;
    }

    // Issue the RPC.
    if (verbose) {
      logger.info("Issuing ResolveEntities call with " + request.getEntitiesCount() + " IDs");
    }
    var response = callDc(request.build());

    // Process response.
    for (var entity : response.getResolvedEntitiesList()) {
      if (entity.getResolvedIdsCount() == 0) {
        // Unable to resolve ID.
        if (verbose) logger.info("Unable to resolve " + entity.getSourceId());
        continue;
      }
      var parts = entity.getSourceId().split(":", 2);
      // TODO: Add back the (entity.getResolvedIdsCount() == 1) assertion after
      // https://github.com/datacommonsorg/reconciliation/issues/15 is fixed.
      if (parts.length != 2) {
        throw new InvalidProtocolBufferException(
            "Malformed ResolveEntitiesResponse.ResolvedEntity " + entity);
      }
      var extProp = parts[0];
      var extId = parts[1];
      var dcid = new String();
      // TODO: Assert only DCID is returned after
      //  https://github.com/datacommonsorg/reconciliation/issues/13 is fixed.
      for (var idProp : entity.getResolvedIds(0).getIdsList()) {
        if (idProp.getProp().equals(Vocabulary.DCID)) {
          dcid = idProp.getVal();
          break;
        }
      }
      if (!dcid.isEmpty()) {
        addToMappedIds(extProp, extId, dcid);
        if (verbose) logger.info("Resolved " + entity.getSourceId() + " -> " + dcid);
      } else {
        if (verbose) logger.info("Resolved to empty dcid for " + entity.getSourceId());
      }
    }

    // Clear the batch.
    batchedIds.clear();
    numBatchedIds = 0;
  }

  private void addToMappedIds(String extProp, String extId, String dcid) {
    mappedIds.computeIfAbsent(extProp, k -> new HashMap<>()).put(extId, dcid);
  }

  // TODO: Use the generic ReconClient to call the API instead of this method.
  private Recon.ResolveEntitiesResponse callDc(Recon.ResolveEntitiesRequest reconReq)
      throws IOException, InterruptedException {
    logCtx.incrementInfoCounterBy("Resolution_NumDcCalls", 1);
    var request =
        HttpRequest.newBuilder(URI.create(API_ROOT))
            .version(HttpClient.Version.HTTP_1_1)
            .header("accept", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(StringUtil.msgToJson(reconReq)))
            .build();
    var response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
    var reconResp = Recon.ResolveEntitiesResponse.newBuilder();
    var jsonBody = response.body().trim();
    JsonFormat.parser().merge(jsonBody, reconResp);
    return reconResp.build();
  }

  private static Map<String, Set<String>> getExternalIds(Mcf.McfGraph.PropertyValues node) {
    Map<String, Set<String>> idMap = new HashMap<>();
    for (var id : Vocabulary.PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS) {
      if (node.getPvsMap().containsKey(id)) {
        Set<String> idVals = new HashSet<>();
        for (var val : node.getPvsOrThrow(id).getTypedValuesList()) {
          if (val.getType() == Mcf.ValueType.TEXT || val.getType() == Mcf.ValueType.NUMBER) {
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
