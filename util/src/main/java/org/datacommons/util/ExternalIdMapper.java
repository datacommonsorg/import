package org.datacommons.util;

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
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Recon;

// Maps external IDs to DCIDs by calling DC Resolution API.
//
// This interface is used as follows:
// 1. Make N submitNode(node) calls
// 2. Call drainAll() finally
// 3. Make N resolveNode(node) calls
// If this order is not followed, errors will be thrown.
public class ExternalIdMapper {
  private static final int MAX_RESOLUTION_BATCH_IDS = 500;
  private static final String API_ROOT = "https://autopush.recon.datacommons.org/entity/resolve";

  private boolean drained = false;
  private final LogWrapper logCtx;
  private final HttpClient httpClient;

  // IDs waiting to be mapped.
  Map<String, Set<String>> pendingIds = new HashMap<>();
  int numPendingIds = 0;

  // IDs mapped already.
  Map<String, Map<String, String>> mappedIds = new HashMap<>();

  public ExternalIdMapper(HttpClient httpClient, LogWrapper logCtx) {
    this.httpClient = httpClient;
    this.logCtx = logCtx;
  }

  public void submitNode(Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    if (drained) {
      throw new UnexpectedException("Cannot call submitMcf after drainAll!");
    }
    // Nothing to do if this is not a resolvable type.
    if (!isResolvableType(node)) return;

    for (var propIds : getExternalIds(node).entrySet()) {
      var prop = propIds.getKey();
      Set<String> pending = pendingIds.getOrDefault(prop, null);
      Map<String, String> mapped = mappedIds.getOrDefault(prop, null);
      int numOrig = pending != null ? pending.size() : 0;
      for (var id : propIds.getValue()) {
        if (mapped != null && mapped.containsKey(id)) {
          // This ID is already mapped.
          continue;
        }
        if (pending == null) pending = new HashSet<>();
        pending.add(id);
      }
      if (pending != null) {
        numPendingIds += (pending.size() - numOrig);
        pendingIds.put(prop, pending);
      }
    }

    if (numPendingIds >= MAX_RESOLUTION_BATCH_IDS) {
      processPending();
    }
  }

  public void drainAll() throws IOException, InterruptedException {
    processPending();
    drained = true;
  }

  // Resolves the given node, if possible, and returns the node's DCID, if resolved. If not,
  // returns empty string, and update "logCtx" with appropriate error.
  //
  // REQUIRES: drainAll() is called.
  public String resolveNode(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws UnexpectedException {
    if (!drained) {
      throw new UnexpectedException("Cannot call resolveNode before drainAll!");
    }
    String foundDcid = new String();

    // Nothing to do if not resolvable.
    if (!isResolvableType(node)) return foundDcid;

    // If there are multiple external IDs, then they all must map to the same DCID!
    String foundExternalProp = null;
    String foundExternalId = null;
    for (var propIds : getExternalIds(node).entrySet()) {
      var prop = propIds.getKey();
      for (var id : propIds.getValue()) {
        if (mappedIds == null
            || !mappedIds.containsKey(prop)
            || !mappedIds.get(prop).containsKey(id)) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Resolution_UnresolvedExternalId_" + prop,
              "Unresolved external ID :: id: '"
                  + id
                  + "', property: '"
                  + prop
                  + "', node: '"
                  + nodeId,
              node.getLocationsList());
          return foundDcid;
        }
        var newDcid = mappedIds.get(prop).get(id);
        if (!foundDcid.isEmpty() && !foundDcid.equals(newDcid)) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Resolution_DivergingDcidsForExternalIds_" + prop + "_" + foundExternalProp,
              "Found diverging DCIDs for external IDs :: extId1: '"
                  + foundExternalId
                  + ", "
                  + "dcid1: '"
                  + foundDcid
                  + "', property1: '"
                  + foundExternalProp
                  + ", "
                  + "extId2: '"
                  + id
                  + "', dcid2:"
                  + newDcid
                  + ", property2: '"
                  + prop
                  + "', node: '"
                  + nodeId
                  + "'",
              node.getLocationsList());
          return foundDcid;
        }
        foundDcid = newDcid;
        foundExternalProp = prop;
        foundExternalId = id;
      }
    }
    return foundDcid;
  }

  // Returns true if this node is of a type that is resolvable by the ID mapper.
  private static boolean isResolvableType(Mcf.McfGraph.PropertyValues node) {
    for (var typeOf : McfUtil.getPropVals(node, Vocabulary.TYPE_OF)) {
      if (Vocabulary.PLACE_TYPES.contains(typeOf)) return true;
    }
    return false;
  }

  private void processPending() throws IOException, InterruptedException {
    Recon.ResolveEntitiesRequest.Builder request = Recon.ResolveEntitiesRequest.newBuilder();
    request.addWantedIdProperties(Vocabulary.DCID);
    for (var propIds : pendingIds.entrySet()) {
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
    var response = callDc(request.build());
    for (var entity : response.getResolvedEntitiesList()) {
      var parts = entity.getSourceId().split(":", 1);
      if (parts.length != 2
          || entity.getResolvedIdsCount() != 1
          || entity.getResolvedIds(0).getIdsCount() != 1) {
        throw new InvalidProtocolBufferException(
            "Malformed ResolveEntitiesResponse.ResolvedEntity " + entity);
      }
      var extProp = parts[0];
      var extId = parts[1];
      var idProp = entity.getResolvedIds(0).getIds(0);
      if (idProp.getProp() != Vocabulary.DCID) {
        throw new InvalidProtocolBufferException(
            "Malformed ResolveEntitiesResponse.ResolvedEntity " + entity);
      }
      var dcid = idProp.getVal();
      if (dcid.isEmpty()) continue;

      var idMap = mappedIds.computeIfAbsent(extProp, k -> new HashMap<>());
      idMap.put(extId, dcid);
    }
  }

  private Recon.ResolveEntitiesResponse callDc(Recon.ResolveEntitiesRequest reconReq)
      throws IOException, InterruptedException {
    var request =
        HttpRequest.newBuilder(URI.create(API_ROOT))
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
