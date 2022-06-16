package org.datacommons.util;

import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.datacommons.proto.Mcf;

// This class checks the existence of typically schema-related, nodes or (select types of)
// triples in the KG or local graph.
//
// Users of this class submit checks for node (submitNodeCheck) or triple (submitTripleCheck)
// along with a logging callback (LogCb).  The implementation batches calls to DC, and on
// completion invokes the callback to notify on existence failures.  At the very end, users
// need to issue a final drain call (drainRemoteCalls).
// This class is thread-safe.
public class ExistenceChecker {
  private static final Logger logger = LogManager.getLogger(ExistenceChecker.class);

  // For now we only need checks for certain Property/Class props.
  private static final Set<String> SCHEMA_PROPERTIES =
      Set.of(Vocabulary.DOMAIN_INCLUDES, Vocabulary.RANGE_INCLUDES, Vocabulary.SUB_CLASS_OF);

  // Batching thresholds.  Allow tests to set this.
  public static int DC_CALL_BATCH_LIMIT = 1000;
  public static int MAX_PENDING_CALLS = 100000;

  // Useful for mocking.
  private final HttpClient httpClient;

  // Logging stuff.
  private final boolean verbose;
  private final LogWrapper logCtx;

  // This is a combination of local KG data and prior cached checks.
  // Node is just the DCID. Triple is "s,p,o" and the property just includes SCHEMA_PROPERTIES.
  private final Set<String> existingNodesOrTriples; // Existence cache
  private final Set<String> missingNodesOrTriples; // Absence cache

  // To amortize DC call latency we batch calls up to DC_CALL_BATCH_LIMIT. The batching happens
  // per (triple) predicate.
  //
  // Batch map:  predicate -> subject -> object -> list of pending call-contexts
  //
  // We batch based on the number of subjects in a predicate. To avoid worst case memory
  // usage, if all checks are for the same node, we have a global limit of max pending calls.
  private final Map<String, Map<String, Map<String, List<LogCb>>>> remoteBatchMap;
  private int totalPendingCallCount = 0;

  public ExistenceChecker(HttpClient httpClient, boolean verbose, LogWrapper logCtx) {
    this.httpClient = httpClient;
    this.logCtx = logCtx;
    this.verbose = verbose;
    existingNodesOrTriples = new HashSet<>();
    missingNodesOrTriples = new HashSet<>();
    remoteBatchMap = new HashMap<>();
  }

  public synchronized void submitNodeCheck(String node, LogCb logCb)
      throws IOException, InterruptedException {
    logCtx.incrementInfoCounterBy("Existence_NumChecks", 1);
    if (checkLocal(node, Vocabulary.TYPE_OF, "", logCb)) {
      return;
    }
    assert !node.isEmpty();
    batchRemoteCall(node, Vocabulary.TYPE_OF, "", logCb);
  }

  public synchronized void submitTripleCheck(String sub, String pred, String obj, LogCb logCb)
      throws IOException, InterruptedException {
    if (pred.equals(Vocabulary.DOMAIN_INCLUDES) && (sub.contains("/") || sub.equals("count"))) {
      // Don't bother with domain checks for schema-less properties.
      // Measured property 'count' is an aggregate that is not a property of an instance, but
      // of a set.
      return;
    }
    logCtx.incrementInfoCounterBy("Existence_NumChecks", 1);
    if (checkLocal(sub, pred, obj, logCb)) {
      return;
    }
    assert !sub.isEmpty();
    batchRemoteCall(sub, pred, obj, logCb);
  }

  public synchronized void addLocalGraph(Mcf.McfGraph graph) {
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      // Skip doing anything with StatVarObs.
      String typeOf = McfUtil.getPropVal(node.getValue(), Vocabulary.TYPE_OF);
      if (typeOf.equals(Vocabulary.STAT_VAR_OBSERVATION_TYPE)
          || typeOf.equals(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX)) {
        continue;
      }

      String dcid = McfUtil.getPropVal(node.getValue(), Vocabulary.DCID);
      if (dcid.isEmpty()) {
        continue;
      }

      existingNodesOrTriples.add(dcid);
      if (missingNodesOrTriples.contains(dcid)) {
        missingNodesOrTriples.remove(dcid);
      }

      if (!typeOf.equals(Vocabulary.CLASS_TYPE) && !typeOf.equals(Vocabulary.PROPERTY_TYPE)) {
        continue;
      }
      for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getValue().getPvsMap().entrySet()) {
        if (SCHEMA_PROPERTIES.contains(pv.getKey())) {
          for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
            var key = makeKey(dcid, pv.getKey(), tv.getValue());
            existingNodesOrTriples.add(key);
            if (missingNodesOrTriples.contains(key)) {
              missingNodesOrTriples.remove(key);
            }
          }
        }
      }
    }
  }

  public synchronized void drainRemoteCalls() throws IOException, InterruptedException {
    // To avoid mutating map while iterating, get the keys first.
    List<String> preds = new ArrayList<>(remoteBatchMap.keySet());
    for (var pred : preds) {
      if (verbose) {
        logger.info("Draining " + remoteBatchMap.get(pred).size() + " dcids for predicate " + pred);
      }
      drainRemoteCallsForPredicate(pred, remoteBatchMap.get(pred));
      remoteBatchMap.remove(pred);
    }
  }

  private void batchRemoteCall(String sub, String pred, String obj, LogCb logCb)
      throws IOException, InterruptedException {
    Map<String, Map<String, List<LogCb>>> subMap = null;
    if (remoteBatchMap.containsKey(pred)) {
      subMap = remoteBatchMap.get(pred);
    } else {
      subMap = new HashMap<>();
    }

    Map<String, List<LogCb>> objMap = null;
    if (subMap.containsKey(sub)) {
      objMap = subMap.get(sub);
    } else {
      objMap = new HashMap<>();
    }

    List<LogCb> calls = null;
    if (objMap.containsKey(obj)) {
      calls = objMap.get(obj);
    } else {
      calls = new ArrayList<>();
    }

    // Add pending call.
    calls.add(logCb);
    objMap.put(obj, calls);
    subMap.put(sub, objMap);
    totalPendingCallCount++;
    remoteBatchMap.put(pred, subMap);

    // Maybe drain the batch.
    if (totalPendingCallCount >= MAX_PENDING_CALLS) {
      if (verbose) logger.info("Draining remote calls due to MAX_PENDING_CALLS");
      drainRemoteCalls();
    } else if (subMap.size() >= DC_CALL_BATCH_LIMIT) {
      if (verbose) {
        logger.info(
            "Draining due to batching limit with "
                + subMap.size()
                + " dcids for "
                + "predicate "
                + pred);
      }
      drainRemoteCallsForPredicate(pred, subMap);
      remoteBatchMap.remove(pred);
    }
  }

  private void drainRemoteCallsForPredicate(
      String pred, Map<String, Map<String, List<LogCb>>> subMap)
      throws IOException, InterruptedException {
    performDcCall(pred, new ArrayList<>(subMap.keySet()), subMap);
  }

  private void performDcCall(
      String pred, List<String> subs, Map<String, Map<String, List<LogCb>>> subMap)
      throws IOException, InterruptedException {
    logCtx.incrementInfoCounterBy("Existence_NumDcCalls", 1);

    var dataJson = ApiHelper.callDc(httpClient, subs, pred);

    if (dataJson == null) {
      if (verbose) {
        logger.info("DC call failed for - " + Strings.join(subs, ',') + ", " + pred);
      }
      // If this was an independent RPC call, don't want to re-issue the call.
      if (subs.size() == 1) {
        var sub = subs.get(0);
        assert subMap.containsKey(sub);
        var objMap = subMap.get(sub);
        for (var cbs : objMap.values()) {
          for (var cb : cbs) {
            cb.logError("Existence_FailedDcCall", "Failed DC Call");
          }
          totalPendingCallCount -= cbs.size();
        }
        subMap.remove(subs.get(0));
        return;
      }
      // Important: If the dcid is malformed, Mixer can return failure. Also, if the URI is too
      // long, then too this happens. So issue independent RPCs now. If this happens often enough,
      // we can revisit.
      logger.warn("DC Call failed (bad DCID or URI length). Issuing individual calls now.");
      for (String sub : subs) {
        performDcCall(pred, List.of(sub), subMap);
      }
      return;
    }

    if (dataJson.entrySet().size() != subs.size()) {
      // Should not really happen, so throw exception
      throw new IOException(
          "Invalid results payload from Staging DC API endpoint for: '"
              + Strings.join(subs, ',')
              + "',"
              + " '"
              + pred
              + "': "
              + dataJson);
    }

    for (var entry : dataJson.entrySet()) {
      var sub = entry.getKey();
      var nodeJson = entry.getValue().getAsJsonObject();
      var objMap = subMap.get(sub);
      for (var kv : objMap.entrySet()) {
        var obj = kv.getKey();
        var cbs = kv.getValue();
        var key = makeKey(sub, pred, obj);
        if (checkOneResult(obj, nodeJson)) {
          existingNodesOrTriples.add(key);
        } else {
          if (verbose) {
            logger.info("Missing " + (obj.isEmpty() ? "node" : "triple") + " in DC " + key);
          }
          missingNodesOrTriples.add(key);
          // Log the missing details.
          for (var cb : cbs) {
            logEntry(cb, obj);
          }
        }
        totalPendingCallCount -= cbs.size();
      }
      subMap.remove(sub);
    }
  }

  private boolean checkOneResult(String obj, JsonObject nodeJson) {
    if (nodeJson.has("out")) {
      if (obj.isEmpty()) {
        // Node existence case.
        if (nodeJson.getAsJsonArray("out").size() > 0) {
          return true;
        }
      } else {
        // Triple existence case.
        for (var objVal : nodeJson.getAsJsonArray("out")) {
          if (objVal.getAsJsonObject().getAsJsonPrimitive("dcid").getAsString().equals(obj)) {
            return true;
          }
        }
      }
    }
    return false;
  }

  // Returns true if we were able to complete the check locally.
  private boolean checkLocal(String sub, String pred, String obj, LogCb logCb) {
    String key = makeKey(sub, pred, obj);
    if (existingNodesOrTriples.contains(key)) {
      return true;
    }
    if (missingNodesOrTriples.contains(key)) {
      logEntry(logCb, obj);
      return true;
    }
    return false;
  }

  private static void logEntry(LogCb logCb, String obj) {
    String message, counter;
    if (obj.isEmpty()) {
      counter = "Existence_MissingReference";
      message = "Failed reference existence check";
    } else {
      counter = "Existence_MissingTriple";
      message = "Failed triple existence check";
    }
    logCb.logError(counter, message);
  }

  private static String makeKey(String s, String p, String o) {
    if (o.isEmpty()) {
      return s;
    }
    return s + "," + p + "," + o;
  }
}
