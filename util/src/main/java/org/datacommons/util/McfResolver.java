package org.datacommons.util;

import java.rmi.UnexpectedException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Resolves an in-memory sub-graph by assigning DCIDs to nodes and replacing local-refs with DCIDs,
// doing so over multiple rounds as long as IDs are being assigned or replaced.
//
// If a node is left with an unassigned DCID or an unreplaced local-ref, it is considered failed.
public class McfResolver {
  private static final Logger logger = LogManager.getLogger(McfResolver.class);
  private static final int ROUND_PROGRESS_LOG_INTERVAL = 1000;

  private final ExternalIdResolver idResolver;
  private final Mcf.McfGraph.Builder output;
  private final Mcf.McfGraph.Builder failed;
  private final LogWrapper logCtx;
  private final boolean verbose;

  public McfResolver(
      Mcf.McfGraph subGraph, boolean verbose, ExternalIdResolver idResolver, LogWrapper logCtx) {
    this.idResolver = idResolver;
    this.logCtx = logCtx;
    this.verbose = verbose;
    // We add input to output, and as the rounds progress move failed nodes out.
    output = subGraph.toBuilder();
    failed = Mcf.McfGraph.newBuilder();
  }

  public void resolve() throws UnexpectedException {
    int round = 0;
    RoundResult localRefReplacement = new RoundResult();
    RoundResult dcidAssignment = new RoundResult();
    while (true) {
      if (round == 0 || dcidAssignment.numUpdated > 0) {
        // First round, or a new DCID got assigned, so we might have a local-ref to replace.
        long replaceLocalRefsStartMillis = System.currentTimeMillis();
        localRefReplacement = replaceLocalRefs();
        if (verbose) {
          logger.info(
              "LocalRef Replacement Round "
                  + (round + 1)
                  + " :: "
                  + localRefReplacement.numUpdated
                  + " replaced, "
                  + failed.getNodesMap().size()
                  + " failed, "
                  + localRefReplacement.needsWork.size()
                  + " remaining, "
                  + (System.currentTimeMillis() - replaceLocalRefsStartMillis)
                  + " ms");
        }
        moveFailedNodes(localRefReplacement.failed, "ReplaceLocalRefs");
      } else {
        break;
      }
      if (round == 0 || localRefReplacement.numUpdated > 0) {
        // First round, or a new local-ref got replaced, so we might be able to assign DCID.
        // For instance, with SVObs or Obs if we assign DCID to place node.
        long assignDcidsStartMillis = System.currentTimeMillis();
        dcidAssignment = assignDcids();
        if (verbose) {
          logger.info(
              "DCID Assignment Round "
                  + (round + 1)
                  + " :: "
                  + dcidAssignment.numUpdated
                  + " assigned, "
                  + failed.getNodesMap().size()
                  + " failed, "
                  + dcidAssignment.needsWork.size()
                  + " remaining, "
                  + (System.currentTimeMillis() - assignDcidsStartMillis)
                  + " ms");
        }
        moveFailedNodes(dcidAssignment.failed, "AssignDcids");
      } else {
        break;
      }
      round++;
    }

    // If there are entries in needsWork, then something is oddly broken. Likely it is a cycle of
    // local refs, and thus we are neither able to assign DCIDs nor replace local-refs.
    for (var kv : localRefReplacement.needsWork.entrySet()) {
      moveFailedNode(kv.getKey(), "ReplaceLocalRefs_Remaining");
      var node = failed.getNodesMap().get(kv.getKey()).toBuilder();
      var userMessage =
          "Unable to replace a local reference :: ref: '"
              + kv.getValue()
              + "', node: '"
              + kv.getKey()
              + "'";
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Resolution_IrreplaceableLocalRef",
          userMessage,
          node.getLocationsList());
      node.setErrorMessage(userMessage);
      failed.putNodes(kv.getKey(), node.build());
    }
    for (var kv : dcidAssignment.needsWork.entrySet()) {
      moveFailedNode(kv.getKey(), "AssignDcids_Remaining");
      var node = failed.getNodesMap().get(kv.getKey()).toBuilder();
      var userMessage =
          "Unable to assign DCID due to unresolved local reference :: ref: '"
              + kv.getValue()
              + "', node: '"
              + kv.getKey()
              + "'";
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Resolution_UnassignableNodeDcid",
          userMessage,
          node.getLocationsList());
      node.setErrorMessage(userMessage);
      failed.putNodes(kv.getKey(), node.build());
    }
  }

  public Mcf.McfGraph resolvedGraph() {
    return output.build();
  }

  public Mcf.McfGraph failedGraph() {
    return failed.build();
  }

  // Result from one round of DCID assignment or local-ref replacement.
  private static class RoundResult {
    // The number of updates made (dcid assignments or local-ref replacements).
    public int numUpdated = 0;
    // These set of nodes failed. No point retrying them in future rounds.
    // NOTE: The site that added to failed should have updated the logCtx.
    public Set<String> failed = new HashSet<>();
    // These set of nodes need work in future rounds.
    // nodeId -> local-ref (without l: prefix)
    public Map<String, String> needsWork = new HashMap<>();
  }

  private enum AssignmentMode {
    STAT_VAR_OBS(true),
    LEGACY_POPULATION(true),
    LEGACY_OBSERVATION(true),
    OTHER(false);

    private final boolean allRefsMustBeResolved;

    AssignmentMode(boolean allRefsMustBeResolved) {
      this.allRefsMustBeResolved = allRefsMustBeResolved;
    }
  }

  private static class RoundState {
    private final Map<String, Mcf.McfGraph.PropertyValues> outputSnapshot;
    private final Set<String> failedSnapshot;
    private final Map<String, Mcf.McfGraph.PropertyValues> updatedNodes;
    private final int totalNodes;
    private final long roundStartMillis;
    private int processedNodes;

    private RoundState(
        Map<String, Mcf.McfGraph.PropertyValues> outputSnapshot, Set<String> failedSnapshot) {
      this.outputSnapshot = outputSnapshot;
      this.failedSnapshot = failedSnapshot;
      this.updatedNodes = new LinkedHashMap<>();
      this.totalNodes = outputSnapshot.size();
      this.roundStartMillis = System.currentTimeMillis();
      this.processedNodes = 0;
    }
  }

  private RoundResult assignDcids() throws UnexpectedException {
    RoundResult roundResult = new RoundResult();
    RoundState roundState = newRoundState(false);
    // For each node...
    for (var entry : roundState.outputSnapshot.entrySet()) {
      var nodeId = entry.getKey();
      var snapshotNode = entry.getValue();
      var node = snapshotNode.toBuilder();
      boolean nodeChanged = false;

      // 0. If DCID exists move on to the next node.
      if (!McfUtil.getPropVal(snapshotNode, Vocabulary.DCID).isEmpty()) {
        finishNode("DCID Assignment", roundState, nodeId, node, false);
        continue;
      }

      // 1. Check if there are any unresolved refs.
      String unresolvedRef = findFirstUnresolvedLocalRef(snapshotNode);

      // 2. Identify the type of node.
      var types = McfUtil.getPropVals(snapshotNode, Vocabulary.TYPE_OF);
      var assignmentMode = getAssignmentMode(types);

      // 3. If there are unresolved refs necessary for DCID generation, defer to next round.
      //
      // For svobs/pop/obs types we need all refs to be resolved to assign DCID.
      if (!unresolvedRef.isEmpty() && assignmentMode.allRefsMustBeResolved) {
        roundResult.needsWork.put(nodeId, unresolvedRef);
        finishNode("DCID Assignment", roundState, nodeId, node, false);
        continue;
      }

      // 4. Attempt DCID generation.
      DcidGenerator.Result result = generateDcid(nodeId, snapshotNode, assignmentMode);
      if (!result.dcid.isEmpty()) {
        roundResult.numUpdated++;
        if (!result.keyString.isEmpty()) {
          node.putPvs(
              Vocabulary.KEY_STRING, McfUtil.newValues(Mcf.ValueType.TEXT, result.keyString));
        }
        node.putPvs(Vocabulary.DCID, McfUtil.newValues(Mcf.ValueType.TEXT, result.dcid));
        nodeChanged = true;
      } else {
        // This is not a node we can assign DCID. So move it to failed nodes.
        // TODO: propagate error from DcidGenerator and IDResolver library.
        String userMessage =
            "Failed to assign DCID :: type: '" + types.get(0) + "', node: '" + nodeId + "'";
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "Resolution_DcidAssignmentFailure_" + types.get(0),
            userMessage,
            node.getLocationsList());
        node.setErrorMessage(userMessage);
        roundResult.failed.add(nodeId);
        nodeChanged = true;
      }
      finishNode("DCID Assignment", roundState, nodeId, node, nodeChanged);
    }
    writeNodeUpdates(roundState.updatedNodes);
    return roundResult;
  }

  private RoundResult replaceLocalRefs() {
    RoundResult roundResult = new RoundResult();
    RoundState roundState = newRoundState(true);
    // For each node...
    for (var entry : roundState.outputSnapshot.entrySet()) {
      var nodeId = entry.getKey();
      var node = entry.getValue().toBuilder();
      boolean nodeChanged = false;
      // For each PV in the node...
      for (var prop : node.getPvsMap().keySet()) {
        var vals = node.getPvsMap().get(prop).toBuilder();
        // For all values in a PV...
        for (int i = 0; i < vals.getTypedValuesCount(); i++) {
          var tv = vals.getTypedValuesBuilder(i);
          String localId = getLocalId(tv);
          if (localId.isEmpty()) continue;

          // This is a local ref.
          boolean inOutput = roundState.outputSnapshot.containsKey(localId);
          boolean inFailed = roundState.failedSnapshot.contains(localId);
          if (!inOutput && !inFailed) {
            // This local ID is missing from the entire sub-graph. Mark it as orphan local-ref
            // and move it to failed nodes.
            var userMessage =
                "Found orphan local ref :: ref: '"
                    + tv.getValue()
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'";
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Resolution_OrphanLocalReference_" + prop,
                userMessage,
                node.getLocationsList());
            node.setErrorMessage(userMessage);
            roundResult.failed.add(nodeId);
            nodeChanged = true;
          } else if (inOutput) {
            // Check if it already has DCID assigned.
            var dcid = McfUtil.getPropVal(roundState.outputSnapshot.get(localId), Vocabulary.DCID);
            if (!dcid.isEmpty()) {
              roundResult.numUpdated++;
              tv.setValue(dcid);
              tv.setType(Mcf.ValueType.RESOLVED_REF);
              // Update values in PV.
              node.putPvs(prop, vals.build());
              nodeChanged = true;
            } else {
              // This could be waiting on the resolution of another ref, so defer to next round.
              roundResult.needsWork.put(nodeId, localId);
            }
          } else { // (inFailed)
            // This is a reference to a failed node. This node is doomed too.
            var userMessage =
                "Found a local ref to an unresolvable node :: ref: '"
                    + tv.getValue()
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'";
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Resolution_ReferenceToFailedNode_" + prop,
                userMessage,
                node.getLocationsList());
            node.setErrorMessage(userMessage);
            roundResult.failed.add(nodeId);
            nodeChanged = true;
          }
        }
      }
      finishNode("LocalRef Replacement", roundState, nodeId, node, nodeChanged);
    }
    writeNodeUpdates(roundState.updatedNodes);
    return roundResult;
  }

  private RoundState newRoundState(boolean includeFailedSnapshot) {
    Set<String> failedSnapshot = new HashSet<>();
    if (includeFailedSnapshot) {
      failedSnapshot.addAll(failed.getNodesMap().keySet());
    }
    return new RoundState(new LinkedHashMap<>(output.getNodesMap()), failedSnapshot);
  }

  private void finishNode(
      String phase,
      RoundState roundState,
      String nodeId,
      Mcf.McfGraph.PropertyValues.Builder node,
      boolean nodeChanged) {
    if (nodeChanged) {
      roundState.updatedNodes.put(nodeId, node.build());
    }
    roundState.processedNodes++;
    logRoundProgress(
        phase, roundState.processedNodes, roundState.totalNodes, roundState.roundStartMillis);
  }

  private String findFirstUnresolvedLocalRef(Mcf.McfGraph.PropertyValues node) {
    for (var pv : node.getPvsMap().entrySet()) {
      for (var val : pv.getValue().getTypedValuesList()) {
        String localId = getLocalId(val);
        if (!localId.isEmpty()) {
          return localId;
        }
      }
    }
    return "";
  }

  private AssignmentMode getAssignmentMode(List<String> types) {
    for (var type : types) {
      if (Vocabulary.isStatVarObs(type)) {
        return AssignmentMode.STAT_VAR_OBS;
      }
      if (Vocabulary.isPopulation(type)) {
        return AssignmentMode.LEGACY_POPULATION;
      }
      if (Vocabulary.isLegacyObservation(type)) {
        return AssignmentMode.LEGACY_OBSERVATION;
      }
    }
    return AssignmentMode.OTHER;
  }

  private DcidGenerator.Result generateDcid(
      String nodeId, Mcf.McfGraph.PropertyValues node, AssignmentMode assignmentMode)
      throws UnexpectedException {
    return switch (assignmentMode) {
      case STAT_VAR_OBS -> DcidGenerator.forStatVarObs(nodeId, node);
      case LEGACY_POPULATION -> DcidGenerator.forPopulation(nodeId, node);
      case LEGACY_OBSERVATION -> DcidGenerator.forObservation(nodeId, node);
      case OTHER -> {
        DcidGenerator.Result result = new DcidGenerator.Result();
        if (idResolver != null) {
          result.dcid = idResolver.resolveNode(nodeId, node);
        }
        yield result;
      }
    };
  }

  private void writeNodeUpdates(Map<String, Mcf.McfGraph.PropertyValues> updatedNodes) {
    for (var entry : updatedNodes.entrySet()) {
      output.putNodes(entry.getKey(), entry.getValue());
    }
  }

  private void logRoundProgress(
      String phase, int processedNodes, int totalNodes, long roundStartMillis) {
    if (!verbose
        || processedNodes % ROUND_PROGRESS_LOG_INTERVAL != 0
        || processedNodes == totalNodes) {
      return;
    }
    logger.info(
        "{} progress: {}/{} nodes in {} ms",
        phase,
        processedNodes,
        totalNodes,
        System.currentTimeMillis() - roundStartMillis);
  }

  private void moveFailedNodes(Set<String> failedNodes, String context) {
    for (var failedNode : failedNodes) {
      moveFailedNode(failedNode, context);
    }
  }

  private void moveFailedNode(String failedNode, String context) {
    if (!output.containsNodes(failedNode)) return;
    if (verbose) {
      logger.info(context + " :: failed node " + failedNode);
    }
    failed.putNodes(failedNode, output.getNodesOrThrow(failedNode));
    output.removeNodes(failedNode);
  }

  private String getLocalId(Mcf.McfGraph.TypedValueOrBuilder tv) {
    String result = new String();
    if (tv.getType() == Mcf.ValueType.UNRESOLVED_REF
        && tv.getValue().startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
      result = tv.getValue().substring(tv.getValue().indexOf(Vocabulary.REFERENCE_DELIMITER) + 1);
    }
    return result;
  }
}
