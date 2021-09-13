package org.datacommons.util;

import java.util.HashMap;
import java.util.HashSet;
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

  private final Mcf.McfGraph.Builder output;
  private final Mcf.McfGraph.Builder failed;
  private final LogWrapper logCtx;
  private final boolean verbose;

  public McfResolver(Mcf.McfGraph subGraph, boolean verbose, LogWrapper logCtx) {
    this.logCtx = logCtx;
    this.verbose = verbose;
    // We add input to output, and as the rounds progress move failed nodes out.
    output = subGraph.toBuilder();
    failed = Mcf.McfGraph.newBuilder();
  }

  public void resolve() {
    boolean firstRound = true;
    RoundResult localRefReplacement = new RoundResult();
    RoundResult dcidAssignment = new RoundResult();
    while (true) {
      if (firstRound || localRefReplacement.numUpdated > 0) {
        // First round, or a new DCID got assigned, so we might have a local-ref to replace.
        localRefReplacement = replaceLocalRefs();
        moveFailedNodes(localRefReplacement.failed, "ReplaceLocalRefs");
      } else {
        break;
      }
      if (firstRound || dcidAssignment.numUpdated > 0) {
        // First round, or a new local-ref got replaced, so we might be able to assign DCID.
        // For instance, with SVObs or Obs if we assign DCID to place node.
        dcidAssignment = assignDcids();
        moveFailedNodes(dcidAssignment.failed, "AssignDcids");
      } else {
        break;
      }
      firstRound = false;
    }

    // If there are entries in needsWork, then something is oddly broken. Likely it is a cycle of
    // local refs, and thus we are neither able to assign DCIDs nor replace local-refs.
    for (var kv : localRefReplacement.needsWork.entrySet()) {
      var pvs = output.getNodesOrThrow(kv.getKey());
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Resolution_IrreplaceableLocalRef_PotentialCycle",
          "Unable to replace a local reference :: ref: '"
              + kv.getValue()
              + "', node: '"
              + kv.getKey()
              + "'",
          pvs.getLocationsList());
    }
    moveFailedNodes(localRefReplacement.needsWork.keySet(), "ReplaceLocalRefs_Remaining");
    for (var kv : dcidAssignment.needsWork.entrySet()) {
      var pvs = output.getNodesOrThrow(kv.getKey());
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Resolution_UnassignableNodeDcid_PotentialCycle",
          "Unable to assign DCID due to unresolved local reference :: ref: '"
              + kv.getValue()
              + "', node: '"
              + kv.getKey()
              + "'",
          pvs.getLocationsList());
    }
    moveFailedNodes(dcidAssignment.needsWork.keySet(), "AssignDcids_Remaining");
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

  private RoundResult assignDcids() {
    RoundResult roundResult = new RoundResult();
    // For each node...
    for (var nodeId : output.getNodesMap().keySet()) {
      var node = output.getNodesMap().get(nodeId).toBuilder();

      // 1. Check if DCID exists and if there are any unresolved refs.
      String unresolvedRef = new String();
      boolean hasDcid = false;
      // For every PV...
      for (var pv : node.getPvsMap().entrySet()) {
        var prop = pv.getKey();
        if (prop.equals(Vocabulary.DCID)) {
          hasDcid = true;
          break;
        }
        // For every value in PV...
        for (var val : pv.getValue().getTypedValuesList()) {
          unresolvedRef = getLocalId(val.toBuilder());
          if (!unresolvedRef.isEmpty()) break;
        }
        if (!unresolvedRef.isEmpty()) break;
      }
      if (hasDcid) {
        // Already has dcid. Move to the next node...
        continue;
      }

      // 2. Identify the type of node.
      boolean isSvObs = false;
      boolean isLegacyPop = false;
      boolean isLegacyObs = false;
      var types = McfUtil.getPropVals(node.build(), Vocabulary.TYPE_OF);
      for (var type : types) {
        if (Vocabulary.isStatVarObs(type)) {
          isSvObs = true;
          break;
        } else if (Vocabulary.isPopulation(type)) {
          isLegacyPop = true;
          break;
        } else if (Vocabulary.isLegacyObservation(type)) {
          isLegacyObs = true;
          break;
        }
      }

      // 3. If there are unresolved refs necessary for DCID generation, defer to next round.
      //
      // For svobs/pop/obs types we need all refs to be resolved to assign DCID.
      boolean allRefsMustBeResolved = isSvObs || isLegacyPop || isLegacyObs;
      if (!unresolvedRef.isEmpty() && allRefsMustBeResolved) {
        roundResult.needsWork.put(nodeId, unresolvedRef);
        continue;
      }

      // 4. Attempt DCID generation.
      DcidGenerator.Result result = null;
      if (isSvObs) {
        result = DcidGenerator.forStatVarObs(nodeId, node.build());
      } else if (isLegacyPop) {
        result = DcidGenerator.forPopulation(nodeId, node.build());
      } else if (isLegacyObs) {
        result = DcidGenerator.forObservation(nodeId, node.build());
      } else {
        // TODO: Use ExternalIdResolver here.
      }
      if (result != null && !result.dcid.isEmpty()) {
        roundResult.numUpdated++;
        if (!result.keyString.isEmpty()) {
          node.putPvs(
              Vocabulary.KEY_STRING, McfUtil.newValues(Mcf.ValueType.TEXT, result.keyString));
        }
        node.putPvs(Vocabulary.DCID, McfUtil.newValues(Mcf.ValueType.TEXT, result.dcid));
        output.putNodes(nodeId, node.build());
      } else {
        // This is not a node we can assign DCID. So move it to failed nodes.
        // TODO: propagate error from DcidGenerator library.
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "Resolution_DcidAssignmentFailure_" + types.get(0),
            "Failed to assign DCID :: type: '" + types.get(0) + "', node: '" + nodeId + "'",
            node.getLocationsList());
        roundResult.failed.add(nodeId);
      }
    }
    return roundResult;
  }

  private RoundResult replaceLocalRefs() {
    RoundResult roundResult = new RoundResult();
    // For each node...
    for (var nodeId : output.getNodesMap().keySet()) {
      var node = output.getNodesMap().get(nodeId).toBuilder();
      // For each PV in the node...
      for (var prop : node.getPvsMap().keySet()) {
        var vals = node.getPvsMap().get(prop).toBuilder();
        // For all values in a PV...
        for (int i = 0; i < vals.getTypedValuesCount(); i++) {
          var tv = vals.getTypedValuesBuilder(i);
          String localId = getLocalId(tv);
          if (localId.isEmpty()) continue;

          // This is a local ref.
          boolean inOutput = output.containsNodes(localId);
          boolean inFailed = failed.containsNodes(localId);
          if (!inOutput && !inFailed) {
            // This local ID is missing from the entire sub-graph. Mark it as orphan local-ref
            // and move it to failed nodes.
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Resolution_OrphanLocalReference_" + prop,
                "Found orphan local ref :: ref: '"
                    + tv.getValue()
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
                node.getLocationsList());
            roundResult.failed.add(nodeId);
          } else if (inOutput) {
            // Check if it already has DCID assigned.
            var dcid = McfUtil.getPropVal(output.getNodesOrThrow(localId), Vocabulary.DCID);
            if (!dcid.isEmpty()) {
              roundResult.numUpdated++;
              tv.setValue(dcid);
              tv.setType(Mcf.ValueType.RESOLVED_REF);
              // Update values in PV.
              node.putPvs(prop, vals.build());
              // Update PVs in node.
              output.putNodes(nodeId, node.build());
            } else {
              // This could be waiting on the resolution of another ref, so defer to next round.
              roundResult.needsWork.put(nodeId, localId);
            }
          } else { // (inFailed)
            // This is a reference to a failed node. This node is doomed too.
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Resolution_ReferenceToFailedNode_" + prop,
                "Found a local ref to an unresolvable node :: ref: '"
                    + tv.getValue()
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
                node.getLocationsList());
            roundResult.failed.add(nodeId);
          }
        }
      }
    }
    return roundResult;
  }

  private void moveFailedNodes(Set<String> failedNodes, String context) {
    for (var failedNode : failedNodes) {
      if (!output.containsNodes(failedNode)) continue;
      if (verbose) {
        logger.info(context + " :: failed node " + failedNode);
      }
      failed.putNodes(failedNode, output.getNodesOrThrow(failedNode));
      output.removeNodes(failedNode);
    }
  }

  private String getLocalId(Mcf.McfGraph.TypedValue.Builder tv) {
    String result = new String();
    if (tv.getType() == Mcf.ValueType.UNRESOLVED_REF
        && tv.getValue().startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
      result = tv.getValue().substring(tv.getValue().indexOf(Vocabulary.REFERENCE_DELIMITER) + 1);
    }
    return result;
  }
}
