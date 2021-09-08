package org.datacommons.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Resolves an in-memory sub-graph by assigning DCIDs to nodes and replacing local-refs with DCIDs,
// doing so over multiple rounds as long as IDs are being assigned or replaced.
//
// If a node is left with an unassigned DCID or an unreplaced local-ref it is considered failed.
public class McfResolver {
  private final Mcf.McfGraph.Builder output;
  private final Mcf.McfGraph.Builder failed;
  private final Mcf.McfGraph input;
  private final LogWrapper logCtx;

  public McfResolver(Mcf.McfGraph subGraph, LogWrapper logCtx) {
    this.logCtx = logCtx;
    input = subGraph;
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
        moveFailedNodes(localRefReplacement.failed);
      } else {
        break;
      }
      if (firstRound || dcidAssignment.numUpdated > 0) {
        // First round, or a new local-ref got replaced, so we might be able to assign DCID.
        // For instance, with SVObs or Obs if we assign DCID to place node.
        dcidAssignment = assignDcids();
        moveFailedNodes(dcidAssignment.failed);
      } else {
        break;
      }
      firstRound = false;
    }

    // If there are entries in needsWork, then something is oddly broken. Likely it is a cycle of
    // local refs, and thus we are neither able to assign DCIDs nor replace local-refs.
    for (var kv : localRefReplacement.needsWork.entrySet()) {
      var pvs = input.getNodesOrThrow(kv.getKey());
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
    moveFailedNodes(localRefReplacement.needsWork.keySet());
    for (var kv : dcidAssignment.needsWork.entrySet()) {
      var pvs = input.getNodesOrThrow(kv.getKey());
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
    moveFailedNodes(dcidAssignment.needsWork.keySet());
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
    // nodeId -> local-ref
    public Map<String, String> needsWork = new HashMap<>();
  }

  private RoundResult assignDcids() {
    RoundResult roundResult = new RoundResult();
    for (var nodePv : output.getNodesMap().entrySet()) {
      String unresolvedRef = new String();
      boolean needsDcid = true;

      var nodeId = nodePv.getKey();
      var node = nodePv.getValue().toBuilder();
      for (var pv : node.getPvsMap().entrySet()) {
        var prop = pv.getKey();
        if (prop.equals(Vocabulary.DCID)) {
          needsDcid = false;
          break;
        }
        var vals = pv.getValue().toBuilder();
        for (int i = 0; i < vals.getTypedValuesCount(); i++) {
          var tv = vals.getTypedValuesBuilder(i);
          var val = tv.getValue();
          if (tv.getType() == Mcf.ValueType.UNRESOLVED_REF
              && val.startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
            unresolvedRef = tv.getValue();
            break;
          }
          if (!unresolvedRef.isEmpty()) break;
        }
      }
      if (!needsDcid) {
        // Nothing to do.
        continue;
      }

      var types = McfUtil.getPropVals(node.build(), Vocabulary.TYPE_OF);
      boolean isSvObs = types.contains(Vocabulary.STAT_VAR_OBSERVATION_TYPE);
      boolean isLegacyPop = types.contains(Vocabulary.LEGACY_POPULATION_TYPE_SUFFIX);
      boolean isLegacyObs = types.contains(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX);
      // For svobs/pop/obs types we need all refs to be resolved to assign DCID.
      boolean allRefsMustBeResolved = isSvObs | isLegacyPop | isLegacyObs;

      if (unresolvedRef.isEmpty() || !allRefsMustBeResolved) {
        // Attempt DCID generation.
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
      } else {
        roundResult.needsWork.put(nodeId, unresolvedRef);
      }
    }
    return roundResult;
  }

  private RoundResult replaceLocalRefs() {
    RoundResult roundResult = new RoundResult();
    for (var nodePv : output.getNodesMap().entrySet()) {
      var nodeId = nodePv.getKey();
      var node = nodePv.getValue().toBuilder();
      boolean hasUnresolvedRef = false;
      for (var pv : node.getPvsMap().entrySet()) {
        var prop = pv.getKey();
        var vals = pv.getValue().toBuilder();
        for (int i = 0; i < vals.getTypedValuesCount(); i++) {
          var tv = vals.getTypedValuesBuilder(i);
          var val = tv.getValue();
          if (tv.getType() == Mcf.ValueType.UNRESOLVED_REF
              && val.startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
            var localId = val.substring(val.indexOf(Vocabulary.REFERENCE_DELIMITER) + 1);
            if (!input.containsNodes(localId)) {
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
            } else {
              var dcid = McfUtil.getPropVal(input.getNodesOrThrow(localId), Vocabulary.DCID);
              if (!dcid.isEmpty()) {
                roundResult.numUpdated++;
                tv.setValue(dcid);
                tv.setType(Mcf.ValueType.RESOLVED_REF);
              } else {
                roundResult.needsWork.put(nodeId, tv.getValue());
              }
            }
          }
        }
      }
    }
    return roundResult;
  }

  private void moveFailedNodes(Set<String> failedNodes) {
    for (var failedNode : failedNodes) {
      if (!output.containsNodes(failedNode)) continue;
      failed.putNodes(failedNode, output.getNodesOrThrow(failedNode));
      output.removeNodes(failedNode);
    }
  }
}
