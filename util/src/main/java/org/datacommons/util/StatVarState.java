package org.datacommons.util;

import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

import java.util.HashMap;
import java.util.Map;

// Tracks global state on StatVars to help detect whether the same curated DCID is assigned to
// different StatVars (by content), or if the same StatVar content is assigned different curated
// DCIDs.  Used by the McfChecker.
// TODO: Consider expanding this to query DC by generated DCID to find curated DCID in KG.
public class StatVarState {
  private final LogWrapper logCtx;
  private Map<String, String> generatedToCurated = new HashMap<>();
  private Map<String, String> curatedToGenerated = new HashMap<>();

  public StatVarState(LogWrapper logCtx) {
    this.logCtx = logCtx;
  }

  void check(String id, Mcf.McfGraph.PropertyValues node) {
    var curatedDcid = McfUtil.getPropVal(node, Vocabulary.DCID);
    if (curatedDcid.isEmpty()) {
      // This will have been handled in the McfChecker.
      return;
    }

    var generated = DcidGenerator.forStatVar(id, node);
    if (generated.dcid.isEmpty()) {
      logCtx.addEntry(Debug.Log.Level.LEVEL_ERROR, "StatVarChecker_MalformedNode",
              "Unable to compute StatVar DCID :: node: '" + id,
              node.getLocationsList());
      return;
    }

    var existingGeneratedDcid = curatedToGenerated.getOrDefault(curatedDcid,null);
    if (existingGeneratedDcid != null && !existingGeneratedDcid.equals(generated.dcid)) {
      logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Checker_SameDcidForDifferentStatVars",
              "Found same curated ID for different StatVars :: curatedDcid: '" +
                      curatedDcid + "', one node: '" + id + "'",
              node.getLocationsList());
      return;
    }
    curatedToGenerated.put(curatedDcid, generated.dcid);

    var existingCuratedDcid = generatedToCurated.getOrDefault(generated.dcid, null);
    if (existingCuratedDcid != null && !existingCuratedDcid.equals(curatedDcid)) {
      logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Checker_DifferentDcidsForSameStatVar",
              "Found different curated IDs for same StatVar :: dcid1: '" + curatedDcid +
                      "', dcid2: '" + existingCuratedDcid + "', one node: '" + id + "'",
              node.getLocationsList());
      return;
    }
    generatedToCurated.put(generated.dcid, curatedDcid);
  }
}
