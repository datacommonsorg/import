package org.datacommons.util;

import java.util.HashMap;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Tracks global state on StatVars to help detect whether the same curated DCID is assigned to
// different StatVars (by content), or if the same StatVar content is assigned different curated
// DCIDs.  Used by the McfChecker.
// TODO: Consider expanding this to query DC by generated DCID to find curated DCID in KG.
public class StatVarState {
  private final LogWrapper logCtx;
  private final Map<String, String> generatedToCurated = new HashMap<>();
  private final Map<String, String> curatedToGenerated = new HashMap<>();

  public StatVarState(LogWrapper logCtx) {
    this.logCtx = logCtx;
  }

  boolean check(String id, Mcf.McfGraph.PropertyValues node) {
    var curatedDcid = McfUtil.getPropVal(node, Vocabulary.DCID);
    if (curatedDcid.isEmpty()) {
      // This will have been handled in the McfChecker.
      return false;
    }

    var generated = DcidGenerator.forStatVar(id, node);
    if (generated.dcid.isEmpty()) {
      // This is due to malformed SV node, which should have been handled in the checker.
      return false;
    }

    var existingGeneratedDcid = curatedToGenerated.getOrDefault(curatedDcid, null);
    if (existingGeneratedDcid != null && !existingGeneratedDcid.equals(generated.dcid)) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_SameDcidForDifferentStatVars",
          "Found same curated ID for different StatVars :: curatedDcid: '"
              + curatedDcid
              + "', node: '"
              + id
              + "'",
          node.getLocationsList());
      return false;
    }
    curatedToGenerated.put(curatedDcid, generated.dcid);

    var existingCuratedDcid = generatedToCurated.getOrDefault(generated.dcid, null);
    if (existingCuratedDcid != null && !existingCuratedDcid.equals(curatedDcid)) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_DifferentDcidsForSameStatVar",
          "Found different curated IDs for same StatVar :: dcid1: '"
              + existingCuratedDcid
              + "', dcid2: '"
              + curatedDcid
              + "', node: '"
              + id
              + "'",
          node.getLocationsList());
      return false;
    }
    generatedToCurated.put(generated.dcid, curatedDcid);
    return true;
  }
}
