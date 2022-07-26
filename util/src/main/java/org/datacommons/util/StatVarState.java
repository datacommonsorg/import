package org.datacommons.util;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.net.http.HttpClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Tracks global state on StatVars to help detect whether the same curated DCID is assigned to
// different StatVars (by content), or if the same StatVar content is assigned different curated
// DCIDs.  Used by the McfChecker.
//
// Tracks statType of the StatVars that we know of. Used for performing existence
// checks for SVObs values only if StatVar has statType == MEASUREMENT_RESULT.
//
// This class is thread-safe.
//
// TODO: Consider expanding this to query DC by generated DCID to find curated DCID in KG.
public class StatVarState {
  private HttpClient httpClient;
  private final LogWrapper logCtx;

  private final Map<String, String> generatedToCurated = new ConcurrentHashMap<>();
  private final Map<String, String> curatedToGenerated = new ConcurrentHashMap<>();

  // Key: StatVar DCID, Value: statType value of the StatVar
  private final Map<String, String> statVarStatType = new ConcurrentHashMap<>();

  public StatVarState(LogWrapper logCtx) {
    this.httpClient = null;
    this.logCtx = logCtx;
  }

  public StatVarState(HttpClient httpClient, LogWrapper logCtx) {
    this.httpClient = httpClient;
    this.logCtx = logCtx;
  }

  public String getStatType(String svDcid) {
    if (!statVarStatType.containsKey(svDcid)) {
      // We do not have the statType in memory, so
      // we will need to fetch is synchronously...
      try {
        fetchStatTypeFromApi(svDcid);
      } catch (IOException | InterruptedException e) {
        // TODO(snny): logCtx error here!!
      }
    }
    return statVarStatType.get(svDcid);
  }

  public void addStatType(String svDcid, String statType) {
    statVarStatType.put(svDcid, statType);
  }

  private void fetchStatTypeFromApi(String svDcid) throws IOException, InterruptedException {
    if (this.httpClient == null) {
      // TODO(snny): throw error? do nothing?
    }

    JsonObject dataJson =
        ApiHelper.fetchPropertyValues(this.httpClient, List.of(svDcid), Vocabulary.STAT_TYPE);
    String statType = parseApiStatTypeResponse(dataJson, svDcid);
    if (statType != null) { // statType == null when the response data was off
      addStatType(svDcid, statType);
    }
  }

  // Returns the statType indicated in the payload.
  // Returns null if there are any issue in the import data.
  protected String parseApiStatTypeResponse(JsonObject payload, String svDcid) {
    // TODO(snny): test this function
    
    if (payload == null) {
      // TODO(snny): handle this
    }

    // TODO(snny): handle assertion errors...

    assert payload.has(svDcid);
    JsonObject nodeJson = payload.getAsJsonObject(svDcid);

    assert nodeJson.has("out");
    JsonArray statTypeValuesJson = nodeJson.getAsJsonArray("out");

    assert statTypeValuesJson.size() == 1;
    JsonObject statTypeJson = statTypeValuesJson.get(0).getAsJsonObject();

    assert statTypeJson.has("value");
    String statType = statTypeJson.get("value").getAsString();

    return statType;
  }

  // Returns false if a dcid collision is found. Returns true otherwise.
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
    if (existingGeneratedDcid != null) {
      if (!existingGeneratedDcid.equals(generated.dcid)) {
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
    } else {
      curatedToGenerated.put(curatedDcid, generated.dcid);
    }

    var existingCuratedDcid = generatedToCurated.getOrDefault(generated.dcid, null);
    if (existingCuratedDcid != null) {
      if (!existingCuratedDcid.equals(curatedDcid)) {
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
    } else {
      generatedToCurated.put(generated.dcid, curatedDcid);
    }
    return true;
  }
}
