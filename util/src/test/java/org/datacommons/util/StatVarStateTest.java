package org.datacommons.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.datacommons.proto.Debug;
import org.junit.Test;

public class StatVarStateTest {
  @Test
  public void funcCheckDuplicateDcid() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("."));
    StatVarState svs = new StatVarState(lw);
    check(
        svs,
        List.of(
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n",
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    gender: schema:Male        \n"
                + "    statType: dcs:measuredValue\n"));
    assertTrue(
        TestUtil.checkLog(lw.getLog(), "Sanity_SameDcidForDifferentStatVars", "Count_Person"));
  }

  @Test
  public void funcCheckDuplicateSVContent() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("."));
    StatVarState svs = new StatVarState(lw);
    check(
        svs,
        List.of(
            "    Node: dcid:Count_Person\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n",
            "    Node: dcid:Count_Person_Male\n"
                + "    typeOf: schema:StatisticalVariable\n"
                + "    populationType: schema:Person\n"
                + "    measuredProperty: dcs:count\n"
                + "    statType: dcs:measuredValue\n"));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Sanity_DifferentDcidsForSameStatVar",
            "dcid1: 'Count_Person', dcid2: 'Count_Person_Male'"));
  }

  public static void check(StatVarState svs, List<String> nodeStrings) throws IOException {
    for (var nodeString : nodeStrings) {
      var g = TestUtil.graphFromMcf(nodeString);
      for (var kv : g.getNodesMap().entrySet()) {
        svs.check(kv.getKey(), kv.getValue());
      }
    }
  }
}
