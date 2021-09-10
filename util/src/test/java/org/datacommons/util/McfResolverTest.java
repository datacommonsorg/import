package org.datacommons.util;

import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class McfResolverTest {
  @Test
  public void instanceMcf() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Paths.get("."));
    McfResolver resolver =
        new McfResolver(
            TestUtil.graphFromMcf(getContent("McfResolverTest_Unresolved_Instance.mcf")),
            true,
            logCtx);
    resolver.resolve();
    assertEquals(
        McfUtil.serializeMcfGraph(resolver.failedGraph(), true),
        TestUtil.mcfFromFile(getFile("McfResolverTest_Resolved_InstanceFailure.mcf")));
    assertEquals(
        McfUtil.serializeMcfGraph(resolver.resolvedGraph(), true),
        TestUtil.mcfFromFile(getFile("McfResolverTest_Resolved_InstanceSuccess.mcf")));

    // There was an orphan ref.
    assertTrue(
        TestUtil.checkLog(log.build(), "Resolution_OrphanLocalReference_parent", "l:AlphabetId"));
    // StatVars cannot be assigned a DCID
    assertTrue(
        TestUtil.checkLog(
            log.build(), "Resolution_DcidAssignmentFailure_StatisticalVariable", "SVId"));
    // Reference to the unresolvable StatVar is marked as failure too.
    assertTrue(
        TestUtil.checkLog(
            log.build(), "Resolution_ReferenceToFailedNode_variableMeasured", "l:SVId"));
  }

  @Test
  public void tmcfCsv() throws IOException, InterruptedException {
    TmcfCsvParser.TEST_mode = true;

    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Paths.get("."));
    logCtx.setLocationFile("McfResolverTest_TmcfCsv.csv");
    TmcfCsvParser parser =
        TmcfCsvParser.init(
            getFile("McfResolverTest_TmcfCsv.tmcf"),
            getFile("McfResolverTest_TmcfCsv.csv"),
            ',',
            logCtx);
    List<Mcf.McfGraph> passList = new ArrayList<>();
    List<Mcf.McfGraph> failList = new ArrayList<>();
    Mcf.McfGraph graph;
    while ((graph = parser.parseNextRow()) != null) {
      McfResolver resolver = new McfResolver(graph, true, logCtx);
      resolver.resolve();
      passList.add(resolver.resolvedGraph());
      failList.add(resolver.failedGraph());
    }
    assertEquals(
        McfUtil.serializeMcfGraph(McfUtil.mergeGraphs(passList), true),
        TestUtil.mcfFromFile(getFile("McfResolverTest_Resolved_TmcfCsvSuccess.mcf")));
    assertEquals(
        McfUtil.serializeMcfGraph(McfUtil.mergeGraphs(failList), true),
        TestUtil.mcfFromFile(getFile("McfResolverTest_Resolved_TmcfCsvFailure.mcf")));
  }

  private String getFile(String name) throws IOException {
    return this.getClass().getResource(name).getPath();
  }

  private String getContent(String name) throws IOException {
    return IOUtils.toString(this.getClass().getResourceAsStream(name), StandardCharsets.UTF_8);
  }
}
