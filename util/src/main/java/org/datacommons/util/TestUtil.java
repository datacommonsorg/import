package org.datacommons.util;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import java.nio.file.Path;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Common set of utils used in unit tests.
public class TestUtil {
  public static LogWrapper newLogCtx(String mcfFile) {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/report.html"));
    logCtx.setLocationFile(mcfFile);
    return logCtx;
  }

  public static Mcf.McfGraph graph(String protoString) throws IOException {
    Mcf.McfGraph.Builder expected = Mcf.McfGraph.newBuilder();
    TextFormat.getParser().merge(new StringReader(protoString), expected);
    return expected.build();
  }

  public static String mcf(String filePath) throws IOException {
    Mcf.McfGraph graph =
        McfParser.parseInstanceMcfFile(filePath, false, TestUtil.newLogCtx(filePath));
    return McfUtil.serializeMcfGraph(graph, true);
  }

  public static Mcf.McfGraph getLocations(Mcf.McfGraph graph) {
    Mcf.McfGraph.Builder newGraph = Mcf.McfGraph.newBuilder();
    newGraph.setType(graph.getType());
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      String nodeId = node.getKey();
      Mcf.McfGraph.PropertyValues pv = node.getValue();
      Mcf.McfGraph.PropertyValues.Builder newPv = Mcf.McfGraph.PropertyValues.newBuilder();
      newPv.addAllLocations(pv.getLocationsList());
      newGraph.putNodes(nodeId, newPv.build());
    }
    return newGraph.build();
  }

  public static Mcf.McfGraph trimLocations(Mcf.McfGraph graph) {
    Mcf.McfGraph.Builder newGraph = Mcf.McfGraph.newBuilder();
    newGraph.setType(graph.getType());
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      Mcf.McfGraph.PropertyValues.Builder newPv = node.getValue().toBuilder();
      newPv.clearLocations();
      newGraph.putNodes(node.getKey(), newPv.build());
    }
    return newGraph.build();
  }
}
