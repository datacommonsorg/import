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

  public static Mcf.McfGraph graphFromProto(String protoString) throws IOException {
    Mcf.McfGraph.Builder expected = Mcf.McfGraph.newBuilder();
    TextFormat.getParser().merge(new StringReader(protoString), expected);
    return expected.build();
  }

  public static Mcf.McfGraph graphFromMcf(String mcfString) throws IOException {
    return McfParser.parseInstanceMcfString(mcfString, false, TestUtil.newLogCtx("InMemory"));
  }

  public static String mcfFromFile(String filePath) throws IOException {
    Mcf.McfGraph graph =
        McfParser.parseInstanceMcfFile(filePath, false, TestUtil.newLogCtx(filePath));
    return McfUtil.serializeMcfGraph(graph, true);
  }

  public static boolean checkLog(Debug.Log log, String counter, String subMessage) {
    if (!log.getCounterSet().getCountersMap().containsKey(counter)) {
      System.err.println("Missing counter " + counter + " stat :: " + log.getCounterSet());
      return false;
    }
    boolean foundCounter = false;
    for (Debug.Log.Entry ent : log.getEntriesList()) {
      if (ent.getCounterKey().equals(counter)) {
        foundCounter = true;
        if (ent.getUserMessage().contains(subMessage)) {
          return true;
        }
      }
    }
    if (foundCounter) {
      System.err.println("Missing message fragment '" + subMessage + "' :: " + log);
    } else {
      System.err.println("Missing counter " + counter + " in entries :: " + log.getCounterSet());
    }
    return false;
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
