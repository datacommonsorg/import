// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.util;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.io.IOUtils;
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

  public static String stringFromFile(String filePath) throws IOException {
    return IOUtils.toString(Paths.get(filePath).toUri(), StandardCharsets.UTF_8);
  }

  public static long getCounter(Debug.Log log, String counter) {
    for (Map.Entry<String, Debug.Log.CounterSet> kv : log.getLevelSummaryMap().entrySet()) {
      if (kv.getValue().getCountersMap().containsKey(counter)) {
        return kv.getValue().getCountersMap().get(counter);
      }
    }
    return -1;
  }

  public static boolean checkLog(Debug.Log log, String counter, String subMessage) {
    if (getCounter(log, counter) == -1) {
      System.err.println("Missing counter " + counter + " stat :: " + log.getLevelSummaryMap());
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
      System.err.println(
          "Missing counter " + counter + " in entries :: " + log.getLevelSummaryMap());
    }
    return false;
  }

  public static boolean checkCounter(Debug.Log log, String counter, long expectedCount) {
    long actualCount = getCounter(log, counter);
    if (actualCount == -1) {
      System.err.println("Missing counter " + counter + " stat :: " + log.getLevelSummaryMap());
      return false;
    }
    if (actualCount == expectedCount) {
      return true;
    }
    System.err.println(
        "Actual count:"
            + actualCount
            + " does not equal expected count:"
            + expectedCount
            + " for counter "
            + counter);
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
