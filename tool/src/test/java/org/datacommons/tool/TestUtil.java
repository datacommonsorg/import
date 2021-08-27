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

package org.datacommons.tool;

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.datacommons.proto.Debug;
import org.junit.rules.TemporaryFolder;

// Common set of utils used in e2e tests.
public class TestUtil {
  public static void assertReportFilesAreSimilar(File directory, String expected, String actual)
      throws IOException {
    String testCase = directory.getName();
    Debug.Log expectedLog = reportToProto(expected).build();
    Debug.Log actualLog = reportToProto(actual).build();
    // Compare the maps, printing log messages along the way and assert only at the very end.
    boolean pass = true;
    pass &=
        areMapsEqual(
            testCase,
            "Counter Set",
            expectedLog.getCounterSet().getCountersMap(),
            actualLog.getCounterSet().getCountersMap());
    pass &=
        areMapsEqual(
            testCase,
            "Level Summary",
            expectedLog.getLevelSummaryMap(),
            actualLog.getLevelSummaryMap());
    pass &= actualLog.getEntriesList().containsAll(expectedLog.getEntriesList());
    pass &= expectedLog.getEntriesList().containsAll(actualLog.getEntriesList());
    assertThat(actualLog).ignoringRepeatedFieldOrder().isEqualTo(expectedLog);
    assertTrue(pass);
  }

  private static boolean areMapsEqual(
      String testCase, String mapType, Map<String, Long> expected, Map<String, Long> actual) {
    boolean equal = true;
    if (expected.keySet().size() > actual.keySet().size()) {
      equal = false;
      Set<String> diff = new HashSet<>((Collection<String>) expected.keySet());
      diff.removeAll((Collection<String>) actual.keySet());
      System.err.println(
          testCase + " :: " + mapType + " has some missing keys: " + String.join(", ", diff));
    } else if (expected.keySet().size() < actual.keySet().size()) {
      equal = false;
      Set<String> diff = new HashSet<>((Collection<String>) actual.keySet());
      diff.removeAll((Collection<String>) expected.keySet());
      System.err.println(
          testCase + " :: " + mapType + " has some extra keys: " + String.join(", ", diff));
    }
    for (String key : expected.keySet()) {
      if (!actual.containsKey(key)) {
        equal = false;
        System.err.println(mapType + " actual report is missing the key: " + key);
      }
      if (!expected.get(key).equals(actual.get(key))) {
        equal = false;
        System.err.println(
            testCase
                + " :: "
                + mapType
                + " has different values for the key "
                + key
                + " : expected ("
                + expected.get(key)
                + ") vs. actual ("
                + actual.get(key)
                + ")");
      }
    }
    return equal;
  }

  private static Debug.Log.Builder reportToProto(String report)
      throws InvalidProtocolBufferException {
    Debug.Log.Builder logBuilder = Debug.Log.newBuilder();
    JsonFormat.parser().merge(report, logBuilder);
    return logBuilder;
  }

  public static void writeSortedReport(Path inputPath, Path outputPath) throws IOException {
    Debug.Log.Builder logBuilder = reportToProto(readStringFromPath(inputPath));
    List<Debug.Log.Entry> entries = new ArrayList<>(logBuilder.getEntriesList());
    Collections.sort(
        entries,
        new Comparator<Debug.Log.Entry>() {
          @Override
          public int compare(Debug.Log.Entry o1, Debug.Log.Entry o2) {
            return o1.toString().compareTo(o2.toString());
          }
        });
    logBuilder.clearEntries();
    logBuilder.addAllEntries(entries);
    String jsonStr = StringEscapeUtils.unescapeJson(JsonFormat.printer().print(logBuilder.build()));
    FileUtils.writeStringToFile(new File(outputPath.toString()), jsonStr, StandardCharsets.UTF_8);
  }

  public static Path getTestFilePath(TemporaryFolder testFolder, String fileName)
      throws IOException {
    return Paths.get(testFolder.getRoot().getPath(), fileName);
  }

  public static Path getOutputFilePath(String parentDirectoryPath, String fileName)
      throws IOException {
    return Path.of(parentDirectoryPath, "output", fileName);
  }

  public static String readStringFromPath(Path filePath) throws IOException {
    File file = new File(filePath.toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }
}
