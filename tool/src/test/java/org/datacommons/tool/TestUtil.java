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

import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.datacommons.proto.Debug;
import org.junit.rules.TemporaryFolder;

// Common set of utils used in e2e tests.
public class TestUtil {
  public static void assertReportFilesAreSimilar(File directory, String expected, String actual)
      throws IOException {
    String testCase = directory.getName();
    Debug.Log.Builder expectedLogBuilder = Debug.Log.newBuilder();
    Debug.Log.Builder actualLogBuilder = Debug.Log.newBuilder();
    JsonFormat.parser().merge(expected, expectedLogBuilder);
    JsonFormat.parser().merge(actual, actualLogBuilder);
    Debug.Log expectedLog = expectedLogBuilder.build();
    Debug.Log actualLog = actualLogBuilder.build();
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
    if (!pass) {
      System.err.println("ACTUAL REPORT for " + testCase + " :: \n\n" + actual);
    }
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
      if (expected.get(key) != actual.get(key)) {
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

  public static String getStringFromTestFile(TemporaryFolder testFolder, String fileName)
      throws IOException {
    File file = new File(Paths.get(testFolder.getRoot().getPath(), fileName).toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }

  public static String getStringFromOutputReport(String parentDirectoryPath) throws IOException {
    File file = new File(Path.of(parentDirectoryPath, "output", "report.json").toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }
}
