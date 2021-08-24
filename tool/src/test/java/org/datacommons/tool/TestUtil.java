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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.Gson;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.datacommons.proto.Debug;
import org.junit.rules.TemporaryFolder;

// Common set of utils used in e2e tests.
public class TestUtil {
  public static void assertReportFilesAreSimilar(String expected, String actual)
      throws IOException {
    Debug.Log expectedLog = new Gson().fromJson(expected, Debug.Log.class);
    Debug.Log actualLog = new Gson().fromJson(actual, Debug.Log.class);
    assertEquals(expectedLog.getCounterSet(), actualLog.getCounterSet());
    assertEquals(expectedLog.getLevelSummaryMap(), actualLog.getLevelSummaryMap());
    assertTrue(actualLog.getEntriesList().containsAll(expectedLog.getEntriesList()));
    assertTrue(expectedLog.getEntriesList().containsAll(actualLog.getEntriesList()));
  }

  public static String getStringFromTestFile(TemporaryFolder testFolder, String fileName)
      throws IOException {
    File file = new File(Paths.get(testFolder.getRoot().getPath(), fileName).toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }

  public static String getStringFromOutputReport(String parentDirectoryPath)
      throws IOException {
    File file = new File(Path.of(parentDirectoryPath, "output","report.json").toString());
    return FileUtils.readFileToString(file, StandardCharsets.UTF_8);
  }
}
