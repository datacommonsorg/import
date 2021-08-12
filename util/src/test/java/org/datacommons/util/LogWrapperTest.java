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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import org.apache.commons.io.FileUtils;
import org.datacommons.proto.Debug;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogWrapperTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void main() throws IOException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    // First use LogWrapper to update logCtx
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    lw.setLocationFile("TestInput.mcf");
    lw.addEntry(Debug.Log.Level.LEVEL_ERROR, "MCF_NoColonFound", "Missing Colon", 10);
    lw.addEntry(Debug.Log.Level.LEVEL_WARNING, "MCF_EmptyValue", "Empty value", 20);
    lw.setLocationFile("TestInput.csv");
    lw.incrementCounterBy("CSV_GeneralStats", 42);
    lw.addEntry(Debug.Log.Level.LEVEL_FATAL, "CSV_FoundABomb", "Found a Time Bomb", 30);

    assertEquals("1 fatal, 1 error(s), 1 warning(s)", lw.summaryString());
    assertEquals(true, lw.loggedTooManyFailures());

    File wantFile = new File(this.getClass().getResource("LogWrapperTest_result.json").getPath());
    lw.persistLog(true);
    File gotFile = new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    assertEquals(
        FileUtils.readFileToString(gotFile, StandardCharsets.UTF_8),
        FileUtils.readFileToString(wantFile, StandardCharsets.UTF_8));
  }
}
