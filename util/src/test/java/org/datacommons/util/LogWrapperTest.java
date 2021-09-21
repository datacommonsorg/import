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

import static org.junit.Assert.*;

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
    lw.addEntry(
        Debug.Log.Level.LEVEL_ERROR, "MCF_NoColonFound", "Missing Colon", "TestInput.mcf", 10);
    lw.addEntry(
        Debug.Log.Level.LEVEL_WARNING, "MCF_EmptyValue", "Empty value", "TestInput.mcf", 20);
    lw.incrementInfoCounterBy("CSV_GeneralStats", 42);
    lw.addEntry(
        Debug.Log.Level.LEVEL_FATAL, "CSV_FoundABomb", "Found a Time Bomb", "TestInput.csv", 30);

    assertEquals("1 fatal, 1 error(s), 1 warning(s)", lw.summaryString());
    assertEquals(false, lw.trackStatus(1, ""));

    File wantFile = new File(this.getClass().getResource("LogWrapperTest_result.json").getPath());
    lw.persistLog();
    File gotFile = new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    assertEquals(
        FileUtils.readFileToString(gotFile, StandardCharsets.UTF_8),
        FileUtils.readFileToString(wantFile, StandardCharsets.UTF_8));
  }

  @Test
  public void tooManyErrors() throws IOException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    // First use LogWrapper to update logCtx
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    for (int i = 1; i <= 50; i++) {
      lw.addEntry(
          Debug.Log.Level.LEVEL_ERROR, "MCF_ErrorCounter" + i, "Foo Error", "TestInput.mcf", i);
      assertTrue(lw.trackStatus(1, ""));
    }
    // One more with the same counter is fine.
    lw.addEntry(Debug.Log.Level.LEVEL_ERROR, "MCF_ErrorCounter1", "Foo Error", "TestInput.mcf", 1);
    assertTrue(lw.trackStatus(1, ""));

    // One more with a new counter is not.
    lw.addEntry(
        Debug.Log.Level.LEVEL_ERROR, "MCF_ErrorCounter51", "Foo Error", "TestInput.mcf", 51);
    assertFalse(lw.trackStatus(1, ""));
  }
}
