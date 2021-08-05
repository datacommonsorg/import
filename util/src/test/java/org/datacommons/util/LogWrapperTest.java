package org.datacommons.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
    {
      LogWrapper lw1 = new LogWrapper(logCtx, "TestInput.mcf");
      lw1.addLog(Debug.Log.Level.LEVEL_ERROR, "MCF_NoColonFound", "Missing Colon", 10);
      lw1.addLog(Debug.Log.Level.LEVEL_WARNING, "MCF_EmptyValue", "Empty value", 20);
    }
    {
      LogWrapper lw2 = new LogWrapper(logCtx, "TestInput.csv");
      lw2.incrementCounterBy("CSV_GeneralStats", 42);
      lw2.addLog(Debug.Log.Level.LEVEL_FATAL, "CSV_FoundABomb", "Found a Time Bomb", 30);
    }

    assertEquals("1 fatal, 1 error(s), 1 warning(s)", LogWrapper.logSummary(logCtx));
    assertEquals(true, LogWrapper.loggedTooManyFailures(logCtx));

    File gotFile = testFolder.newFile("result.json");
    File wantFile = new File(this.getClass().getResource("LogWrapperTest_result.json").getPath());
    LogWrapper.writeLog(logCtx, gotFile.toPath());
    assertEquals(
        FileUtils.readFileToString(gotFile, StandardCharsets.UTF_8),
        FileUtils.readFileToString(wantFile, StandardCharsets.UTF_8));
  }
}
