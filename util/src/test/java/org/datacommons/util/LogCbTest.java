package org.datacommons.util;

import static org.junit.Assert.assertTrue;

import org.datacommons.proto.Debug;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class LogCbTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void logErrorWithErrCb() {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    LogCb logCb = new LogCb(lw, Debug.Log.Level.LEVEL_ERROR, "InMemory", 0);
    String testCounter = "test_counter";
    String testMessage = "test_message";

    logCb.logError(testCounter, testMessage);
    assertTrue(TestUtil.checkLog(logCtx.build(), "test_counter", "test_message"));

    logCb.setDetail(LogCb.VALUE_KEY, "test_value");
    logCb.logError(testCounter, testMessage);
    assertTrue(TestUtil.checkLog(logCtx.build(), "test_counter", "value: 'test_value'"));

    logCb.setCounterPrefix("MCF");
    logCb.logError(testCounter, testMessage);
    assertTrue(TestUtil.checkLog(logCtx.build(), "MCF_test_counter", "test_message"));

    logCb.setCounterPrefix("");
    logCb.setCounterSuffix("Prop");
    logCb.logError(testCounter, testMessage);
    assertTrue(TestUtil.checkLog(logCtx.build(), "test_counter_Prop", "test_message"));
  }
}
