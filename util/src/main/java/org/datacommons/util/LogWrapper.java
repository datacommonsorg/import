package org.datacommons.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;

public class LogWrapper {
  private static final Logger logger = LogManager.getLogger(McfParser.class);
  private static final int MAX_ERROR_LIMIT = 50;
  private static final int MAX_MESSAGES_PER_COUNTER = 10;

  private Debug.Log.Builder logCtx;
  private String fileName;

  public LogWrapper(Debug.Log.Builder logCtx, String fileName) {
    this.logCtx = logCtx;
    this.fileName = fileName;
  }

  public void addLog(Debug.Log.Level level, String counter, String message, long lno) {
    if (logCtx == null) return;
    addLog(level, counter, message, lno, -1, null);
  }

  public void incrementCounterBy(String counter, int incr) {
    Long c = Long.valueOf(incr);
    if (logCtx.getCounterSet().getCountersMap().containsKey(counter)) {
      c = logCtx.getCounterSet().getCountersMap().get(counter) + Long.valueOf(incr);
    }
    logCtx.getCounterSetBuilder().putCounters(counter, c);
  }

  public static void writeLog(Debug.Log.Builder logCtx, Path logPath)
      throws InvalidProtocolBufferException, IOException {
    if (logCtx.getLevelSummaryMap().isEmpty()) {
      logger.info("Found no warnings or errors!");
      return;
    }
    logger.info(
        "Failures: {}.  Writing details to {}",
        logSummary(logCtx),
        logPath.toAbsolutePath().toString());
    File logFile = new File(logPath.toString());
    // Without the unescaping something like 'Node' shows up as \u0027Node\u0027
    String jsonStr = StringEscapeUtils.unescapeJson(JsonFormat.printer().print(logCtx.build()));
    FileUtils.writeStringToFile(logFile, jsonStr, StandardCharsets.UTF_8);
  }

  public static String logSummary(Debug.Log.Builder logCtx) {
    return logCtx.getLevelSummaryMap().getOrDefault("LEVEL_FATAL", 0L)
        + " fatal, "
        + logCtx.getLevelSummaryMap().getOrDefault("LEVEL_ERROR", 0L)
        + " error(s), "
        + logCtx.getLevelSummaryMap().getOrDefault("LEVEL_WARNING", 0L)
        + " warning(s)";
  }

  public static boolean loggedTooManyFailures(Debug.Log.Builder logCtx) {
    if (logCtx.getLevelSummaryOrDefault("LEVEL_FATAL", 0) > 0) {
      logger.error("Found a fatal failure. Quitting!");
      return true;
    }
    if (logCtx.getLevelSummaryOrDefault("LEVEL_ERROR", 0) > MAX_ERROR_LIMIT) {
      logger.error("Found too many failures. Quitting!");
      return true;
    }
    return false;
  }

  private void addLog(
      Debug.Log.Level level, String counter, String message, long lno, long cno, String cname) {
    if (level == Debug.Log.Level.LEVEL_ERROR || level == Debug.Log.Level.LEVEL_FATAL) {
      String displayLevel = level.name().replace("LEVEL_", "");
      logger.error("{} {}:{} - {}: {}", displayLevel, fileName, lno, counter, message);
    }
    String counterName = counter == null || counter.isEmpty() ? "MissingCounterName" : counter;
    long counterValue = logCtx.getCounterSet().getCountersOrDefault(counterName, 0);
    logCtx.getCounterSetBuilder().putCounters(counterName, counterValue + 1);
    logCtx.putLevelSummary(level.name(), logCtx.getLevelSummaryOrDefault(level.name(), 0) + 1);

    if (counterValue <= MAX_MESSAGES_PER_COUNTER) {
      // Log only up to certain full messages per counter. This can spam the log for WARNING msgs.
      Debug.Log.Entry.Builder e = logCtx.addEntriesBuilder();
      e.setLevel(level);
      e.setUserMessage(message);
      e.setCounterKey(counterName);

      Debug.Log.Location.Builder l = e.getLocationBuilder();
      l.setFile(fileName);
      l.setLineNumber(lno);
      if (cno > 0) l.setColumnNumber(cno);
      if (cname != null && !cname.isEmpty()) l.setColumnName(cname);
    }
  }
}
