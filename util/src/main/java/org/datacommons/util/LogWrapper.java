package org.datacommons.util;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;

// The class that provides logging functionality.
public class LogWrapper {
  private static final Logger logger = LogManager.getLogger(McfParser.class);

  private static final long SECONDS_BETWEEN_STATUS = 60;
  public static final String REPORT_JSON = "report.json";
  public static final int MAX_ERROR_LIMIT = 50;
  public static final int MAX_MESSAGES_PER_COUNTER = 10;

  private Debug.Log.Builder log;
  private Path logPath;
  private String locationFile;
  private Instant lastStatusAt;
  private long countAtLastStatus;

  public LogWrapper(Debug.Log.Builder log, Path outputDir) {
    this.log = log;
    logPath = Paths.get(outputDir.toString(), REPORT_JSON);
    logger.info(
        "Report written periodically to {}", logPath.toAbsolutePath().normalize().toString());
    locationFile = "FileNotSet.idk";
    lastStatusAt = Instant.now();
    countAtLastStatus = 0;
  }

  public void updateLocationFile(String locationFile) {
    this.locationFile = locationFile;
  }

  public void addEntry(Debug.Log.Level level, String counter, String message, long lno) {
    if (log == null) return;
    addEntry(level, counter, message, lno, -1, null);
  }

  public void incrementCounterBy(String counter, int incr) {
    Long c = Long.valueOf(incr);
    if (log.getCounterSet().getCountersMap().containsKey(counter)) {
      c = log.getCounterSet().getCountersMap().get(counter) + Long.valueOf(incr);
    }
    log.getCounterSetBuilder().putCounters(counter, c);
  }

  public void provideStatus(long count, String thing)
      throws InvalidProtocolBufferException, IOException {
    Instant now = Instant.now();
    if (Duration.between(lastStatusAt, now).getSeconds() >= SECONDS_BETWEEN_STATUS) {
      logger.info(
          "Processed {} {} of {};  {}.",
          count - countAtLastStatus,
          thing,
          locationFile,
          summaryString());
      persistLog(true);
      lastStatusAt = now;
    }
  }

  public void persistLog(boolean silent) throws InvalidProtocolBufferException, IOException {
    if (log.getLevelSummaryMap().isEmpty()) {
      if (!silent) logger.info("Found no warnings or errors!");
      return;
    }
    File logFile = new File(logPath.toString());
    // Without the unescaping something like 'Node' shows up as \u0027Node\u0027
    String jsonStr = StringEscapeUtils.unescapeJson(JsonFormat.printer().print(log.build()));
    FileUtils.writeStringToFile(logFile, jsonStr, StandardCharsets.UTF_8);
    if (!silent) {
      logger.info(
          "Failures: {}.  Wrote details to {}",
          summaryString(),
          logPath.toAbsolutePath().normalize().toString());
    }
  }

  public boolean loggedTooManyFailures() {
    if (log.getLevelSummaryOrDefault("LEVEL_FATAL", 0) > 0) {
      logger.error("Found a fatal failure. Quitting!");
      return true;
    }
    if (log.getLevelSummaryOrDefault("LEVEL_ERROR", 0) > MAX_ERROR_LIMIT) {
      logger.error("Found too many failures. Quitting!");
      return true;
    }
    return false;
  }

  public String summaryString() {
    return log.getLevelSummaryMap().getOrDefault("LEVEL_FATAL", 0L)
        + " fatal, "
        + log.getLevelSummaryMap().getOrDefault("LEVEL_ERROR", 0L)
        + " error(s), "
        + log.getLevelSummaryMap().getOrDefault("LEVEL_WARNING", 0L)
        + " warning(s)";
  }

  private void addEntry(
      Debug.Log.Level level, String counter, String message, long lno, long cno, String cname) {
    if (level == Debug.Log.Level.LEVEL_ERROR || level == Debug.Log.Level.LEVEL_FATAL) {
      String displayLevel = level.name().replace("LEVEL_", "");
      logger.error("{} {}:{} - {}: {}", displayLevel, locationFile, lno, counter, message);
    }
    String counterName = counter == null || counter.isEmpty() ? "MissingCounterName" : counter;
    long counterValue = log.getCounterSet().getCountersOrDefault(counterName, 0);
    log.getCounterSetBuilder().putCounters(counterName, counterValue + 1);
    log.putLevelSummary(level.name(), log.getLevelSummaryOrDefault(level.name(), 0) + 1);

    if (counterValue <= MAX_MESSAGES_PER_COUNTER) {
      // Log only up to certain full messages per counter. This can spam the log for WARNING msgs.
      Debug.Log.Entry.Builder e = log.addEntriesBuilder();
      e.setLevel(level);
      e.setUserMessage(message);
      e.setCounterKey(counterName);

      Debug.Log.Location.Builder l = e.getLocationBuilder();
      l.setFile(locationFile);
      l.setLineNumber(lno);
      if (cno > 0) l.setColumnNumber(cno);
      if (cname != null && !cname.isEmpty()) l.setColumnName(cname);
    }
  }
}
