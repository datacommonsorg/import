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

import static org.datacommons.proto.Debug.Log.Level.*;

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Debug.StatValidationResult;

// The class that provides logging functionality.  This class is Thread Safe.
// TODO: Profile and maybe optimize incrementInfoCounterBy() and trackStatus() locking.
public class LogWrapper {
  private static final Logger logger = LogManager.getLogger(LogWrapper.class);

  private static final long SECONDS_BETWEEN_STATUS = 30;
  public static final String REPORT_JSON = "report.json";
  public static final int MAX_ERROR_COUNTERS_LIMIT = 50;
  public static final int MAX_MESSAGES_PER_COUNTER = 30;

  public static boolean TEST_MODE = false;

  private final Path logPath;
  public final boolean persistLog;
  private Debug.Log.Builder log;

  private Instant lastStatusAt;
  private long countAtLastStatus = 0;
  private final Set<String> countersWithErrors = new HashSet<>();

  public LogWrapper(Debug.Log.Builder log, Path outputDir) {
    this.log = log;
    this.persistLog = true;
    this.logPath = Paths.get(outputDir.toString(), REPORT_JSON);
    logger.info(
        "Report written periodically to {}", logPath.toAbsolutePath().normalize().toString());
    lastStatusAt = Instant.now();
  }

  public LogWrapper(Debug.Log.Builder log) {
    this.log = log;
    this.persistLog = false;
    this.logPath = null;
    lastStatusAt = Instant.now();
  }

  public void addEntry(
      Debug.Log.Level level, String counter, String message, List<Debug.Log.Location> locations) {
    if (log == null) return;
    if (!locations.isEmpty()) {
      Debug.Log.Location loc = locations.get(0);
      addEntry(level, counter, message, loc.getFile(), loc.getLineNumber());
    } else {
      addEntry(level, counter, message, "FileNotSet.idk", -1);
    }
  }

  public synchronized void addStatsCheckSummaryEntry(StatValidationResult statValidationResult) {
    if (log == null) return;
    log.addStatsCheckSummary(statValidationResult);
  }

  public synchronized void incrementInfoCounterBy(String counter, int incr) {
    incrementCounterBy(LEVEL_INFO.name(), counter, incr);
  }

  public synchronized void incrementWarningCounterBy(String counter, int incr) {
    incrementCounterBy(LEVEL_WARNING.name(), counter, incr);
  }

  // Updates status, provides message and return a boolean indicating if everything was successful.
  // If this returns false, we should bail.
  public synchronized boolean trackStatus(long count, String filePath, String thing)
      throws IOException {
    Instant now = Instant.now();
    if (Duration.between(lastStatusAt, now).getSeconds() >= SECONDS_BETWEEN_STATUS) {
      if (filePath.isEmpty()) {
        logger.info("{} {} [{}]", count - countAtLastStatus, thing, summaryString());
      } else {
        logger.info(
            "{} {} of {} [{}]",
            count - countAtLastStatus,
            thing,
            Path.of(filePath).getFileName().toString(),
            summaryString());
      }
      if (persistLog) persistLog(true);
      lastStatusAt = now;
      countAtLastStatus = count;
    }
    return !loggedTooManyFailures();
  }

  public synchronized void persistLog() throws IOException {
    persistLog(false);
  }

  private void persistLog(boolean silent) throws IOException {
    File logFile = new File(logPath.toString());
    FileUtils.writeStringToFile(logFile, StringUtil.msgToJson(log.build()), StandardCharsets.UTF_8);
    if (!silent) {
      logger.info(
          "Wrote details to {} [{}]",
          logPath.toAbsolutePath().normalize().toString(),
          summaryString());
    }
  }

  public synchronized String dumpLog() throws InvalidProtocolBufferException {
    return StringUtil.msgToJson(log.build());
  }

  private boolean loggedTooManyFailures() {
    if (log.getLevelSummaryMap().containsKey(LEVEL_FATAL.name())) {
      logger.error("Found a fatal failure. Quitting!");
      return true;
    }
    if (countersWithErrors.size() > MAX_ERROR_COUNTERS_LIMIT) {
      logger.error("Found too many failure types. Quitting!");
      return true;
    }
    return false;
  }

  public String summaryString() {
    return log.getLevelSummaryMap()
            .getOrDefault("LEVEL_FATAL", Debug.Log.CounterSet.getDefaultInstance())
            .getCountersMap()
            .size()
        + " fatal, "
        + log.getLevelSummaryMap()
            .getOrDefault("LEVEL_ERROR", Debug.Log.CounterSet.getDefaultInstance())
            .getCountersMap()
            .size()
        + " error(s), "
        + log.getLevelSummaryMap()
            .getOrDefault("LEVEL_WARNING", Debug.Log.CounterSet.getDefaultInstance())
            .getCountersMap()
            .size()
        + " warning(s)";
  }

  private void incrementCounterBy(String level, String counter, int incr) {
    long c = incr;
    var cset =
        log
            .getLevelSummaryMap()
            .getOrDefault(level, Debug.Log.CounterSet.getDefaultInstance())
            .toBuilder();
    if (cset.getCountersMap().containsKey(counter)) {
      c += cset.getCountersMap().get(counter);
    }
    cset.putCounters(counter, c);
    log.putLevelSummary(level, cset.build());
  }

  public synchronized void addEntry(
      Debug.Log.Level level, String counter, String message, String file, long lno) {
    if (TEST_MODE) System.err.println(counter + " - " + message);
    String counterName = counter == null || counter.isEmpty() ? "MissingCounterName" : counter;
    if (level == Debug.Log.Level.LEVEL_ERROR || level == LEVEL_FATAL) {
      countersWithErrors.add(counterName);
    }
    incrementCounterBy(level.name(), counterName, 1);
    var counterValue = log.getLevelSummaryMap().get(level.name()).getCountersMap().get(counterName);
    if (counterValue <= MAX_MESSAGES_PER_COUNTER) {
      // Log only up to certain full messages per counter. This can spam the log for WARNING msgs.
      Debug.Log.Entry.Builder e = log.addEntriesBuilder();
      e.setLevel(level);
      e.setUserMessage(message);
      e.setCounterKey(counterName);

      Debug.Log.Location.Builder l = e.getLocationBuilder();
      l.setFile(file);
      l.setLineNumber(lno);
    }
  }
}
