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

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.text.StringEscapeUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;

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

// The class that provides logging functionality.
public class LogWrapper {
  private static final Logger logger = LogManager.getLogger(LogWrapper.class);

  private static final long SECONDS_BETWEEN_STATUS = 30;
  public static final String REPORT_JSON = "report.json";
  public static final int MAX_ERROR_COUNTERS_LIMIT = 50;
  public static final int MAX_MESSAGES_PER_COUNTER = 10;

  private Debug.Log.Builder log;
  private Path logPath;
  private String locationFile;
  private Instant lastStatusAt;
  private long countAtLastStatus;
  private Set<String> countersWithErrors;

  public LogWrapper(Debug.Log.Builder log, Path outputDir) {
    this.log = log;
    this.countersWithErrors = new HashSet<>();
    logPath = Paths.get(outputDir.toString(), REPORT_JSON);
    logger.info(
        "Report written every {}s to {}",
        SECONDS_BETWEEN_STATUS,
        logPath.toAbsolutePath().normalize().toString());
    locationFile = "FileNotSet.idk";
    lastStatusAt = Instant.now();
    countAtLastStatus = 0;
  }

  public void setLocationFile(String locationFile) {
    this.locationFile = Path.of(locationFile).getFileName().toString();
  }

  public String getLocationFile() {
    return locationFile;
  }

  public void addEntry(Debug.Log.Level level, String counter, String message, long lno) {
    if (log == null) return;
    addEntry(level, counter, message, locationFile, lno);
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
      String msg;
      if (locationFile.isEmpty()) {
        logger.info("{} {} [{}]", count - countAtLastStatus, thing, summaryString());
      } else {
        logger.info(
            "{} {} of {} [{}]", count - countAtLastStatus, thing, locationFile, summaryString());
      }
      persistLog(true);
      lastStatusAt = now;
      countAtLastStatus = count;
    }
  }

  public void persistLog(boolean silent) throws InvalidProtocolBufferException, IOException {
    File logFile = new File(logPath.toString());
    // Without the unescaping something like 'Node' shows up as \u0027Node\u0027
    String jsonStr = StringEscapeUtils.unescapeJson(JsonFormat.printer().print(log.build()));
    FileUtils.writeStringToFile(logFile, jsonStr, StandardCharsets.UTF_8);
    if (!silent) {
      logger.info(
          "Wrote details to {} [{}]",
          logPath.toAbsolutePath().normalize().toString(),
          summaryString());
    }
  }

  public boolean loggedTooManyFailures() {
    if (log.getLevelSummaryOrDefault("LEVEL_FATAL", 0) > 0) {
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
    return log.getLevelSummaryMap().getOrDefault("LEVEL_FATAL", 0L)
        + " fatal, "
        + log.getLevelSummaryMap().getOrDefault("LEVEL_ERROR", 0L)
        + " error(s), "
        + log.getLevelSummaryMap().getOrDefault("LEVEL_WARNING", 0L)
        + " warning(s)";
  }

  private void addEntry(
      Debug.Log.Level level, String counter, String message, String file, long lno) {
    if (level == Debug.Log.Level.LEVEL_ERROR || level == Debug.Log.Level.LEVEL_FATAL) {
      countersWithErrors.add(counter);
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
      l.setFile(file);
      l.setLineNumber(lno);
    }
  }
}
