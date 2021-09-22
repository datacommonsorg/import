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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Debug.StatValidationResult;

// The class that provides logging functionality.  This class is Thread Safe.
// This class can be heavily contended so it uses concurrent-hashmaps and atomic counters for
// the fast-path functions, and the object lock for slow-path functions.
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

  // Update with lock, but read without object lock.
  private volatile Instant lastStatusAt;
  // Updated and read with object lock.
  private long countAtLastStatus = 0;
  private AtomicLong currentCount = new AtomicLong();
  // A copy of the counters that are updated into the Log before persisting.
  private final List<ConcurrentHashMap<String, Long>> counterMaps = new ArrayList<>();

  public LogWrapper(Debug.Log.Builder log, Path outputDir) {
    this.log = log;
    this.persistLog = true;
    this.logPath = Paths.get(outputDir.toString(), REPORT_JSON);
    logger.info(
        "Report written periodically to {}", logPath.toAbsolutePath().normalize().toString());
    lastStatusAt = Instant.now();
    initCounterMap();
  }

  public LogWrapper(Debug.Log.Builder log) {
    this.log = log;
    this.persistLog = false;
    this.logPath = null;
    lastStatusAt = Instant.now();
    initCounterMap();
  }

  private void initCounterMap() {
    for (var level : Debug.Log.Level.values()) {
      counterMaps.add(new ConcurrentHashMap<>());
    }
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

  // Lock Note: This happens at the very end.
  public synchronized void addStatsCheckSummaryEntry(StatValidationResult statValidationResult) {
    if (log == null) return;
    log.addStatsCheckSummary(statValidationResult);
  }

  public void incrementInfoCounterBy(String counter, int incr) {
    incrementCounterBy(LEVEL_INFO, counter, incr);
  }

  public void incrementWarningCounterBy(String counter, int incr) {
    incrementCounterBy(LEVEL_WARNING, counter, incr);
  }

  // Updates status, provides message and return a boolean indicating if everything was successful.
  // If this returns false, we should bail.
  public boolean trackStatus(long incCount, String thing) throws IOException {
    Instant now = Instant.now();
    currentCount.addAndGet(incCount);
    if (Duration.between(lastStatusAt, now).getSeconds() >= SECONDS_BETWEEN_STATUS) {
      trackStatusLocked(now, incCount, thing);
    }
    return !loggedTooManyFailures();
  }

  // Lock Note: This should happen approx every SECONDS_BETWEEN_STATUS.
  public synchronized void trackStatusLocked(Instant now, long incCount, String thing)
      throws IOException {
    // Check again with the lock held.
    if (Duration.between(lastStatusAt, now).getSeconds() < SECONDS_BETWEEN_STATUS) {
      // Lost the race.
      return;
    }

    logger.info("{} {} [{}]", currentCount.get() - countAtLastStatus, thing, summaryString());
    if (persistLog) persistLog(true);
    lastStatusAt = now;
    countAtLastStatus = currentCount.get();
  }

  // Lock Note: This should only happen at the very end.
  public synchronized void persistLog() throws IOException {
    persistLog(false);
  }

  // Lock Note: Used only by tests.
  public synchronized Debug.Log getLog() {
    refreshCounters();
    return log.build();
  }

  private void persistLog(boolean silent) throws IOException {
    refreshCounters();
    File logFile = new File(logPath.toString());
    FileUtils.writeStringToFile(logFile, StringUtil.msgToJson(log.build()), StandardCharsets.UTF_8);
    if (!silent) {
      logger.info(
          "Wrote details to {} [{}]",
          logPath.toAbsolutePath().normalize().toString(),
          summaryString());
    }
  }

  // Lock Note: This should happen only on fatal errors.
  public synchronized String dumpLog() throws InvalidProtocolBufferException {
    refreshCounters();
    return StringUtil.msgToJson(log.build());
  }

  private boolean loggedTooManyFailures() {
    if (!counterMaps.get(LEVEL_FATAL.getNumber()).isEmpty()) {
      logger.error("Found a fatal failure. Quitting!");
      return true;
    }
    if (counterMaps.get(LEVEL_ERROR.getNumber()).size() > MAX_ERROR_COUNTERS_LIMIT) {
      logger.error("Found too many failure types. Quitting!");
      return true;
    }
    return false;
  }

  public String summaryString() {
    return counterMaps.get(LEVEL_FATAL.getNumber()).size()
        + " fatal, "
        + counterMaps.get(LEVEL_ERROR.getNumber()).size()
        + " error(s), "
        + counterMaps.get(LEVEL_WARNING.getNumber()).size()
        + " warning(s)";
  }

  private void incrementCounterBy(Debug.Log.Level level, String counter, int incr) {
    counterMaps
        .get(level.getNumber())
        .compute(
            counter,
            (k, v) -> {
              return v == null ? incr : v + incr;
            });
  }

  private void refreshCounters() {
    log.clearLevelSummary();
    for (var level : Debug.Log.Level.values()) {
      if (counterMaps.get(level.getNumber()).isEmpty()) continue;
      var cset =
          log
              .getLevelSummaryMap()
              .getOrDefault(level.name(), Debug.Log.CounterSet.getDefaultInstance())
              .toBuilder();
      for (var kv : counterMaps.get(level.getNumber()).entrySet()) {
        cset.putCounters(kv.getKey(), kv.getValue());
      }
      log.putLevelSummary(level.name(), cset.build());
    }
  }

  public void addEntry(
      Debug.Log.Level level, String counter, String message, String file, long lno) {
    if (TEST_MODE) System.err.println(counter + " - " + message);
    String counterName = counter == null || counter.isEmpty() ? "MissingCounterName" : counter;
    incrementCounterBy(level, counterName, 1);

    var counterValue =
        counterMaps
            .get(level.getNumber())
            .computeIfAbsent(
                counterName,
                v -> {
                  return 0L;
                });
    if (counterValue <= MAX_MESSAGES_PER_COUNTER) {
      // Log only up to certain full messages per counter. This can spam the log for WARNING msgs.
      addEntryLocked(level, counterName, message, file, lno);
    }
  }

  // Lock Note: This should only happen for a total of MAX_MESSAGES_PER_COUNTER x
  // MAX_ERROR_COUNTERS_LIMIT times.
  private synchronized void addEntryLocked(
      Debug.Log.Level level, String counter, String message, String file, long lno) {
    Debug.Log.Entry.Builder e = log.addEntriesBuilder();
    e.setLevel(level);
    e.setUserMessage(message);
    e.setCounterKey(counter);

    Debug.Log.Location.Builder l = e.getLocationBuilder();
    l.setFile(file);
    l.setLineNumber(lno);
  }
}
