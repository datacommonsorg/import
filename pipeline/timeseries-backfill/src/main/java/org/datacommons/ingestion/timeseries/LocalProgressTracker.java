package org.datacommons.ingestion.timeseries;

import com.google.cloud.Timestamp;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;

/** Emits local progress logs for validator and DirectRunner runs. */
final class LocalProgressTracker implements AutoCloseable {
  interface LogSink {
    void info(String message);
  }

  private final String runnerName;
  private final int progressEverySourceRows;
  private final int heartbeatSeconds;
  private final LogSink logSink;
  private final LongSupplier currentTimeMillis;
  private final ScheduledExecutorService heartbeatExecutor;
  private final AtomicLong sourceRows;
  private final AtomicLong timeSeriesRows;
  private final AtomicLong timeSeriesAttributeRows;
  private final AtomicLong statVarObservationRows;
  private final AtomicLong lastRowProgressMillis;
  private final AtomicBoolean closed;

  LocalProgressTracker(
      String runnerName, int progressEverySourceRows, int heartbeatSeconds, Logger logger) {
    this(
        runnerName,
        progressEverySourceRows,
        heartbeatSeconds,
        logger::info,
        System::currentTimeMillis,
        createHeartbeatExecutor(runnerName, heartbeatSeconds));
  }

  LocalProgressTracker(
      String runnerName,
      int progressEverySourceRows,
      int heartbeatSeconds,
      LogSink logSink,
      LongSupplier currentTimeMillis,
      ScheduledExecutorService heartbeatExecutor) {
    this.runnerName = runnerName;
    this.progressEverySourceRows = progressEverySourceRows;
    this.heartbeatSeconds = heartbeatSeconds;
    this.logSink = logSink;
    this.currentTimeMillis = currentTimeMillis;
    this.heartbeatExecutor = heartbeatExecutor;
    this.sourceRows = new AtomicLong();
    this.timeSeriesRows = new AtomicLong();
    this.timeSeriesAttributeRows = new AtomicLong();
    this.statVarObservationRows = new AtomicLong();
    this.lastRowProgressMillis = new AtomicLong(currentTimeMillis.getAsLong());
    this.closed = new AtomicBoolean();

    if (heartbeatExecutor != null) {
      heartbeatExecutor.scheduleAtFixedRate(
          this::logHeartbeatNow, heartbeatSeconds, heartbeatSeconds, TimeUnit.SECONDS);
    }
  }

  boolean isEnabled() {
    return progressEverySourceRows > 0 || heartbeatSeconds > 0;
  }

  void recordRow(int timeSeriesAttributeRowsDelta, int statVarObservationRowsDelta) {
    long sourceRowCount = sourceRows.incrementAndGet();
    timeSeriesRows.incrementAndGet();
    timeSeriesAttributeRows.addAndGet(timeSeriesAttributeRowsDelta);
    statVarObservationRows.addAndGet(statVarObservationRowsDelta);
    lastRowProgressMillis.set(currentTimeMillis.getAsLong());

    if (progressEverySourceRows > 0 && sourceRowCount % progressEverySourceRows == 0) {
      logSink.info(buildProgressMessage("progress"));
    }
  }

  void recordValidatorFlush(int mutationCount, Timestamp writeTimestamp) {
    logSink.info(
        runnerName + " flush: mutations=" + mutationCount + ", write_timestamp=" + writeTimestamp);
  }

  void logHeartbeatNow() {
    if (heartbeatSeconds <= 0 || closed.get()) {
      return;
    }
    logSink.info(buildProgressMessage("heartbeat"));
  }

  @Override
  public void close() {
    if (!closed.compareAndSet(false, true)) {
      return;
    }
    if (heartbeatExecutor != null) {
      heartbeatExecutor.shutdownNow();
    }
  }

  private String buildProgressMessage(String kind) {
    long sourceRowCount = sourceRows.get();
    long idleSeconds =
        TimeUnit.MILLISECONDS.toSeconds(
            currentTimeMillis.getAsLong() - lastRowProgressMillis.get());
    StringBuilder message =
        new StringBuilder()
            .append(runnerName)
            .append(' ')
            .append(kind)
            .append(": source_rows=")
            .append(sourceRowCount)
            .append(", timeseries_rows=")
            .append(timeSeriesRows.get())
            .append(", timeseries_attribute_rows=")
            .append(timeSeriesAttributeRows.get())
            .append(", stat_var_observation_rows=")
            .append(statVarObservationRows.get())
            .append(", seconds_since_last_row=")
            .append(idleSeconds);
    if (sourceRowCount == 0) {
      message.append(", no_source_rows_yet=true");
    }
    return message.toString();
  }

  private static ScheduledExecutorService createHeartbeatExecutor(
      String runnerName, int heartbeatSeconds) {
    if (heartbeatSeconds <= 0) {
      return null;
    }
    ThreadFactory threadFactory =
        runnable -> {
          Thread thread = new Thread(runnable, runnerName + "-progress-heartbeat");
          thread.setDaemon(true);
          return thread;
        };
    return Executors.newSingleThreadScheduledExecutor(threadFactory);
  }
}
