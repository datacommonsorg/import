package org.datacommons.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;

class LogWrapper {
  private static final Logger logger = LogManager.getLogger(McfParser.class);

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

  private void addLog(
      Debug.Log.Level level, String counter, String message, long lno, long cno, String cname) {
    if (level == Debug.Log.Level.LEVEL_ERROR || level == Debug.Log.Level.LEVEL_FATAL) {
      logger.error("{} - @{}:{} - {} - {}", level.name(), fileName, lno, counter, message);
    }
    Debug.Log.Entry.Builder e = logCtx.addEntriesBuilder();
    e.setLevel(level);
    e.setUserMessage(message);

    Debug.Log.Location.Builder l = e.getLocationBuilder();
    l.setFile(fileName);
    l.setLineNumber(lno);
    if (cno > 0) l.setColumnNumber(cno);
    if (cname != null && !cname.isEmpty()) l.setColumnName(cname);

    if (counter != null && !counter.isEmpty()) {
      e.setCounterKey(counter);
      logCtx
          .getCounterSetBuilder()
          .putCounters(counter, logCtx.getCounterSet().getCountersOrDefault(counter, 0) + 1);
    }

    // Update level summary.
    logCtx.putLevelSummary(level.name(), logCtx.getLevelSummaryOrDefault(level.name(), 0));
  }
}
