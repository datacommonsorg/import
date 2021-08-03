package org.datacommons.util;

import org.datacommons.proto.Debug;

class LogWrapper {
  private Debug.Log.Builder logCtx;
  private String fileName;

  public LogWrapper(Debug.Log.Builder logCtx, String fileName) {
    logCtx = logCtx;
    fileName = fileName;
  }

  public void AddLog(Debug.Log.Level level, String counter, String message, long lno) {
    if (logCtx == null) return;
    AddLog(level, counter, message, lno, -1 , null);
  }

  public void AddLog(Debug.Log.Level level, String counter, String message, long lno, long cno) {
    if (logCtx == null) return;
    AddLog(level, counter, message, lno, cno, null);
  }

  public void AddLog(Debug.Log.Level level, String counter, String message, long lno,
                     String cname) {
    if (logCtx == null) return;
    AddLog(level, counter, message, lno, -1, cname);
  }

  private void AddLog(Debug.Log.Level level, String counter, String message, long lno, long cno,
                      String cname) {
    Debug.Log.Entry.Builder e = logCtx.addEntriesBuilder();
    e.setLevel(level);
    e.setUserMessage(message);
    if (counter != null && !counter.isEmpty()) {
      e.setCounterKey(counter);
      logCtx.getCounterSetBuilder().getCountersMap().put(counter,
              logCtx.getCounterSet().getCountersOrDefault(counter, 0) + 1);
    }
    Debug.Log.Location.Builder l = e.getLocationBuilder();
    l.setFile(fileName);
    l.setLineNumber(lno);
    if (cno > 0) l.setColumnNumber(cno);
    if (cname != null && !cname.isEmpty()) l.setColumnName(cname);
  }
}
