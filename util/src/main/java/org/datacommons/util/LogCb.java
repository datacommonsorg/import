package org.datacommons.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Debug;

// A class to hold information for logging errors or warnings
public class LogCb {
  public static final String VALUE_KEY = "value";
  public static final String COLUMN_KEY = "column";
  public static final String PROP_KEY = "property";
  public static final String NODE_KEY = "node";
  private static final List<String> messageDetailsKeys =
      Arrays.asList(VALUE_KEY, COLUMN_KEY, PROP_KEY, NODE_KEY);
  private final LogWrapper logCtx;
  private final Debug.Log.Level logLevel;
  private final long lineNum;
  private Map<String, String> messageDetails = new HashMap<>();
  private String counter_prefix = "";
  private String counter_suffix = "";

  public LogCb(LogWrapper logCtx, Debug.Log.Level logLevel, long lineNum) {
    this.logCtx = logCtx;
    this.logLevel = logLevel;
    this.lineNum = lineNum;
  }

  public org.datacommons.util.LogCb setDetail(String key, String val) {
    this.messageDetails.put(key, val);
    return this;
  }

  public org.datacommons.util.LogCb setCounterPrefix(String prefix) {
    this.counter_prefix = prefix;
    return this;
  }

  public org.datacommons.util.LogCb setCounterSuffix(String suffix) {
    this.counter_suffix = suffix;
    return this;
  }

  public void logError(String counter, String problemMessage) {
    if (!counter_prefix.isEmpty()) {
      counter = counter_prefix + "_" + counter;
    }
    if (!counter_suffix.isEmpty()) {
      counter = counter + "_" + counter_suffix;
    }
    String message = problemMessage + " :: ";
    boolean isFirstDetail = true;
    for (String detailKey : messageDetailsKeys) {
      if (messageDetails.containsKey(detailKey)) {
        message +=
            (isFirstDetail ? "" : ", ") + detailKey + ": '" + messageDetails.get(detailKey) + "'";
        isFirstDetail = false;
      }
    }
    this.logCtx.addEntry(this.logLevel, counter, message, this.lineNum);
  }
}
