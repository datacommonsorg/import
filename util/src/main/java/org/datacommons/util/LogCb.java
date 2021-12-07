package org.datacommons.util;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// A class to hold information for logging errors or warnings
public class LogCb {
  public static final String VALUE_KEY = "value";
  public static final String PREF_KEY = "property-ref";
  public static final String VREF_KEY = "value-ref";
  public static final String COLUMN_KEY = "column";
  public static final String PROP_KEY = "property";
  public static final String SUB_KEY = "subject";
  public static final String PRED_KEY = "predicate";
  public static final String OBJ_KEY = "object";
  public static final String NODE_KEY = "node";
  // This specifies the order in which the keys should be displayed.
  private static final List<String> messageDetailsKeys =
      Arrays.asList(
          VALUE_KEY,
          PREF_KEY,
          VREF_KEY,
          COLUMN_KEY,
          PROP_KEY,
          SUB_KEY,
          PRED_KEY,
          OBJ_KEY,
          NODE_KEY);
  private final LogWrapper logCtx;
  private final Debug.Log.Level logLevel;
  private Mcf.McfGraph.PropertyValues node; // Optional
  private long lineNum;
  private String fileName;
  private Map<String, String> messageDetails = new HashMap<>();
  private String counter_prefix = "";
  private String counter_suffix = "";

  public LogCb(LogWrapper logCtx, Debug.Log.Level logLevel, String fileName, long lineNum) {
    this.logCtx = logCtx;
    this.logLevel = logLevel;
    this.fileName = fileName;
    this.lineNum = lineNum;
  }

  public LogCb(LogWrapper logCtx, Debug.Log.Level logLevel, Mcf.McfGraph.PropertyValues node) {
    this.logCtx = logCtx;
    this.logLevel = logLevel;
    this.node = node;
  }

  public org.datacommons.util.LogCb setDetail(String key, String val) {
    messageDetails.put(key, val);
    return this;
  }

  public org.datacommons.util.LogCb setCounterPrefix(String prefix) {
    counter_prefix = prefix;
    return this;
  }

  public org.datacommons.util.LogCb setCounterSuffix(String suffix) {
    counter_suffix = suffix;
    return this;
  }

  public void logError(String counter, String problemMessage) {
    if (!counter_prefix.isEmpty()) {
      counter = counter_prefix + "_" + counter;
    }
    if (!counter_suffix.isEmpty()) {
      counter = counter + "_" + counter_suffix;
    }
    String finalMessage = getFinalMessage(problemMessage);
    if (node != null) {
      logCtx.addEntry(logLevel, counter, finalMessage, node.getLocationsList());
    } else {
      logCtx.addEntry(logLevel, counter, finalMessage, fileName, lineNum);
    }
  }

  private String getFinalMessage(String problemMessage) {
    if (messageDetails.isEmpty()) return problemMessage;

    StringBuilder message = new StringBuilder();
    message.append(problemMessage).append(" :: ");
    boolean isFirstDetail = true;
    for (String detailKey : messageDetailsKeys) {
      if (messageDetails.containsKey(detailKey)) {
        if (!isFirstDetail) {
          message.append(", ");
        }
        message.append(detailKey).append(": '").append(messageDetails.get(detailKey)).append("'");
        isFirstDetail = false;
      }
    }
    return message.toString();
  }
}
