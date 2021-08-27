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

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.regex.Pattern;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// A container class of MCF related utilities.
public class McfUtil {
  // Serializes an MCFGraph proto into a string.  Sorts output if |sort| is true.
  public static String serializeMcfGraph(Mcf.McfGraph graph, boolean sort) {
    StringBuilder result = new StringBuilder();
    String sentinel = graph.getType() == Mcf.McfType.TEMPLATE_MCF ? "Template" : "Node";
    List<String> keys = new ArrayList<>(graph.getNodesMap().keySet());
    if (sort) Collections.sort(keys);
    for (String key : keys) {
      Mcf.McfGraph.PropertyValues node = graph.getNodesMap().get(key);
      result.append(sentinel + ": " + key + "\n");
      List<String> lines = new ArrayList<>();
      for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
        if (pv.getValue().getTypedValuesCount() == 0) continue;
        List<String> vals = new ArrayList<>();
        for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
          String value = getValue(tv);
          if (!value.isEmpty()) vals.add(value);
        }
        if (sort) Collections.sort(vals);
        if (!vals.isEmpty()) {
          lines.add(pv.getKey() + ": " + String.join(", ", vals) + "\n");
        }
      }
      if (sort) Collections.sort(lines);
      result.append(String.join("", lines));
      result.append("\n");
    }
    return result.toString();
  }

  public static String serializeMcfNode(
      String nodeId, Mcf.McfGraph.PropertyValues node, boolean sort) {
    Mcf.McfGraph.Builder g = Mcf.McfGraph.newBuilder();
    g.putNodes(nodeId, node);
    return serializeMcfGraph(g.build(), sort);
  }

  public static String getPropVal(Mcf.McfGraph.PropertyValues node, String property) {
    String val = "";
    try {
      Mcf.McfGraph.Values vals = node.getPvsOrThrow(property);
      if (vals.getTypedValuesCount() > 0) {
        val = stripNamespace(vals.getTypedValues(0).getValue());
      }
    } catch (IllegalArgumentException ex) {
      // Not having a value is not an error.
    }
    return val;
  }

  public static List<String> getPropVals(Mcf.McfGraph.PropertyValues node, String property) {
    List<String> result = new ArrayList<>();
    try {
      Mcf.McfGraph.Values vals = node.getPvsOrThrow(property);
      if (vals.getTypedValuesCount() > 0) {
        for (Mcf.McfGraph.TypedValue tv : vals.getTypedValuesList()) {
          result.add(stripNamespace(tv.getValue()));
        }
      }
    } catch (IllegalArgumentException ex) {
      // Not having a value is not an error.
    }
    return result;
  }

  public static List<Mcf.McfGraph.TypedValue> getPropTvs(
      Mcf.McfGraph.PropertyValues node, String property) {
    try {
      return node.getPvsOrThrow(property).getTypedValuesList();
    } catch (IllegalArgumentException ex) {
      // Not having a value is not an error.
    }
    return null;
  }

  public static Mcf.McfGraph.Values newValues(Mcf.ValueType type, String value) {
    Mcf.McfGraph.Values.Builder vals = Mcf.McfGraph.Values.newBuilder();
    Mcf.McfGraph.TypedValue.Builder tv = vals.addTypedValuesBuilder();
    tv.setType(type);
    tv.setValue(value);
    return vals.build();
  }

  // Given a list of MCF graphs, merges common nodes and de-duplicates PVs.
  public static Mcf.McfGraph mergeGraphs(List<Mcf.McfGraph> graphs) throws AssertionError {
    if (graphs.isEmpty()) {
      throw new AssertionError("mergeGraphs called with empty graphs!");
    }

    // node-id -> {prop -> vals}
    HashMap<String, HashMap<String, HashSet<Mcf.McfGraph.TypedValue>>> dedupMap = new HashMap<>();
    // node-id -> locations
    HashMap<String, List<Debug.Log.Location>> locationMap = new HashMap<>();

    for (Mcf.McfGraph graph : graphs) {
      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
        for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getValue().getPvsMap().entrySet()) {
          if (!dedupMap.containsKey(node.getKey())) {
            dedupMap.put(node.getKey(), new HashMap<>());
          }
          if (!dedupMap.get(node.getKey()).containsKey(pv.getKey())) {
            dedupMap.get(node.getKey()).put(pv.getKey(), new HashSet<>());
          }
          for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
            dedupMap.get(node.getKey()).get(pv.getKey()).add(tv);
          }
        }
        for (Debug.Log.Location loc : node.getValue().getLocationsList()) {
          if (!locationMap.containsKey(node.getKey())) {
            locationMap.put(node.getKey(), new ArrayList<Debug.Log.Location>());
          }
          locationMap.get(node.getKey()).add(loc);
        }
      }
    }

    Mcf.McfGraph.Builder result = Mcf.McfGraph.newBuilder();
    result.setType(graphs.get(0).getType());
    for (Map.Entry<String, HashMap<String, HashSet<Mcf.McfGraph.TypedValue>>> node :
        dedupMap.entrySet()) {
      Mcf.McfGraph.PropertyValues.Builder pvs =
          result
              .getNodesOrDefault(node.getKey(), Mcf.McfGraph.PropertyValues.getDefaultInstance())
              .toBuilder();
      for (Map.Entry<String, HashSet<Mcf.McfGraph.TypedValue>> pv : node.getValue().entrySet()) {
        Mcf.McfGraph.Values.Builder tvs =
            pvs.getPvsOrDefault(pv.getKey(), Mcf.McfGraph.Values.getDefaultInstance()).toBuilder();
        for (Mcf.McfGraph.TypedValue tv : pv.getValue()) {
          tvs.addTypedValues(tv);
        }
        pvs.putPvs(pv.getKey(), tvs.build());
      }
      if (locationMap.containsKey(node.getKey())) {
        pvs.addAllLocations(locationMap.get(node.getKey()));
      }
      result.putNodes(node.getKey(), pvs.build());
    }
    return result.build();
  }

  public static String stripNamespace(String val) {
    if (val.startsWith(Vocabulary.DCID_PREFIX)
        || val.startsWith(Vocabulary.SCHEMA_ORG_PREFIX)
        || val.startsWith(Vocabulary.DC_SCHEMA_PREFIX)) {
      return val.substring(val.indexOf(Vocabulary.REFERENCE_DELIMITER) + 1);
    }
    return val;
  }

  private static String getValue(Mcf.McfGraph.TypedValue typedValue) {
    if (typedValue.getType() == Mcf.ValueType.TEXT) {
      return "\"" + typedValue.getValue() + "\"";
    } else if (typedValue.getType() == Mcf.ValueType.RESOLVED_REF) {
      return Vocabulary.DCID_PREFIX + typedValue.getValue();
    } else {
      return typedValue.getValue();
    }
  }

  // From https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatterBuilder.html
  private static final List<String> DATE_PATTERNS =
      List.of(
          "yyyy",
          "yyyy-MM",
          "yyyyMM",
          "yyyy-M",
          "yyyy-MM-dd",
          "yyyyMMdd",
          "yyyy-M-d",
          "yyyy-MM-dd'T'HH:mm",
          "yyyy-MM-dd'T'HH:mm:ss",
          "yyyy-MM-dd'T'HH:mm:ss.SSS",
          "yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
  // The Java API does not match 20071, 2007101, so add these for compatibility with CPP
  // implementation.
  private static final List<String> EXTRA_DATE_PATTERNS = List.of("^\\d{5}$", "^\\d{7}$");

  public static boolean isValidISO8601Date(String dateValue) {
    for (String pattern : DATE_PATTERNS) {
      try {
        DateTimeFormatter.ofPattern(pattern, Locale.ENGLISH).parse(dateValue);
        return true;
      } catch (DateTimeParseException ex) {
        // Pass through
      }
    }
    for (String pattern : EXTRA_DATE_PATTERNS) {
      if (Pattern.matches(pattern, dateValue)) {
        return true;
      }
    }
    return false;
  }

  public static boolean isNumber(String val) {
    try {
      long l = Long.parseLong(val);
      return true;
    } catch (NumberFormatException e) {
    }
    try {
      long l = Long.parseUnsignedLong(val);
      return true;
    } catch (NumberFormatException e) {
    }
    try {
      double d = Double.parseDouble(val);
      return true;
    } catch (NumberFormatException e) {
    }
    return false;
  }

  public static boolean isBool(String val) {
    String v = val.toLowerCase();
    return v.equals("true") || v.equals("1") || v.equals("false") || v.equals("0");
  }

  // A class to hold information for logging errors or warnings
  public static class LogCb {
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

    public LogCb setDetail(String key, String val) {
      this.messageDetails.put(key, val);
      return this;
    }

    public LogCb setCounterPrefix(String prefix) {
      this.counter_prefix = prefix;
      return this;
    }

    public LogCb setCounterSuffix(String suffix) {
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
}
