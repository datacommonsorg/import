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

import static java.util.stream.Collectors.toList;

import com.google.common.collect.ImmutableSet;
import java.util.*;
import org.datacommons.proto.Debug.Log.Level;
import org.datacommons.proto.LogLocation;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.proto.Recon.EntityIds;
import org.datacommons.proto.Recon.EntitySubGraph;
import org.datacommons.proto.Recon.IdWithProperty;

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
      if (node.hasErrorMessage()) {
        // Print location and user-message.
        for (var loc : node.getLocationsList()) {
          result.append("# From " + loc.getFile() + ":" + loc.getLineNumber() + "\n");
        }
        result.append("# Error: " + node.getErrorMessage() + "\n");
      }
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
    Mcf.McfGraph.Values vals = node.getPvsOrDefault(property, null);
    if (vals != null) {
      if (vals.getTypedValuesCount() > 0) {
        val = stripNamespace(vals.getTypedValues(0).getValue());
      }
    } else {
      // Not having a value is not an error.
    }
    return val;
  }

  public static List<String> getPropVals(Mcf.McfGraph.PropertyValues node, String property) {
    List<String> result = new ArrayList<>();
    Mcf.McfGraph.Values vals = node.getPvsOrDefault(property, null);
    if (vals != null && vals.getTypedValuesCount() > 0) {
      for (Mcf.McfGraph.TypedValue tv : vals.getTypedValuesList()) {
        result.add(stripNamespace(tv.getValue()));
      }
    }
    return result;
  }

  public static List<Mcf.McfGraph.TypedValue> getPropTvs(
      Mcf.McfGraph.PropertyValues node, String property) {
    var vals = node.getPvsOrDefault(property, null);
    if (vals != null) return vals.getTypedValuesList();
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
      return Mcf.McfGraph.newBuilder().build();
    }

    // node-id -> {prop -> vals}
    HashMap<String, HashMap<String, HashSet<Mcf.McfGraph.TypedValue>>> dedupMap = new HashMap<>();
    // node-id -> locations
    HashMap<String, List<LogLocation.Location>> locationMap = new HashMap<>();

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
        for (LogLocation.Location loc : node.getValue().getLocationsList()) {
          if (!locationMap.containsKey(node.getKey())) {
            locationMap.put(node.getKey(), new ArrayList<LogLocation.Location>());
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

  // Return whether the node is a statVarObservation node with value of type number.
  public static boolean isSvObWithNumberValue(McfGraph.PropertyValues node) {
    List<String> types = McfUtil.getPropVals(node, Vocabulary.TYPE_OF);
    McfGraph.Values nodeValues =
        node.getPvsOrDefault(Vocabulary.VALUE, McfGraph.Values.getDefaultInstance());
    return types.contains(Vocabulary.STAT_VAR_OBSERVATION_TYPE)
        && nodeValues.getTypedValuesCount() != 0
        && nodeValues.getTypedValues(0).getType() == ValueType.NUMBER;
  }

  // Returns a new EntitySubGraph with sourceId = "<property>:<value>" and a single entity ID with
  // the specified property and value.
  public static EntitySubGraph newEntitySubGraph(String property, String value) {
    return newEntitySubGraph(newIdWithProperty(property, value));
  }

  public static EntitySubGraph newEntitySubGraph(IdWithProperty idWithProperty) {
    return EntitySubGraph.newBuilder()
        .setSourceId(String.format("%s:%s", idWithProperty.getProp(), idWithProperty.getVal()))
        .setEntityIds(EntityIds.newBuilder().addIds(idWithProperty))
        .build();
  }

  public static IdWithProperty newIdWithProperty(String property, String value) {
    return IdWithProperty.newBuilder().setProp(property).setVal(value).build();
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

  // Gets place resolvable and assignable ids with property from the specified node.
  static Set<IdWithProperty> getEntities(PropertyValues node) {
    ImmutableSet.Builder<IdWithProperty> builder = ImmutableSet.builder();

    for (String prop : Vocabulary.PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS) {
      if (node.containsPvs(prop)) {
        for (TypedValue typedValue : node.getPvsMap().get(prop).getTypedValuesList()) {
          if (typedValue.getType() == ValueType.TEXT || typedValue.getType() == ValueType.NUMBER) {
            builder.add(newIdWithProperty(prop, typedValue.getValue()));
          }
        }
      }
    }

    return builder.build();
  }

  static Optional<String> getResolved(
      PropertyValues node, Map<IdWithProperty, String> resolvedEntities, LogWrapper logWrapper) {
    Set<IdWithProperty> externalEntities = getEntities(node);
    Set<String> dcids =
        new LinkedHashSet<>(
            externalEntities.stream()
                .filter(resolvedEntities::containsKey)
                .map(resolvedEntities::get)
                .collect(toList()));

    if (dcids.isEmpty()) {
      return Optional.empty();
    }

    if (dcids.size() > 1) {
      logWrapper.addEntry(
          Level.LEVEL_ERROR,
          "Resolution_DivergingDcidsForExternalIds",
          String.format("Divergence found. dcids = %s, external ids = %s", dcids, externalEntities),
          node.getLocationsList());
      return Optional.empty();
    }

    return dcids.stream().findFirst();
  }
}
