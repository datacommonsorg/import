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

import static org.datacommons.util.McfUtil.getPropVals;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Does additional transformations on parsed MCF nodes
// - expanding ComplexValues into nodes
// - adding constraintProperties to StatVar nodes (unless explicitly declared on the node)
//
// TODO: Pass in a separate SV nodes to clean SVObs double values.
public class McfMutator {
  private Mcf.McfGraph.Builder graph;
  private LogWrapper logCtx;
  private boolean isBaseDc;
  private boolean generateFallbacks;

  /**
   * Mutates the MCF graph.
   *
   * @param graph The MCF graph builder to mutate.
   * @param logCtx The log context for reporting errors.
   * @param isBaseDc Whether the run is for Base DC. When false, automatic generation of the
   *     "definition" field on StatisticalVariables is skipped to avoid creating synthetic edges.
   * @param generateFallbacks Whether to generate fallback properties like name and definition.
   * @return The mutated MCF graph.
   */
  public static Mcf.McfGraph mutate(
      Mcf.McfGraph.Builder graph, LogWrapper logCtx, boolean isBaseDc, boolean generateFallbacks) {
    McfMutator m = new McfMutator();
    m.graph = graph;
    m.logCtx = logCtx;
    m.isBaseDc = isBaseDc;
    m.generateFallbacks = generateFallbacks;
    // Make a list of node names because we don't want to be iterating over the map
    // while mutating
    // it.
    List<String> nodeIdList = new ArrayList<>();
    graph.getNodesMap().forEach((k, v) -> nodeIdList.add(k));
    for (String nodeId : nodeIdList) {
      m.graph.putNodes(nodeId, m.mutateNode(nodeId, m.graph.getNodesOrThrow(nodeId).toBuilder()));
    }
    return m.graph.build();
  }

  private Mcf.McfGraph.PropertyValues mutateNode(
      String nodeId, Mcf.McfGraph.PropertyValues.Builder node) {
    List<String> types = getPropVals(node.build(), Vocabulary.TYPE_OF);
    if (types == null || types.isEmpty()) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Mutator_MissingTypeOf",
          "Missing typeOf value for node :: node: '" + nodeId + "'",
          node.getLocationsList());
      return node.build();
    }
    boolean isLegacyObs = false;
    boolean isStatVar = false;
    for (String type : types) {
      if (Vocabulary.isLegacyObservation(type)) {
        isLegacyObs = true;
      }
      if (Vocabulary.STAT_VAR_TYPE.equals(type)) {
        isStatVar = true;
      }
    }
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      Mcf.McfGraph.Values.Builder vb = Mcf.McfGraph.Values.newBuilder();
      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        Mcf.McfGraph.TypedValue.Builder tvb = tv.toBuilder();
        if (isLegacyObs && Vocabulary.isStatValueProperty(prop)) {
          if (tv.getType() != Mcf.ValueType.NUMBER && tv.getType() != Mcf.ValueType.TEXT) {
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Mutator_InvalidObsValue",
                "Unexpected type for Observation value; must be number or text :: type: '"
                    + tv.getType().name()
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
                node.getLocationsList());
            return node.build();
          }
          tvb.setValue(prepForDoubleConversion(tv.getValue()));
        }

        if (tv.getType() == Mcf.ValueType.COMPLEX_VALUE) {
          Mcf.McfGraph.PropertyValues.Builder complexNode =
              Mcf.McfGraph.PropertyValues.newBuilder();
          ComplexValueParser cvParser =
              new ComplexValueParser(
                  nodeId, node.build(), prop, tv.getValue(), complexNode, logCtx);
          if (cvParser.parse()) {
            tvb.setValue(cvParser.getDcid());
            tvb.setType(Mcf.ValueType.RESOLVED_REF);
            graph.putNodes(cvParser.getDcid(), complexNode.build());
          }
        }
        vb.addTypedValues(tvb.build());
      }
      node.putPvs(prop, vb.build());
    }

    if (isStatVar) {
      Map<String, Mcf.McfGraph.Values> constraintPvs = new TreeMap<>();
      List<String> declaredConstraintProps = getDeclaredConstraintProps(node);
      boolean hasDeclaredConstraintProps = !declaredConstraintProps.isEmpty();
      if (hasDeclaredConstraintProps) {
        // Respect an explicitly declared constraintProperties list: the declared properties
        // define the variable, and any other property on the node is treated as metadata.
        for (String prop : declaredConstraintProps) {
          if (Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(prop)) {
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_WARNING,
                "Mutator_InvalidConstraintProperty",
                "StatVar declares a non-constraint property in constraintProperties :: property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
                node.getLocationsList());
            continue;
          }
          Mcf.McfGraph.Values vals = getPv(node, prop);
          if (vals == null || vals.getTypedValuesCount() == 0) {
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_WARNING,
                "Mutator_MissingConstraintPropertyValue",
                "StatVar declares a constraint property that has no value :: property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
                node.getLocationsList());
            continue;
          }
          constraintPvs.put(prop, vals);
        }
      } else {
        for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
          String prop = McfUtil.stripNamespace(pv.getKey());
          if (!Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(prop)) {
            constraintPvs.put(prop, pv.getValue());
          }
        }
      }

      // Write the validated, de-duplicated constraintProperties to the node (or remove
      // constraintProperties if none of the declared properties are valid).
      removePv(node, Vocabulary.CONSTRAINT_PROPS);
      if (!constraintPvs.isEmpty()) {
        Mcf.McfGraph.Values.Builder valuesBuilder = Mcf.McfGraph.Values.newBuilder();
        for (String propDcid : constraintPvs.keySet()) {
          valuesBuilder.addTypedValues(
              Mcf.McfGraph.TypedValue.newBuilder()
                  .setValue(propDcid)
                  .setType(Mcf.ValueType.RESOLVED_REF)
                  .build());
        }
        node.putPvs(Vocabulary.CONSTRAINT_PROPS, valuesBuilder.build());
      }

      if (generateFallbacks) {
        if (isBaseDc && !node.containsPvs(Vocabulary.DEFINITION)) {
          String definition = McfUtil.generateSVDefinition(node, constraintPvs);
          Mcf.McfGraph.Values.Builder valuesBuilder =
              Mcf.McfGraph.Values.newBuilder()
                  .addTypedValues(
                      Mcf.McfGraph.TypedValue.newBuilder()
                          .setValue(definition)
                          .setType(Mcf.ValueType.TEXT)
                          .build());
          node.putPvs(Vocabulary.DEFINITION, valuesBuilder.build());
        }

        if (!node.containsPvs(Vocabulary.NAME)) {
          String name = StatVarNameGenerator.generateName(node.build(), constraintPvs.keySet());
          if (!name.isEmpty()) {
            Mcf.McfGraph.Values.Builder valuesBuilder =
                Mcf.McfGraph.Values.newBuilder()
                    .addTypedValues(
                        Mcf.McfGraph.TypedValue.newBuilder()
                            .setValue(name)
                            .setType(Mcf.ValueType.TEXT)
                            .build());
            node.putPvs(Vocabulary.NAME, valuesBuilder.build());
          }
        }
      }
    }

    return node.build();
  }

  // Returns the namespace-stripped, de-duplicated properties listed in the node's
  // constraintProperties, in declaration order. Returns an empty list if none are declared.
  private static List<String> getDeclaredConstraintProps(
      Mcf.McfGraph.PropertyValuesOrBuilder node) {
    Set<String> props = new LinkedHashSet<>();
    Mcf.McfGraph.Values declared = getPv(node, Vocabulary.CONSTRAINT_PROPS);
    if (declared != null) {
      for (Mcf.McfGraph.TypedValue tv : declared.getTypedValuesList()) {
        String prop = McfUtil.stripNamespace(tv.getValue().trim());
        if (!prop.isEmpty()) {
          props.add(prop);
        }
      }
    }
    return new ArrayList<>(props);
  }

  private static Mcf.McfGraph.Values getPv(
      Mcf.McfGraph.PropertyValuesOrBuilder node, String targetProp) {
    Mcf.McfGraph.Values vals = node.getPvsMap().get(targetProp);
    if (vals != null) {
      return vals;
    }
    String strippedTarget = McfUtil.stripNamespace(targetProp);
    for (Map.Entry<String, Mcf.McfGraph.Values> entry : node.getPvsMap().entrySet()) {
      if (McfUtil.stripNamespace(entry.getKey()).equals(strippedTarget)) {
        return entry.getValue();
      }
    }
    return null;
  }

  private static void removePv(Mcf.McfGraph.PropertyValues.Builder node, String targetProp) {
    String strippedTarget = McfUtil.stripNamespace(targetProp);
    List<String> keysToRemove = new ArrayList<>();
    for (String key : node.getPvsMap().keySet()) {
      if (McfUtil.stripNamespace(key).equals(strippedTarget)) {
        keysToRemove.add(key);
      }
    }
    for (String key : keysToRemove) {
      node.removePvs(key);
    }
  }

  private static String prepForDoubleConversion(String v) {
    return v.replace(" ", "").replace(",", "").replace("%", "");
  }
}
