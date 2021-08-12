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

import java.util.List;
import java.util.Map;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Does additional transformations on parsed MCF nodes, like expanding ComplexValues into nodes.
//
// TODO: Implement constraintProperties computation for SV (CPP Parity).
// TODO: Attach provenance maybe (CPP Parity).
// TODO: Pass in a separate SV nodes to clean SVObs double values.
public class McfMutator {
  private Mcf.McfGraph.Builder graph;
  private LogWrapper logCtx;

  public static Mcf.McfGraph mutate(Mcf.McfGraph.Builder graph, LogWrapper logCtx) {
    McfMutator m = new McfMutator();
    m.graph = graph;
    m.logCtx = logCtx;
    for (String nodeId : graph.getNodesMap().keySet()) {
      m.graph.putNodes(nodeId, m.mutateNode(nodeId, m.graph.getNodesOrThrow(nodeId).toBuilder()));
    }
    return m.graph.build();
  }

  private Mcf.McfGraph.PropertyValues mutateNode(
      String nodeId, Mcf.McfGraph.PropertyValues.Builder node) {
    List<String> types = getPropVals(node.build(), Vocabulary.TYPE_OF);
    if (types == null) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Mutator_MissingTypeOf",
          "Missing typeOf value for node " + nodeId,
          node.getLocationsList());
      return node.build();
    }
    boolean isLegacyObs = false;
    for (String type : types) {
      if (Vocabulary.isLegacyObservation(type)) {
        isLegacyObs = true;
        break;
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
                "Wrong inferred type " + tv.getType() + " for property " + prop + " in " + nodeId,
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
    return node.build();
  }

  private static String prepForDoubleConversion(String v) {
    return v.replace(" ", "").replace(",", "").replace("%", "");
  }
}
