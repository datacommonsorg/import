package org.datacommons.util;

import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

import java.util.List;
import java.util.Map;

import static org.datacommons.util.McfUtil.getPropVal;
import static org.datacommons.util.McfUtil.getPropVals;

// Does additional transformations on parsed MCF nodes, like expanding ComplexValues into nodes.
public class McfMutator {
  private Mcf.McfGraph graph;
  private LogWrapper logCtx;

  public McfMutator(Mcf.McfGraph graph, LogWrapper logCtx) {
    this.graph = graph;
    this.logCtx = logCtx;
  }

  public void apply() {
    for (String nodeId : graph.getNodesMap().keySet()) {
      Mcf.McfGraph.PropertyValues node = graph.toBuilder().getNodesOrThrow(nodeId);
      applyOnNode(nodeId, node);
      graph.toBuilder().putNodes(nodeId, node);
    }
  }

  private void applyOnNode(String nodeId, Mcf.McfGraph.PropertyValues node) {
    List<String> types = getPropVals(node, Vocabulary.TYPE_OF);
    if (types == null) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Schema_MissingTypeOf",
          "Missing typeOf value for node " + nodeId,
          node.getLocationsList());
      return;
    }
    String statTypeVal = getPropVal(node, Vocabulary.STAT_TYPE);
    boolean isObs = false;
    boolean isStatVarObs = false;
    for (String type : types) {
      if (Vocabulary.isObservation(type)) {
        isStatVarObs = Vocabulary.isStatVarObs(type);
        isObs = true;
        break;
      }
    }
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        if ((isStatVarObs && statTypeVal != null && Vocabulary.isStatValueProperty(statTypeVal))
            || (isObs && Vocabulary.isStatValueProperty(prop))) {
          if (tv.getType() != Mcf.ValueType.NUMBER && tv.getType() != Mcf.ValueType.TEXT) {
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Schema_InvalidObsValue",
                "Wrong inferred type " + tv.getType() + " for property " + prop + " in " + nodeId,
                node.getLocationsList());
            return;
          }
          tv.toBuilder().setValue(prepForDoubleConversion(tv.getValue()));
        }

        if (tv.getType() == Mcf.ValueType.COMPLEX_VALUE) {
          Mcf.McfGraph.PropertyValues.Builder complexNode =
              Mcf.McfGraph.PropertyValues.newBuilder();
          ComplexValueParser cvParser = new ComplexValueParser(nodeId, node, prop, tv.getValue(),
                  complexNode, logCtx);
          if (cvParser.parse()) {
            tv.toBuilder().setValue(cvParser.getDcid());
            tv.toBuilder().setType(Mcf.ValueType.RESOLVED_REF);
            graph.toBuilder().putNodes(cvParser.getDcid(), complexNode.build());
          }
        }
      }
    }
  }

  private static String prepForDoubleConversion(String v) {
    return v.replace(" ", "").replace(",", "").replace("%", "");
  }
}
