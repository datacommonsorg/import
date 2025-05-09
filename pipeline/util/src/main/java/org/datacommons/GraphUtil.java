package org.datacommons;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf.MCFOptimizedGraph;
import org.datacommons.proto.Mcf.MCFStatVarObsSeries;
import org.datacommons.proto.Mcf.MCFStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfType;
import org.datacommons.proto.Mcf.ValueType;

/** Util functions for processing MCF graphs. */
public class GraphUtil {

  enum Property {
    typeOf,
    observationAbout,
    variableMeasured,
    observationDate,
    observationPeriod,
    measurementMethod,
    unit,
    value,
    scalingFactor,
    dcid;
  }

  public static final String STAT_VAR_OB = "StatVarObservation";

  /**
   * Flattens an optimized MCF graph into a list of graph node
   *
   * @param optimized_graph input optimized graph
   * @return list of graph nodes
   */
  public static List<McfGraph> ConvertFromMCFStatVarObsSeries(MCFOptimizedGraph optimized_graph) {
    MCFStatVarObsSeries.Key s_key = optimized_graph.getSvObsSeries().getKey(); // Obs series key.

    List<McfGraph> mcf_graphs = new ArrayList<>();
    McfGraph.PropertyValues.Builder base_node = McfGraph.PropertyValues.newBuilder();

    // Set required common PVs.
    SetPropVal(Property.typeOf.name(), ValueType.RESOLVED_REF, STAT_VAR_OB, base_node);
    SetPropVal(
        Property.observationAbout.name(),
        ValueType.RESOLVED_REF,
        s_key.getObservationAbout(),
        base_node);
    SetPropVal(
        Property.variableMeasured.name(),
        ValueType.RESOLVED_REF,
        s_key.getVariableMeasured(),
        base_node);

    // Set optional common PVs.
    String m;
    if (!(m = s_key.getMeasurementMethod()).isEmpty()) {
      SetPropVal(Property.measurementMethod.name(), ValueType.RESOLVED_REF, m, base_node);
    }
    if (!(m = s_key.getObservationPeriod()).isEmpty()) {
      SetPropVal(Property.observationPeriod.name(), ValueType.TEXT, m, base_node);
    }
    if (!(m = s_key.getScalingFactor()).isEmpty()) {
      SetPropVal(Property.scalingFactor.name(), ValueType.NUMBER, m, base_node);
    }
    if (!(m = s_key.getUnit()).isEmpty()) {
      SetPropVal(Property.unit.name(), ValueType.TEXT, m, base_node);
    }
    McfGraph.PropertyValues base = base_node.build();

    for (StatVarObs o : optimized_graph.getSvObsSeries().getSvObsListList()) {
      // Copy common PVs.
      McfGraph.PropertyValues.Builder node = base.toBuilder();
      // Set required PVs.
      SetPropVal(Property.dcid.name(), ValueType.TEXT, o.getDcid(), node);
      SetPropVal(Property.observationDate.name(), ValueType.TEXT, o.getDate(), node);
      if (o.hasNumber()) {
        SetPropVal(Property.value.name(), ValueType.NUMBER, Double.toString(o.getNumber()), node);
      } else if (o.hasText()) {
        SetPropVal(Property.value.name(), ValueType.TEXT, o.getText(), node);
      }

      // Set optional PVs.
      for (Map.Entry<String, McfGraph.Values> entry : o.getPvs().getPvsMap().entrySet()) {
        node.putPvs(entry.getKey(), entry.getValue());
      }

      McfGraph.Builder g = McfGraph.newBuilder();
      g.setType(McfType.INSTANCE_MCF);
      String node_id = o.getLocalNodeId().isEmpty() ? o.getDcid() : o.getLocalNodeId();
      g.putNodes(node_id, node.build());
      mcf_graphs.add(g.build());
    }

    return mcf_graphs;
  }

  /**
   * Updates a PV in an McfGraph instance
   *
   * @param prop property to update
   * @param val_type value type
   * @param val property value
   * @param node node to update
   */
  public static void SetPropVal(
      String prop, ValueType val_type, String val, McfGraph.PropertyValues.Builder node) {
    McfGraph.TypedValue tv =
        McfGraph.TypedValue.newBuilder().setType(val_type).setValue(val).build();
    McfGraph.Values v = McfGraph.Values.newBuilder().addTypedValues(tv).build();
    node.putPvs(prop, v);
  }

  /**
   * Gets value for a property from Graph node
   *
   * @param node PVs to read value from
   * @param prop property to fetch
   * @return value for the property
   */
  public static String GetPropVal(McfGraph.PropertyValues node, String prop) {
    McfGraph.Values v = node.getPvsMap().get(prop);
    if (v != null && v.getTypedValuesCount() > 0) {
      String val = v.getTypedValues(0).getValue();
      if (val.startsWith("dcid:") || val.startsWith("dcs:") || val.startsWith("schema:")) {
        return val.substring(val.indexOf(':') + 1);
      } else {
        return val;
      }
    }
    return "";
  }

  /**
   * Gets value for a property from a map of PVs
   *
   * @param pvs a map of property-values
   * @param key property to fetch
   * @return property value
   */
  private static String GetPropertyValue(Map<String, McfGraph.Values> pvs, String key) {
    String value = pvs.get(key) != null ? pvs.get(key).getTypedValues(0).getValue() : "";
    if (value.contains(",")) {
      value = String.format("\"%s\"", value);
    }
    return value;
  }

  /**
   * Returns a key-val pair from PVs of a graph node Key: PVs excluding value Val: Value, scaling
   * factor properties
   *
   * @param node input PVs
   * @return String array of key-val pair
   */
  public static String[] GetPropValKV(McfGraph.PropertyValues node) {
    Map<String, McfGraph.Values> pvs = node.getPvsMap();
    String key =
        String.format(
            "%s,%s,%s,%s,%s,%s,%s",
            GetPropertyValue(pvs, Property.variableMeasured.name()),
            GetPropertyValue(pvs, Property.observationAbout.name()),
            GetPropertyValue(pvs, Property.observationDate.name()),
            GetPropertyValue(pvs, Property.observationPeriod.name()),
            GetPropertyValue(pvs, Property.measurementMethod.name()),
            GetPropertyValue(pvs, Property.unit.name()),
            GetPropertyValue(pvs, Property.scalingFactor.name()));

    String value = GetPropertyValue(pvs, Property.value.name());
    return new String[] {key, value};
  }

  /**
   * Converts a byte stream of (optimized graph format) into a list of flattened graph nodes
   *
   * @param element input byte stream
   * @return list of graph nodes
   */
  public static List<McfGraph> parseToGraph(byte[] element) {
    MCFOptimizedGraph optimized_graph;
    List<McfGraph> graphList = new ArrayList<>();
    try {
      optimized_graph = MCFOptimizedGraph.parseFrom(element);
      for (McfGraph g : ConvertFromMCFStatVarObsSeries(optimized_graph)) {
        graphList.add(g);
      }
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Failed to parse protocol buffer descriptor for generated code.", e);
    }
    return graphList;
  }

  /**
   * Converts an MCF string into a graph proto (only SVObs)
   *
   * @param input input mcf string
   * @return graph node
   */
  public static McfGraph convertToGraph(String input) {
    McfGraph.Builder g = McfGraph.newBuilder();
    g.setType(McfType.INSTANCE_MCF);
    String node_id = "";
    McfGraph.PropertyValues.Builder base_node = McfGraph.PropertyValues.newBuilder();
    String[] lines = input.split("\n");
    for (String line : lines) {
      line = line.trim();
      if (line.isEmpty() || line.startsWith("//") || line.startsWith("#")) {
        continue;
      }
      int colon = line.indexOf(":");
      String lhs = line.substring(0, colon).trim();
      String rhs = line.substring(colon + 1).trim();
      if (lhs.equals(Property.dcid.name())) {
        node_id = rhs;
      }
      SetPropVal(lhs, ValueType.TEXT, rhs, base_node);
    }
    McfGraph.PropertyValues pvs = base_node.build();
    if (GetPropertyValue(pvs.getPvsMap(), Property.typeOf.name())
        .equals(Property.dcid.name() + ":" + STAT_VAR_OB)) {
      g.putNodes(node_id, base_node.build());
    }
    return g.build();
  }
}