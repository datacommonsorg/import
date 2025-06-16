package org.datacommons.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.datacommons.proto.Mcf.McfStatVarObsSeries.StatVarObs;
import org.datacommons.proto.Mcf.McfType;
import org.datacommons.proto.Mcf.ValueType;

/** Util functions for processing MCF graphs. */
public class GraphUtils {
  public enum Property {
    /** Properties for a StatVarObservation. */
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

  private static final String REQ_SV_OBS_PROPS[] = {
    /** Required properties for a StatVarObservation. */
    Property.observationDate.name(),
    Property.observationAbout.name(),
    Property.variableMeasured.name(),
    Property.dcid.name(),
    Property.value.name()
  };

  private static final Set<String> SVOBS_PROTO_PROPS =
      Set.of(
          /** Standard properties expected in StatVarObservation nodes. */
          Property.typeOf.name(),
          Property.observationAbout.name(),
          Property.variableMeasured.name(),
          Property.observationDate.name(),
          Property.observationPeriod.name(),
          Property.measurementMethod.name(),
          Property.unit.name(),
          Property.value.name(),
          Property.scalingFactor.name(),
          Property.dcid.name());

  /**
   * Checks if a given property name is one of the standard SVObs properties.
   *
   * @param prop The property name to check.
   * @return True if the property is a standard SVObs property, false otherwise.
   */
  public static boolean isSvObsProp(String prop) {
    return SVOBS_PROTO_PROPS.contains(prop);
  }

  /**
   * Checks if a given set of property values represents a StatVarObservation node.
   *
   * @param pvs The property values to check.
   * @return True if the property values indicate a StatVarObservation, false otherwise.
   */
  public static boolean isObservation(PropertyValues pvs) {
    return GraphUtils.getPropertyValue(pvs.getPvsMap(), GraphUtils.Property.typeOf.name())
        .contains(GraphUtils.STAT_VAR_OB);
  }

  /**
   * Updates a PV in an McfGraph instance
   *
   * @param prop property to update
   * @param val_type value type
   * @param val property value
   * @param node node to update
   */
  public static void setPropVal(
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
  public static String getPropVal(McfGraph.PropertyValues node, String prop) {
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
  public static String getPropertyValue(Map<String, McfGraph.Values> pvs, String key) {
    String value = pvs.get(key) != null ? pvs.get(key).getTypedValues(0).getValue() : "";
    if (value.contains(",")) {
      value = String.format("\"%s\"", value);
    }
    return value;
  }

  /**
   * Returns a key-val pair from PVs of a graph node (SVObs only) Key: PVs excluding value Val:
   * Value
   *
   * @param graph input graph
   * @return list of series-value pairs
   */
  public static List<String[]> getSeriesValues(McfGraph graph) {
    List<String[]> res = new ArrayList<>();
    for (PropertyValues pvs : graph.getNodesMap().values()) {
      if (isObservation(pvs)) {
        Map<String, McfGraph.Values> pv = pvs.getPvsMap();
        String key =
            String.format(
                "%s,%s,%s,%s,%s,%s,%s",
                getPropertyValue(pv, Property.variableMeasured.name()),
                getPropertyValue(pv, Property.observationAbout.name()),
                getPropertyValue(pv, Property.observationDate.name()),
                getPropertyValue(pv, Property.observationPeriod.name()),
                getPropertyValue(pv, Property.measurementMethod.name()),
                getPropertyValue(pv, Property.unit.name()),
                getPropertyValue(pv, Property.scalingFactor.name()));
        String value = getPropertyValue(pv, Property.value.name());
        res.add(new String[] {key, value});
      }
    }
    return res;
  }

  /**
   * Gets the double value of a specific property from a graph node.
   *
   * @param node The graph node (PropertyValues) to read from.
   * @param prop The property name whose value should be retrieved as a double.
   * @return The double value of the property, or Double.NaN if the value is not a valid number.
   */
  public static Double nodeDoubleValue(McfGraph.PropertyValues node, String prop) {
    String str_val = getPropVal(node, prop);
    if (str_val.isEmpty()) throw new IllegalArgumentException("Failed to get double value.");
    try {
      double v = Double.parseDouble(str_val);
      return v;
    } catch (NumberFormatException nfe) {
      return Double.NaN;
    }
  }

  /**
   * Flattens an optimized MCF graph into a list of graph node
   *
   * @param optimized_graph input optimized graph
   * @return list of McfGraph instances, each representing a single StatVarObservation.
   */
  public static List<McfGraph> convertMcfStatVarObsSeriesToMcfGraph(
      McfOptimizedGraph optimized_graph) {
    McfStatVarObsSeries.Key s_key = optimized_graph.getSvObsSeries().getKey(); // Obs series key.

    List<McfGraph> mcf_graphs = new ArrayList<>();
    McfGraph.PropertyValues.Builder base_node = McfGraph.PropertyValues.newBuilder();

    // Set required common PVs.
    setPropVal(Property.typeOf.name(), ValueType.RESOLVED_REF, GraphUtils.STAT_VAR_OB, base_node);
    setPropVal(
        Property.observationAbout.name(),
        ValueType.RESOLVED_REF,
        s_key.getObservationAbout(),
        base_node);
    setPropVal(
        Property.variableMeasured.name(),
        ValueType.RESOLVED_REF,
        s_key.getVariableMeasured(),
        base_node);

    // Set optional common PVs.
    String val;
    if (!(val = s_key.getMeasurementMethod()).isEmpty()) {
      setPropVal(Property.measurementMethod.name(), ValueType.RESOLVED_REF, val, base_node);
    }
    if (!(val = s_key.getObservationPeriod()).isEmpty()) {
      setPropVal(Property.observationPeriod.name(), ValueType.TEXT, val, base_node);
    }
    if (!(val = s_key.getScalingFactor()).isEmpty()) {
      setPropVal(Property.scalingFactor.name(), ValueType.NUMBER, val, base_node);
    }
    if (!(val = s_key.getUnit()).isEmpty()) {
      setPropVal(Property.unit.name(), ValueType.TEXT, val, base_node);
    }
    McfGraph.PropertyValues base = base_node.build();

    for (StatVarObs o : optimized_graph.getSvObsSeries().getSvObsListList()) {
      // Copy common PVs.
      McfGraph.PropertyValues.Builder node = base.toBuilder();
      // Set required PVs.
      setPropVal(Property.dcid.name(), ValueType.TEXT, o.getDcid(), node);
      setPropVal(Property.observationDate.name(), ValueType.TEXT, o.getDate(), node);
      if (o.hasNumber()) {
        setPropVal(Property.value.name(), ValueType.NUMBER, Double.toString(o.getNumber()), node);
      } else if (o.hasText()) {
        setPropVal(Property.value.name(), ValueType.TEXT, o.getText(), node);
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
   * Converts a single StatVarObservation graph node into an McfStatVarObsSeries proto.
   *
   * @param node The McfGraph.PropertyValues representing a StatVarObservation.
   * @return An McfStatVarObsSeries proto containing the data from the input node.
   */
  public static McfStatVarObsSeries convertMcfGraphToMcfStatVarObsSeries(
      McfGraph.PropertyValues node) {
    if (!isObservation(node)) {
      throw new IllegalArgumentException("Not a StatVarObservation");
    }
    for (String prop : REQ_SV_OBS_PROPS) {
      String val = getPropVal(node, prop);
      if (val.isEmpty()) {
        throw new IllegalArgumentException("Missing " + prop);
      }
    }

    McfStatVarObsSeries.Builder res = McfStatVarObsSeries.newBuilder();
    // Assemble key.
    McfStatVarObsSeries.Key.Builder key = McfStatVarObsSeries.Key.newBuilder();
    key.setObservationAbout(getPropVal(node, "observationAbout"));
    key.setVariableMeasured(getPropVal(node, "variableMeasured"));
    String val;
    if (!(val = getPropVal(node, "measurementMethod")).isEmpty()) {
      key.setMeasurementMethod(val);
    }
    if (!(val = getPropVal(node, "observationPeriod")).isEmpty()) {
      key.setObservationPeriod(val);
    }
    if (!(val = getPropVal(node, "scalingFactor")).isEmpty()) {
      key.setScalingFactor(val);
    }
    if (!(val = getPropVal(node, "unit")).isEmpty()) {
      key.setUnit(val);
    }
    res.setKey(key.build());
    // Assemble StatVarObs.
    McfStatVarObsSeries.StatVarObs.Builder svo = res.addSvObsListBuilder();
    svo.setDate(getPropVal(node, "observationDate"));
    String dcid = getPropVal(node, "dcid");
    svo.setDcid(dcid);
    Double value;
    if (!(value = nodeDoubleValue(node, "value")).isNaN()) {
      svo.setNumber(value);
    } else { // Non-number value.
      svo.setText(getPropVal(node, "value"));
    }
    McfGraph.PropertyValues.Builder pvs = svo.getPvsBuilder();
    for (Map.Entry<String, McfGraph.Values> entry : node.getPvsMap().entrySet()) {
      if (!isSvObsProp(entry.getKey())) {
        pvs.putPvs(entry.getKey(), entry.getValue());
      }
    }
    pvs.build();
    svo.build();
    return res.build();
  }

  /**
   * Converts an MCF string into a graph proto (only SVObs)
   *
   * @param input input mcf string
   * @return An McfGraph proto representing the input MCF string.
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
      setPropVal(lhs, ValueType.TEXT, rhs, base_node);
    }
    g.putNodes(node_id, base_node.build());
    return g.build();
  }

  /**
   * Reads an MCF file and converts its content into a list of McfGraph protos.
   *
   * @param fileName The path to the MCF file.
   * @return A list of McfGraph protos, where each proto represents a node or block from the MCF
   *     file.
   */
  public static List<McfGraph> readMcfFile(String fileName) throws FileNotFoundException {

    List<McfGraph> graphList = new ArrayList<>();
    File file = new File(fileName);
    Scanner scanner = new Scanner(file);
    scanner.useDelimiter("\n\n");
    while (scanner.hasNext()) {
      String token = scanner.next();
      McfGraph g = convertToGraph(token);
      graphList.add(g);
    }
    scanner.close();
    return graphList;
  }
}
