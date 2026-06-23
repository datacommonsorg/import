package org.datacommons.ingestion.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.transforms.DoFn;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.util.McfUtil;

/** Transformer for data to convert complex values into nodes. */
public class GraphTransformer extends DoFn<McfGraph, McfGraph> {

  private static final Pattern QUANTITY_PATTERN =
      Pattern.compile("\\[(\\-?[\\d\\.]+) ([a-zA-Z]+)\\]");
  private static final Pattern LAT_LONG_PATTERN =
      Pattern.compile("\\[LatLong ([\\d\\.\\-]+) ([\\d\\.\\-]+)\\]");

  private static final Set<String> NON_CONSTRAINT_PROPS =
      Set.of(
          "dcid",
          "typeOf",
          "name",
          "description",
          "provenance",
          "memberOf",
          "populationType",
          "measuredProperty",
          "statType",
          "measurementQualifier",
          "measurementDenominator",
          "measurementMethod",
          "constraintProperties",
          "observationProperties",
          "entityMapping");

  @ProcessElement
  public void processElement(@Element McfGraph inputGraph, OutputReceiver<McfGraph> receiver) {
    McfGraph.Builder graphBuilder = McfGraph.newBuilder();

    // Copy existing nodes, modifying if necessary, and add generated nodes to the same builder.
    for (Map.Entry<String, PropertyValues> entry : inputGraph.getNodesMap().entrySet()) {
      String nodeId = entry.getKey();
      PropertyValues pv = entry.getValue();

      processNode(nodeId, pv, inputGraph, graphBuilder);
    }

    if (graphBuilder.getNodesCount() > 0) {
      receiver.output(graphBuilder.build());
    }
  }

  private void processNode(
      String nodeId, PropertyValues pv, McfGraph inputGraph, McfGraph.Builder graphBuilder) {
    PropertyValues.Builder newPvBuilder = pv.toBuilder();

    // Extract provenance if available to propagate to new nodes
    String provenance = null;
    if (pv.containsPvs("provenance")) {
      List<TypedValue> provList = pv.getPvsMap().get("provenance").getTypedValuesList();
      if (!provList.isEmpty()) {
        provenance = provList.get(0).getValue();
      }
    }

    // Iterate over all properties
    for (String propertyName : pv.getPvsMap().keySet()) {
      handleProperty(newPvBuilder, propertyName, provenance, inputGraph, graphBuilder);
    }

    // Check if this is a StatisticalVariable node
    if (newPvBuilder.containsPvs("typeOf")) {
      for (TypedValue tv : newPvBuilder.getPvsMap().get("typeOf").getTypedValuesList()) {
        if ("StatisticalVariable".equals(McfUtil.stripNamespace(tv.getValue()))) {
          handleStatisticalVariable(newPvBuilder);
          break;
        }
      }
    }

    graphBuilder.putNodes(nodeId, newPvBuilder.build());
  }

  private void handleStatisticalVariable(PropertyValues.Builder pvBuilder) {
    List<String> constraintProps = new ArrayList<>();
    for (String prop : pvBuilder.getPvsMap().keySet()) {
      if (!NON_CONSTRAINT_PROPS.contains(prop)) {
        constraintProps.add(prop);
      }
    }

    if (!constraintProps.isEmpty()) {
      Collections.sort(constraintProps);
      Values.Builder valuesBuilder = Values.newBuilder();
      for (String propDcid : constraintProps) {
        valuesBuilder.addTypedValues(
            TypedValue.newBuilder().setValue(propDcid).setType(ValueType.RESOLVED_REF).build());
      }
      pvBuilder.putPvs("constraintProperties", valuesBuilder.build());
    }
  }

  private void handleProperty(
      PropertyValues.Builder pvBuilder,
      String propertyName,
      String provenance,
      McfGraph inputGraph,
      McfGraph.Builder graphBuilder) {
    if (pvBuilder.containsPvs(propertyName)) {
      Values values = pvBuilder.getPvsMap().get(propertyName);
      Values.Builder newValues = Values.newBuilder();
      boolean modified = false;

      for (TypedValue tv : values.getTypedValuesList()) {
        if (tv.getType() == ValueType.COMPLEX_VALUE
            || (tv.getType() == ValueType.TEXT && tv.getValue().contains("["))) {
          Matcher quantityMatcher = QUANTITY_PATTERN.matcher(tv.getValue());
          Matcher latLongMatcher = LAT_LONG_PATTERN.matcher(tv.getValue());

          if (quantityMatcher.find()) {
            String val = quantityMatcher.group(1);
            String unit = quantityMatcher.group(2);

            String quantityNodeId = unit + val; // e.g., Kilometer31.61
            String quantityDcid = "dcid:" + quantityNodeId;

            if (!inputGraph.containsNodes(quantityNodeId)
                && !graphBuilder.containsNodes(quantityNodeId)) {
              // Create the Quantity node
              PropertyValues.Builder quantityPv = PropertyValues.newBuilder();
              quantityPv.putPvs("typeOf", createValues("Quantity", ValueType.RESOLVED_REF));
              quantityPv.putPvs("value", createValues(val, ValueType.TEXT));
              quantityPv.putPvs("unitOfMeasure", createValues(unit, ValueType.RESOLVED_REF));
              quantityPv.putPvs("dcid", createValues(quantityNodeId, ValueType.TEXT));
              quantityPv.putPvs("name", createValues(unit + " " + val, ValueType.TEXT));

              if (provenance != null) {
                quantityPv.putPvs("provenance", createValues(provenance, ValueType.RESOLVED_REF));
              }

              graphBuilder.putNodes(quantityNodeId, quantityPv.build());
            }

            // Update reference
            newValues.addTypedValues(
                TypedValue.newBuilder()
                    .setValue(quantityNodeId)
                    .setType(ValueType.RESOLVED_REF)
                    .build());
            modified = true;
          } else if (latLongMatcher.find()) {
            String lat = latLongMatcher.group(1);
            String lon = latLongMatcher.group(2);

            String newDcid = generateLatLongDcid(lat, lon);
            if (!inputGraph.containsNodes(newDcid) && !graphBuilder.containsNodes(newDcid)) {
              PropertyValues.Builder geoPv = PropertyValues.newBuilder();
              geoPv.putPvs("typeOf", createValues("GeoCoordinates", ValueType.RESOLVED_REF));
              geoPv.putPvs("latitude", createValues(lat, ValueType.TEXT));
              geoPv.putPvs("longitude", createValues(lon, ValueType.TEXT));
              geoPv.putPvs("name", createValues(lat + "," + lon, ValueType.TEXT));
              geoPv.putPvs("dcid", createValues(newDcid, ValueType.RESOLVED_REF));
              if (provenance != null) {
                geoPv.putPvs("provenance", createValues(provenance, ValueType.RESOLVED_REF));
              }
              graphBuilder.putNodes(newDcid, geoPv.build());
            }

            newValues.addTypedValues(
                TypedValue.newBuilder().setValue(newDcid).setType(ValueType.RESOLVED_REF).build());
            modified = true;
          } else {
            newValues.addTypedValues(tv);
          }
        } else {
          newValues.addTypedValues(tv);
        }
      }

      if (modified) {
        pvBuilder.putPvs(propertyName, newValues.build());
      }
    }
  }

  private Values createValues(String value, ValueType type) {
    return Values.newBuilder()
        .addTypedValues(TypedValue.newBuilder().setValue(value).setType(type).build())
        .build();
  }

  private String generateLatLongDcid(String latStr, String lonStr) {
    try {
      double lat = Double.parseDouble(latStr);
      double lon = Double.parseDouble(lonStr);
      long latE5 = Math.round(lat * 100000);
      long lonE5 = Math.round(lon * 100000);
      return "latLong/" + latE5 + "_" + lonE5;
    } catch (NumberFormatException e) {
      // Fallback if parsing fails, though regex matched numbers
      return "latLong/" + latStr + "_" + lonStr;
    }
  }
}
