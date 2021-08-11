package org.datacommons.util;

import com.google.common.base.Charsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Checks common types of nodes on naming and schema requirements.
// TODO: Pass in a separate SV nodes so we can validate SVObs better.
public class McfChecker {
  private final int MAX_DCID_LENGTH = 256;
  private final List<String> PROPS_ONLY_IN_PROP =
      List.of(Vocabulary.DOMAIN_INCLUDES, Vocabulary.RANGE_INCLUDES, Vocabulary.SUB_PROPERTY_OF);
  private final List<String> PROPS_ONLY_IN_CLASS = List.of(Vocabulary.SUB_CLASS_OF);
  private final Set<String> CLASS_REFS_IN_CLASS =
      Set.of(Vocabulary.NAME, Vocabulary.LABEL, Vocabulary.DCID, Vocabulary.SUB_CLASS_OF);
  private final Set<String> CLASS_REFS_IN_PROP =
      Set.of(Vocabulary.DOMAIN_INCLUDES, Vocabulary.RANGE_INCLUDES);
  private final Set<String> PROP_REFS_IN_PROP =
      Set.of(Vocabulary.NAME, Vocabulary.LABEL, Vocabulary.DCID, Vocabulary.SUB_PROPERTY_OF);
  private Mcf.McfGraph graph;
  private LogWrapper logCtx;

  public McfChecker(Mcf.McfGraph graph, LogWrapper logCtx) {
    this.graph = graph;
    this.logCtx = logCtx;
  }

  public void check() {
    for (String nodeId : graph.getNodesMap().keySet()) {
      Mcf.McfGraph.PropertyValues node = graph.toBuilder().getNodesOrThrow(nodeId);
      checkNode(nodeId, node);
      if (graph.getType() == Mcf.McfType.TEMPLATE_MCF) {
        checkTemplate(nodeId, node);
      }
    }
  }

  private void checkNode(String nodeId, Mcf.McfGraph.PropertyValues node) {
    checkCommon(nodeId, node);
    for (String typeOf : McfUtil.getPropVals(node, Vocabulary.TYPE_OF)) {
      if (Vocabulary.isStatVarObs(typeOf)) {
        checkSVObs(nodeId, node);
      } else if (typeOf.equals(Vocabulary.CLASS_TYPE) || typeOf.equals(Vocabulary.PROPERTY_TYPE)) {
        checkClassOrProp(typeOf, nodeId, node);
      } else if (Vocabulary.isStatVar(typeOf)) {
        checkStatVar(nodeId, node);
      } else if (Vocabulary.isLegacyObservation(typeOf)) {
        checkLegacyObs(nodeId, node);
      } else if (Vocabulary.isPopulation(typeOf)) {
        checkLegacyPopulation(nodeId, node);
      }
    }
  }

  private void checkStatVar(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String popType =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.POPULATION_TYPE);
    checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, true);

    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.MEASURED_PROP);
    checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, false);

    String statType =
        checkRequiredSingleValueProp(nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.STAT_TYPE);
    checkInitCasing(nodeId, node, Vocabulary.STAT_TYPE, statType, false);
  }

  private void checkSVObs(String nodeId, Mcf.McfGraph.PropertyValues node) {
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.VARIABLE_MEASURED);
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_ABOUT);
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.GENERIC_VALUE);
    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !McfUtil.isValidIS8601Date(obsDate)) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant value '"
              + obsDate
              + "' for property "
              + Vocabulary.OBSERVATION_DATE
              + " in node "
              + nodeId,
          node.getLocationsList());
    }
  }

  private void checkLegacyPopulation(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String popType =
        checkRequiredSingleValueProp(
            nodeId, node, "StatisticalPopulation", Vocabulary.POPULATION_TYPE);
    checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, true);

    checkRequiredSingleValueProp(nodeId, node, "StatisticalPopulation", Vocabulary.LOCATION);
  }

  private void checkLegacyObs(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASURED_PROP);
    checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, false);

    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVED_NODE);

    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !McfUtil.isValidIS8601Date(obsDate)) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant value '"
              + obsDate
              + "' for property "
              + Vocabulary.OBSERVATION_DATE
              + " in node "
              + nodeId,
          node.getLocationsList());
    }

    boolean value_present = false;
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      if (Vocabulary.isStatValueProperty(prop)) {
        String val =
            checkRequiredSingleValueProp(
                nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, prop);
        if (graph.getType() != Mcf.McfType.TEMPLATE_MCF && !val.isEmpty()) {
          if (!McfParser.isNumber(val)) {
            logCtx.addEntry(
                Debug.Log.Level.LEVEL_ERROR,
                "Sanity_NonDoubleObsValue",
                "Expected value of type double for Observation property "
                    + prop
                    + " in node "
                    + nodeId,
                node.getLocationsList());
          }
        }
        value_present = true;
      }
    }
    if (!value_present) {
      checkRequiredSingleValueProp(
          nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASUREMENT_RESULT);
    }
  }

  private void checkCommon(String nodeId, Mcf.McfGraph.PropertyValues node) {
    checkRequiredValueProp(nodeId, node, "Thing", Vocabulary.TYPE_OF);
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      if (prop.isEmpty()) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "Sanity_EmptyProperty",
            "Found an empty property in node " + nodeId,
            node.getLocationsList());
        continue;
      }

      if (!Character.isLowerCase(prop.charAt(0))) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "Sanity_InitUpperCasePropName",
            "Found property name "
                + prop
                + " that does not start with a "
                + " lower-case in node "
                + nodeId,
            node.getLocationsList());
        continue;
      }

      Mcf.McfGraph.Values vals = pv.getValue();
      if (prop.equals(Vocabulary.DCID)) {
        if (vals.getTypedValuesCount() != 1) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_MultipleDCIDValues",
              "Property dcid must have exactly one value, but found "
                  + vals.getTypedValuesCount()
                  + " in node "
                  + nodeId,
              node.getLocationsList());
          continue;
        }
        String dcid = vals.getTypedValues(0).getValue();
        if (vals.getTypedValues(0).getType() == Mcf.ValueType.TABLE_ENTITY) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_DcidTableEntity",
              "Value of property dcid must not be an entity reference ("
                  + dcid
                  + ") in node "
                  + nodeId,
              node.getLocationsList());
          continue;
        }
        if (dcid.length() > MAX_DCID_LENGTH) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_VeryLongDcid",
              "Found a very long dcid value (> " + MAX_DCID_LENGTH + ") for node " + nodeId,
              node.getLocationsList());
          continue;
        }
        // TODO: Expand this to include other special characters.
        if (!dcid.startsWith("bio/") && dcid.contains(" ")) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_SpaceInDcid",
              "Found a whitespace in dcid value (" + dcid + ") in node " + nodeId,
              node.getLocationsList());
          continue;
        }
      }

      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        if (tv.getType() != Mcf.ValueType.TEXT
            && !Charsets.US_ASCII.newEncoder().canEncode(tv.getValue())) {
          // Non-text values must be ascii.
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_NonAsciiValueInNonText",
              "Value '"
                  + tv.getValue()
                  + "' for property "
                  + prop
                  + " in "
                  + "node "
                  + nodeId
                  + " is a non-text value which contains non-ascii "
                  + "characters.",
              node.getLocationsList());
        }
        if (Vocabulary.isReferenceProperty(prop)
            && (tv.getType() == Mcf.ValueType.TEXT || tv.getType() == Mcf.ValueType.NUMBER)) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_RefPropHasNonRefValue",
              "Value '"
                  + tv.getValue()
                  + "' for reference property "
                  + prop
                  + " in "
                  + "node "
                  + nodeId
                  + " is not a reference.",
              node.getLocationsList());
        }
      }
    }
  }

  private void checkClassOrProp(String typeOf, String nodeId, Mcf.McfGraph.PropertyValues node) {
    List<String> unexpectedProps =
        typeOf.equals(Vocabulary.CLASS_TYPE) ? PROPS_ONLY_IN_CLASS : PROPS_ONLY_IN_PROP;
    for (String prop : unexpectedProps) {
      if (!McfUtil.getPropVal(node, prop).isEmpty()) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "Sanity_UnexpectedPropInSchema",
            typeOf + " node " + nodeId + " must not include property " + prop,
            node.getLocationsList());
      }
    }
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        String val = tv.getValue();
        if (val.isEmpty()) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_EmptySchemaValue",
              "Found empty value for property " + prop + " in node " + nodeId,
              node.getLocationsList());
          continue;
        }
        if (!Charsets.US_ASCII.newEncoder().canEncode(val)) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "Sanity_NonAsciiValueInSchema",
              "Value ("
                  + val
                  + ") in property "
                  + prop
                  + " of node "
                  + nodeId
                  + " contains non-ascii characters",
              node.getLocationsList());
          continue;
        }
        if (typeOf.equals(Vocabulary.CLASS_TYPE) && CLASS_REFS_IN_CLASS.contains(prop)
            || (typeOf.equals(Vocabulary.PROPERTY_TYPE) && CLASS_REFS_IN_PROP.contains(prop))) {
          checkInitCasing(nodeId, node, prop, val, true);
        }
        if (typeOf.equals(Vocabulary.PROPERTY_TYPE) && PROP_REFS_IN_PROP.contains(prop)) {
          checkInitCasing(nodeId, node, prop, val, false);
        }
      }
    }
    if (typeOf.equals(Vocabulary.CLASS_TYPE)
        && !McfUtil.getPropVal(node, Vocabulary.DCID).equals(Vocabulary.THING_TYPE)) {
      checkRequiredSingleValueProp(nodeId, node, Vocabulary.CLASS_TYPE, Vocabulary.SUB_CLASS_OF);
    }
  }

  private void checkTemplate(String nodeId, Mcf.McfGraph.PropertyValues node) {}

  private String checkRequiredSingleValueProp(
      String nodeId, Mcf.McfGraph.PropertyValues node, String typeOf, String prop) {
    List<String> vals = McfUtil.getPropVals(node, prop);
    if (vals.isEmpty()) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_MissingOrEmpty_" + prop,
          "Missing or empty value for property "
              + prop
              + " in node "
              + nodeId
              + " of type "
              + typeOf,
          node.getLocationsList());
      return "";
    }
    if (vals.size() != 1) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_MultipleVals_" + prop,
          "Found multiple values for " + prop + " in node " + nodeId,
          node.getLocationsList());
      return "";
    }
    return vals.get(0);
  }

  private void checkRequiredValueProp(
      String nodeId, Mcf.McfGraph.PropertyValues node, String typeOf, String prop) {
    List<String> vals = McfUtil.getPropVals(node, prop);
    if (vals.isEmpty()) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_MissingOrEmpty_" + prop,
          "Missing or empty value for property "
              + prop
              + " in node "
              + nodeId
              + " of type "
              + typeOf,
          node.getLocationsList());
    }
  }

  private void checkInitCasing(
      String nodeId,
      Mcf.McfGraph.PropertyValues node,
      String prop,
      String value,
      boolean expectInitUpper) {
    if (value.isEmpty()) return;
    if (expectInitUpper && !Character.isUpperCase(value.charAt(0))) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_NotInitUpper_" + prop,
          "Found a class ref '"
              + value
              + "' in property "
              + prop
              + " in node "
              + nodeId
              + " which does not start with an upper case",
          node.getLocationsList());
    } else if (!expectInitUpper && !Character.isLowerCase(value.charAt(0))) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "Sanity_NotInitLower_" + prop,
          "Found a property ref '"
              + value
              + "' in property "
              + prop
              + " in node "
              + nodeId
              + " which does not start with a lower case",
          node.getLocationsList());
    }
  }
}
