package org.datacommons.util;

import com.google.common.base.Charsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Checks common types of nodes on naming and schema requirements.
// TODO: Pass in associated SV nodes to validate SVObs better.
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
  private Set<String> columns; // Relevant only when graph.type() == TEMPLATE_MCF
  boolean foundFailure = false;

  // Argument |graph| may be Instance or Template MCF.
  public McfChecker(Mcf.McfGraph graph, LogWrapper logCtx) {
    this.graph = graph;
    this.logCtx = logCtx;
  }

  // Used with Template MCF when there are columns from CSV header.
  public McfChecker(Mcf.McfGraph graph, Set<String> columns, LogWrapper logCtx) {
    this.graph = graph;
    this.columns = columns;
    this.logCtx = logCtx;
  }

  // Used to check a single node.
  public McfChecker(
      Mcf.McfType mcfType, String nodeId, Mcf.McfGraph.PropertyValues node, LogWrapper logCtx) {
    Mcf.McfGraph.Builder nodeGraph = Mcf.McfGraph.newBuilder();
    nodeGraph.setType(mcfType);
    nodeGraph.putNodes(nodeId, node);
    this.graph = nodeGraph.build();
    this.logCtx = logCtx;
  }

  // Returns true if there was an sanity error found.
  public boolean check() {
    foundFailure = false;
    for (String nodeId : graph.getNodesMap().keySet()) {
      Mcf.McfGraph.PropertyValues node = graph.toBuilder().getNodesOrThrow(nodeId);
      checkNode(nodeId, node);
      if (graph.getType() == Mcf.McfType.TEMPLATE_MCF) {
        checkTemplateNode(nodeId, node);
      }
    }
    return !foundFailure;
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

  private void checkTemplateNode(String nodeId, Mcf.McfGraph.PropertyValues node) {
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        if (tv.getType() == Mcf.ValueType.TABLE_ENTITY) {
          if (!graph.getNodesMap().containsKey(tv.getValue())) {
            addLog(
                "Sanity_TmcfMissingEntityDef",
                "Missing entity '"
                    + tv.getValue()
                    + "' referred to in property "
                    + pv.getKey()
                    + " of TMCF entity "
                    + nodeId,
                node);
            continue;
          }
        } else if (tv.getType() == Mcf.ValueType.TABLE_COLUMN) {
          // NOTE: If the MCF had parsed, the schema terms should be valid, thus
          // the ValueOrDie().
          McfParser.SchemaTerm term = McfParser.parseSchemaTerm(tv.getValue(), null);
          if (term.type != McfParser.SchemaTerm.Type.COLUMN) {
            addLog(
                "Sanity_TmcfUnexpectedNonColumn",
                "Unable to parse TMCF column '"
                    + tv.getValue()
                    + "' in property "
                    + pv.getKey()
                    + " of TMCF entity "
                    + nodeId,
                node);
            continue;
          }
          if (columns != null && !columns.contains(term.value)) {
            addLog(
                "Sanity_TmcfMissingColumn",
                "Column '"
                    + term.value
                    + "' referred in TMCF node "
                    + nodeId
                    + " is missing from the CSV header",
                node);
            continue;
          }
        }
      }
    }
  }

  private void checkStatVar(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String popType =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.POPULATION_TYPE);
    checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, "", true);

    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.MEASURED_PROP);
    checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, "", false);

    String statType =
        checkRequiredSingleValueProp(nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.STAT_TYPE);
    if (!Vocabulary.isStatValueProperty(statType)
        && !statType.equals(Vocabulary.MEASUREMENT_RESULT)) {
      addLog(
          "Sanity_UnknownStatType",
          "Found an unknown statType value '" + statType + "' in node " + nodeId,
          node);
    }
  }

  private void checkSVObs(String nodeId, Mcf.McfGraph.PropertyValues node) {
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.VARIABLE_MEASURED);
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_ABOUT);
    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !McfUtil.isValidISO8601Date(obsDate)) {
      addLog(
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant value '"
              + obsDate
              + "' for property "
              + Vocabulary.OBSERVATION_DATE
              + " in node "
              + nodeId,
          node);
    }
    checkRequiredSingleValueProp(
        Debug.Log.Level.LEVEL_WARNING,
        nodeId,
        node,
        Vocabulary.STAT_VAR_OBSERVATION_TYPE,
        Vocabulary.GENERIC_VALUE);
  }

  private void checkLegacyPopulation(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String popType =
        checkRequiredSingleValueProp(
            nodeId, node, "StatisticalPopulation", Vocabulary.POPULATION_TYPE);
    checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, "", true);

    checkRequiredSingleValueProp(nodeId, node, "StatisticalPopulation", Vocabulary.LOCATION);
  }

  private void checkLegacyObs(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASURED_PROP);
    checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, "", false);

    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVED_NODE);

    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !McfUtil.isValidISO8601Date(obsDate)) {
      addLog(
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant value '"
              + obsDate
              + "' for property "
              + Vocabulary.OBSERVATION_DATE
              + " in node "
              + nodeId,
          node);
    }

    boolean value_present = false;
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      if (Vocabulary.isStatValueProperty(prop)) {
        String val =
            checkRequiredSingleValueProp(
                nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, prop);
        if (graph.getType() != Mcf.McfType.TEMPLATE_MCF && !val.isEmpty()) {
          if (!McfUtil.isNumber(val)) {
            addLog(
                "Sanity_NonDoubleObsValue",
                "Expected value of type double for Observation property "
                    + prop
                    + " in node "
                    + nodeId,
                node);
          }
        }
        value_present = true;
      }
    }
    if (!value_present) {
      List<String> vals = McfUtil.getPropVals(node, Vocabulary.MEASUREMENT_RESULT);
      if (vals.isEmpty()) {
        addLog(
            Debug.Log.Level.LEVEL_WARNING,
            "Sanity_ObsMissingValueProp",
            "Missing any value " + "property in node " + nodeId,
            node);
      } else {
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASUREMENT_RESULT);
      }
    }
  }

  private void checkCommon(String nodeId, Mcf.McfGraph.PropertyValues node) {
    checkRequiredValueProp(nodeId, node, "Thing", Vocabulary.TYPE_OF);
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      if (prop.isEmpty()) {
        addLog("Sanity_EmptyProperty", "Found an empty property in node " + nodeId, node);
        continue;
      }

      if (!Character.isLowerCase(prop.charAt(0))) {
        addLog(
            "Sanity_NotInitLowerPropName",
            "Property name '" + prop + "' does not start with a " + "lower-case in node " + nodeId,
            node);
        continue;
      }

      Mcf.McfGraph.Values vals = pv.getValue();
      if (prop.equals(Vocabulary.DCID)) {
        if (vals.getTypedValuesCount() != 1) {
          addLog(
              "Sanity_MultipleDcidValues",
              "Property dcid must have exactly one value, but found "
                  + vals.getTypedValuesCount()
                  + " in node "
                  + nodeId,
              node);
          continue;
        }
        String dcid = vals.getTypedValues(0).getValue();
        if (vals.getTypedValues(0).getType() == Mcf.ValueType.TABLE_ENTITY) {
          addLog(
              "Sanity_DcidTableEntity",
              "Value of property dcid must not be an entity reference ("
                  + dcid
                  + ") in node "
                  + nodeId,
              node);
          continue;
        }
        if (dcid.length() > MAX_DCID_LENGTH) {
          addLog(
              "Sanity_VeryLongDcid",
              "Found a very long dcid value (> " + MAX_DCID_LENGTH + ") for node " + nodeId,
              node);
          continue;
        }
        // TODO: Expand this to include other special characters.
        if (!dcid.startsWith("bio/") && dcid.contains(" ")) {
          addLog(
              "Sanity_SpaceInDcid",
              "Found a whitespace in dcid value (" + dcid + ") in node " + nodeId,
              node);
          continue;
        }
      }

      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        if (tv.getType() != Mcf.ValueType.TEXT
            && !Charsets.US_ASCII.newEncoder().canEncode(tv.getValue())) {
          // Non-text values must be ascii.
          addLog(
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
              node);
        }
        if (Vocabulary.isReferenceProperty(prop)
            && (tv.getType() == Mcf.ValueType.TEXT || tv.getType() == Mcf.ValueType.NUMBER)) {
          addLog(
              "Sanity_RefPropHasNonRefValue",
              "Value '"
                  + tv.getValue()
                  + "' for reference property "
                  + prop
                  + " in "
                  + "node "
                  + nodeId
                  + " is not a reference.",
              node);
        }
      }
    }
  }

  private void checkClassOrProp(String typeOf, String nodeId, Mcf.McfGraph.PropertyValues node) {
    List<String> unexpectedProps =
        typeOf.equals(Vocabulary.CLASS_TYPE) ? PROPS_ONLY_IN_PROP : PROPS_ONLY_IN_CLASS;
    for (String prop : unexpectedProps) {
      if (!McfUtil.getPropVal(node, prop).isEmpty()) {
        addLog(
            "Sanity_UnexpectedPropIn" + typeOf,
            typeOf + " node " + nodeId + " must not include property " + prop,
            node);
      }
    }
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        String val = tv.getValue();
        if (val.isEmpty()) {
          addLog(
              "Sanity_EmptySchemaValue",
              "Found empty value for property " + prop + " in node " + nodeId,
              node);
          continue;
        }
        if (!Charsets.US_ASCII.newEncoder().canEncode(val)) {
          addLog(
              "Sanity_NonAsciiValueInSchema",
              "Value ("
                  + val
                  + ") in property "
                  + prop
                  + " of node "
                  + nodeId
                  + " contains non-ascii characters",
              node);
          continue;
        }
        if (typeOf.equals(Vocabulary.CLASS_TYPE) && CLASS_REFS_IN_CLASS.contains(prop)
            || (typeOf.equals(Vocabulary.PROPERTY_TYPE) && CLASS_REFS_IN_PROP.contains(prop))) {
          checkInitCasing(nodeId, node, prop, val, typeOf, true);
        }
        if (typeOf.equals(Vocabulary.PROPERTY_TYPE) && PROP_REFS_IN_PROP.contains(prop)) {
          checkInitCasing(nodeId, node, prop, val, typeOf, false);
        }
      }
    }
    if (typeOf.equals(Vocabulary.CLASS_TYPE)
        && !McfUtil.getPropVal(node, Vocabulary.DCID).equals(Vocabulary.THING_TYPE)) {
      checkRequiredSingleValueProp(nodeId, node, Vocabulary.CLASS_TYPE, Vocabulary.SUB_CLASS_OF);
    }
  }

  private String checkRequiredSingleValueProp(
      String nodeId, Mcf.McfGraph.PropertyValues node, String typeOf, String prop) {
    return checkRequiredSingleValueProp(Debug.Log.Level.LEVEL_ERROR, nodeId, node, typeOf, prop);
  }

  private String checkRequiredSingleValueProp(
      Debug.Log.Level level,
      String nodeId,
      Mcf.McfGraph.PropertyValues node,
      String typeOf,
      String prop) {
    List<String> vals = McfUtil.getPropVals(node, prop);
    if (vals.isEmpty()) {
      addLog(
          level,
          "Sanity_MissingOrEmpty_" + prop,
          "Missing or empty value for property "
              + prop
              + " in node "
              + nodeId
              + " of type "
              + typeOf,
          node);
      return "";
    }
    if (vals.size() != 1) {
      addLog(
          level,
          "Sanity_MultipleVals_" + prop,
          "Found multiple values for " + prop + " in node " + nodeId,
          node);
      return "";
    }
    return vals.get(0);
  }

  private void checkRequiredValueProp(
      String nodeId, Mcf.McfGraph.PropertyValues node, String typeOf, String prop) {
    List<String> vals = McfUtil.getPropVals(node, prop);
    if (vals.isEmpty()) {
      addLog(
          "Sanity_MissingOrEmpty_" + prop,
          "Missing or empty value for property "
              + prop
              + " in node "
              + nodeId
              + " of type "
              + typeOf,
          node);
    }
  }

  private void checkInitCasing(
      String nodeId,
      Mcf.McfGraph.PropertyValues node,
      String prop,
      String value,
      String typeOf,
      boolean expectInitUpper) {
    if (value.isEmpty()) return;
    String optType = !typeOf.isEmpty() ? "In" + typeOf : "";
    if (expectInitUpper && !Character.isUpperCase(value.charAt(0))) {
      addLog(
          "Sanity_NotInitUpper_" + prop + optType,
          "Found a class ref '"
              + value
              + "' in property "
              + prop
              + " in node "
              + nodeId
              + " which does not start with an upper case",
          node);
    } else if (!expectInitUpper && !Character.isLowerCase(value.charAt(0))) {
      addLog(
          "Sanity_NotInitLower_" + prop + optType,
          "Found a property ref '"
              + value
              + "' in property "
              + prop
              + " in node "
              + nodeId
              + " which does not start with a lower case",
          node);
    }
  }

  private void addLog(String counter, String message, Mcf.McfGraph.PropertyValues node) {
    addLog(Debug.Log.Level.LEVEL_ERROR, counter, message, node);
  }

  private void addLog(
      Debug.Log.Level level, String counter, String message, Mcf.McfGraph.PropertyValues node) {
    foundFailure = true;
    logCtx.addEntry(level, counter, message, node.getLocationsList());
  }
}
