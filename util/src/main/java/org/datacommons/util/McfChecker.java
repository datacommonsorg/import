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

import com.google.common.base.Charsets;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Checks common types of nodes on naming and schema requirements.
//
// TODO: Add more column information
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

  // Includes: a-z A-Z 0-9 _ & + - % / . )( :
  private final Pattern VALID_DCID_PATTERN = Pattern.compile("^[\\w&/%\\)\\(+\\-\\.:]+$");
  // Everything in VALID_DCID_PATTERN, and then: ' * >< ][ | ; <space>
  // TODO: Drop this after Bio DCIDs are fixed
  private final Pattern VALID_BIO_DCID_PATTERN =
      Pattern.compile("^[\\w&/%\\)\\(+\\-\\.'\\*><\\]\\[|:; ]+$");

  private Mcf.McfGraph graph;
  private LogWrapper logCtx;
  private Set<String> columns; // Relevant only when graph.type() == TEMPLATE_MCF
  boolean foundFailure = false;
  private ExistenceChecker existenceChecker;

  // Argument |graph| may be Instance or Template MCF.
  public static boolean check(
      Mcf.McfGraph graph, ExistenceChecker existenceChecker, LogWrapper logCtx)
      throws IOException, InterruptedException {
    return new McfChecker(graph, null, existenceChecker, logCtx).check();
  }

  // Used to check a single node from TMcfCsvParser.
  public static boolean checkNode(
      Mcf.McfType mcfType, String nodeId, Mcf.McfGraph.PropertyValues node, LogWrapper logCtx)
      throws IOException, InterruptedException {
    Mcf.McfGraph.Builder nodeGraph = Mcf.McfGraph.newBuilder();
    nodeGraph.setType(mcfType);
    nodeGraph.putNodes(nodeId, node);
    return new McfChecker(nodeGraph.build(), null, null, logCtx).check();
  }

  // Used with Template MCF when there are columns from CSV header.
  public static boolean checkTemplate(
      Mcf.McfGraph graph, Set<String> columns, ExistenceChecker existenceChecker, LogWrapper logCtx)
      throws IOException, InterruptedException {
    return new McfChecker(graph, columns, existenceChecker, logCtx).check();
  }

  private McfChecker(
      Mcf.McfGraph graph,
      Set<String> columns,
      ExistenceChecker existenceChecker,
      LogWrapper logCtx) {
    this.graph = graph;
    this.columns = columns;
    this.logCtx = logCtx;
    this.existenceChecker = existenceChecker;
  }

  // Returns true if there was an sanity error found.
  private boolean check() throws IOException, InterruptedException {
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

  private void checkNode(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    if (node.hasTemplateNode()) {
      // The TMCF node ref is more user-friendly.
      nodeId = node.getTemplateNode();
    }
    var types = checkCommon(nodeId, node);
    for (String typeOf : types) {
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
                "No definition found for a referenced 'E:' value :: reference: '"
                    + tv.getValue()
                    + "', property: '"
                    + pv.getKey()
                    + "' node: '"
                    + nodeId
                    + "'",
                node);
            continue;
          }
        } else if (tv.getType() == Mcf.ValueType.TABLE_COLUMN) {
          // NOTE: If the MCF had parsed, the schema terms should be valid, thus
          // the ValueOrDie().
          long lineNum = -1;
          if (!node.getLocationsList().isEmpty()) {
            lineNum = node.getLocationsList().get(0).getLineNumber();
          }
          LogCb logCb =
              new LogCb(logCtx, Debug.Log.Level.LEVEL_ERROR, lineNum)
                  .setDetail(LogCb.VALUE_KEY, tv.getValue())
                  .setDetail(LogCb.NODE_KEY, nodeId);
          McfParser.SchemaTerm term = McfParser.parseSchemaTerm(tv.getValue(), logCb);
          if (term.type != McfParser.SchemaTerm.Type.COLUMN) {
            addLog(
                "Sanity_UnexpectedNonColumn",
                "Expected value to be a TMCF column that starts with 'C:' :: value: '"
                    + tv.getValue()
                    + "', property: '"
                    + pv.getKey()
                    + "', node: '"
                    + nodeId
                    + "'",
                node);
            continue;
          }
          if (columns != null && !columns.contains(term.value)) {
            addLog(
                "Sanity_TmcfMissingColumn",
                "Column referred to in TMCF is missing from CSV header :: column: '"
                    + term.value
                    + "', node: '"
                    + nodeId
                    + "'",
                node);
            continue;
          }
        }
      }
    }
  }

  private void checkStatVar(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    String popType =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.POPULATION_TYPE);
    if (!popType.isEmpty()) {
      checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, "", true);
    }

    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.MEASURED_PROP);
    if (!mProp.isEmpty()) {
      checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, "", false);
    }
    // TODO: Do this check for all constraint properties too.
    if (existenceChecker != null) {
      LogCb logCb =
          new LogCb(logCtx, Debug.Log.Level.LEVEL_WARNING, node)
              .setDetail(LogCb.SUB_KEY, mProp)
              .setDetail(LogCb.PRED_KEY, Vocabulary.DOMAIN_INCLUDES)
              .setDetail(LogCb.OBJ_KEY, popType)
              .setDetail(LogCb.NODE_KEY, nodeId)
              .setCounterSuffix(Vocabulary.DOMAIN_INCLUDES);
      existenceChecker.submitTripleCheck(mProp, Vocabulary.DOMAIN_INCLUDES, popType, logCb);
    }

    String statType =
        checkRequiredSingleValueProp(nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.STAT_TYPE);
    if (!statType.isEmpty()
        && !Vocabulary.isStatValueProperty(statType)
        && !statType.equals(Vocabulary.MEASUREMENT_RESULT)) {
      addLog(
          "Sanity_UnknownStatType",
          "Found an unknown statType value :: value: '" + statType + "', node: '" + nodeId + "'",
          node);
    }

    // Every SV must have DCID defined.
    checkRequiredSingleValueProp(nodeId, node, Vocabulary.STAT_VAR_TYPE, Vocabulary.DCID);
  }

  private void checkSVObs(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.VARIABLE_MEASURED);
    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_ABOUT);
    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.STAT_VAR_OBSERVATION_TYPE, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !StringUtil.isValidISO8601Date(obsDate)) {
      addLog(
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant date value :: value: '"
              + obsDate
              + "', property: '"
              + Vocabulary.OBSERVATION_DATE
              + "', node: '"
              + nodeId
              + "'",
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
    if (!popType.isEmpty()) {
      checkInitCasing(nodeId, node, Vocabulary.POPULATION_TYPE, popType, "", true);
    }

    checkRequiredSingleValueProp(nodeId, node, "StatisticalPopulation", Vocabulary.LOCATION);
  }

  private void checkLegacyObs(String nodeId, Mcf.McfGraph.PropertyValues node) {
    String mProp =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASURED_PROP);
    if (!mProp.isEmpty()) {
      checkInitCasing(nodeId, node, Vocabulary.MEASURED_PROP, mProp, "", false);
    }

    checkRequiredSingleValueProp(
        nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVED_NODE);

    String obsDate =
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.OBSERVATION_DATE);
    if (graph.getType() != Mcf.McfType.TEMPLATE_MCF
        && !obsDate.isEmpty()
        && !StringUtil.isValidISO8601Date(obsDate)) {
      addLog(
          "Sanity_InvalidObsDate",
          "Found a non-ISO8601 compliant date value :: value: '"
              + obsDate
              + "', property: '"
              + Vocabulary.OBSERVATION_DATE
              + "', node: '"
              + nodeId
              + "'",
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
          if (!StringUtil.isNumber(val)) {
            addLog(
                "Sanity_NonDoubleObsValue",
                "Found a non-double Observation value :: value: '"
                    + val
                    + "', property: '"
                    + prop
                    + "', node: '"
                    + nodeId
                    + "'",
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
            "Observation node missing value property :: node: '" + nodeId + "'",
            node);
      } else {
        checkRequiredSingleValueProp(
            nodeId, node, Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, Vocabulary.MEASUREMENT_RESULT);
      }
    }
  }

  // Returns true if we want to check existence for the given prop and types.
  private boolean shouldCheckExistence(String prop, Set<String> types) {
    if (types.contains(Vocabulary.LEGACY_POPULATION_TYPE_SUFFIX)
        || types.contains(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX)) {
      return false;
    }
    // For StatVarObs, we check variableMeasured, measurementMethod and unit only.
    if (types.contains(Vocabulary.STAT_VAR_OBSERVATION_TYPE)
        && !prop.equals(Vocabulary.VARIABLE_MEASURED)
        && !prop.equals(Vocabulary.MEASUREMENT_METHOD)
        && !prop.equals(Vocabulary.UNIT)) {
      return false;
    }
    return true;
  }

  private Set<String> checkCommon(String nodeId, Mcf.McfGraph.PropertyValues node)
      throws IOException, InterruptedException {
    Set<String> types =
        new HashSet<>(checkRequiredValueProp(nodeId, node, "Thing", Vocabulary.TYPE_OF));
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : node.getPvsMap().entrySet()) {
      String prop = pv.getKey();
      if (prop.isEmpty()) {
        addLog("Sanity_EmptyProperty", "Found an empty property :: node: '" + nodeId + "'", node);
        continue;
      }

      if (!Character.isLowerCase(prop.charAt(0))) {
        addLog(
            "Sanity_NotInitLowerPropName",
            "Found property name that does not start with a lower-case :: property: '"
                + prop
                + "', node: '"
                + nodeId
                + "'",
            node);
        continue;
      }

      Mcf.McfGraph.Values vals = pv.getValue();
      if (prop.equals(Vocabulary.DCID)) {
        if (vals.getTypedValuesCount() != 1) {
          addLog(
              "Sanity_MultipleDcidValues",
              "Found dcid with more than one value :: count: "
                  + vals.getTypedValuesCount()
                  + ", node: '"
                  + nodeId
                  + "'",
              node);
          continue;
        }
        var dcid = vals.getTypedValues(0);
        if (vals.getTypedValues(0).getType() == Mcf.ValueType.TABLE_ENTITY) {
          addLog(
              "Sanity_DcidTableEntity",
              "Value of dcid property must not be an 'E:' reference :: value: '"
                  + dcid.getValue()
                  + "', node: '"
                  + nodeId
                  + "'",
              node);
          continue;
        }
        if (dcid.getValue().length() > MAX_DCID_LENGTH) {
          addLog(
              "Sanity_VeryLongDcid",
              "Found a very long dcid value; must be less than "
                  + MAX_DCID_LENGTH
                  + " :: node: '"
                  + nodeId
                  + "'",
              node);
          continue;
        }
        // dcid value should typically be TEXT, but some MCFs mistakenly have refs too.
        if ((dcid.getType() == Mcf.ValueType.TEXT || dcid.getType() == Mcf.ValueType.RESOLVED_REF)
            && !checkDcid(dcid.getValue(), Vocabulary.DCID, nodeId, node)) {
          continue;
        }
      }

      if (existenceChecker != null) {
        LogCb logCb =
            new LogCb(logCtx, Debug.Log.Level.LEVEL_WARNING, node)
                .setDetail(LogCb.PREF_KEY, prop)
                .setDetail(LogCb.NODE_KEY, nodeId)
                .setCounterSuffix(Vocabulary.PROPERTY_TYPE);
        existenceChecker.submitNodeCheck(prop, logCb);
      }

      for (Mcf.McfGraph.TypedValue tv : pv.getValue().getTypedValuesList()) {
        if (tv.getType() != Mcf.ValueType.TEXT
            && !Charsets.US_ASCII.newEncoder().canEncode(tv.getValue())) {
          // Non-text values must be ascii.
          addLog(
              "Sanity_NonAsciiValueInNonText",
              "Found non-ascii characters in a value that is not text :: "
                  + "value: '"
                  + tv.getValue()
                  + "', type: '"
                  + tv.getType().name()
                  + "', property: '"
                  + prop
                  + "', node: '"
                  + nodeId
                  + "'",
              node);
        }
        if (Vocabulary.isReferenceProperty(prop)
            && (tv.getType() == Mcf.ValueType.TEXT || tv.getType() == Mcf.ValueType.NUMBER)) {
          addLog(
              "Sanity_RefPropHasNonRefValue",
              "Found text/numeric value in a reference property :: value: '"
                  + tv.getValue()
                  + "', property: '"
                  + prop
                  + "', node: '"
                  + nodeId
                  + "'",
              node);
        }
        if (tv.getType() == Mcf.ValueType.RESOLVED_REF) {
          if (!checkDcid(tv.getValue(), prop, nodeId, node)) {
            // Failed. checkDcid would have updated logCtx, pass through...
          } else if (shouldCheckExistence(prop, types) && existenceChecker != null) {
            LogCb logCb =
                new LogCb(logCtx, Debug.Log.Level.LEVEL_WARNING, node)
                    .setDetail(LogCb.VREF_KEY, tv.getValue())
                    .setDetail(LogCb.PROP_KEY, prop)
                    .setDetail(LogCb.NODE_KEY, nodeId)
                    .setCounterSuffix(prop);
            existenceChecker.submitNodeCheck(tv.getValue(), logCb);
          }
        }
      }
    }
    return types;
  }

  private String getInvalidChars(String ref, Pattern p) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < ref.length(); i++) {
      var c = ref.charAt(i);
      if (!p.matcher(Character.toString(c)).matches()) {
        result.append(c);
      }
    }
    return result.toString();
  }

  private boolean checkDcid(
      String ref, String prop, String nodeId, Mcf.McfGraph.PropertyValues node) {
    Pattern p;
    if (ref.startsWith("bio/")) {
      p = VALID_BIO_DCID_PATTERN;
    } else {
      p = VALID_DCID_PATTERN;
    }
    if (!p.matcher(ref).matches()) {
      addLog(
          "Sanity_InvalidChars_" + prop,
          "Found invalid chars in dcid value :: value: '"
              + ref
              + "', invalid-chars: '"
              + getInvalidChars(ref, p)
              + "', property: '"
              + prop
              + "', node: '"
              + nodeId
              + "'",
          node);
      return false;
    }
    return true;
  }

  private void checkClassOrProp(String typeOf, String nodeId, Mcf.McfGraph.PropertyValues node) {
    List<String> unexpectedProps =
        typeOf.equals(Vocabulary.CLASS_TYPE) ? PROPS_ONLY_IN_PROP : PROPS_ONLY_IN_CLASS;
    for (String prop : unexpectedProps) {
      if (!McfUtil.getPropVal(node, prop).isEmpty()) {
        addLog(
            "Sanity_UnexpectedPropIn" + typeOf,
            "Unexpected property in "
                + typeOf
                + " node :: property: '"
                + prop
                + "', "
                + "node: '"
                + nodeId
                + "'",
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
              "Found empty property value :: property: '" + prop + "', node '" + nodeId + "'",
              node);
          continue;
        }
        // TODO: perhaps make an exception for description
        if (!Charsets.US_ASCII.newEncoder().canEncode(val)) {
          addLog(
              "Sanity_NonAsciiValueInSchema",
              "Schema node has property values with non-ascii characters :: "
                  + "value: '"
                  + val
                  + "', property: '"
                  + prop
                  + "', node: '"
                  + nodeId
                  + "'",
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
    // Assert that the DCID matches name/label.
    var dcid = McfUtil.getPropVal(node, Vocabulary.DCID);
    var name = McfUtil.getPropVal(node, Vocabulary.NAME);
    if (name.isEmpty()) name = McfUtil.getPropVal(node, Vocabulary.LABEL);
    if (!dcid.isEmpty() && !name.isEmpty() && !dcid.equals(name)) {
      addLog(
          "Sanity_DcidNameMismatchInSchema",
          "Schema node with dcid/name mismatch :: name: '"
              + name
              + "', dcid: '"
              + dcid
              + "',"
              + " node: '"
              + nodeId
              + "'",
          node);
    }

    if (typeOf.equals(Vocabulary.CLASS_TYPE) && !dcid.equals(Vocabulary.THING_TYPE)) {
      checkRequiredValueProp(nodeId, node, Vocabulary.CLASS_TYPE, Vocabulary.SUB_CLASS_OF);
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
    List<Mcf.McfGraph.TypedValue> tvs = McfUtil.getPropTvs(node, prop);
    if (tvs == null || tvs.isEmpty()) {
      addLog(
          level,
          "Sanity_MissingOrEmpty_" + prop,
          "Found a missing or empty property value :: property: '"
              + prop
              + "', node: '"
              + nodeId
              + "', type: '"
              + typeOf
              + "'",
          node);
      return "";
    }
    if (tvs.size() != 1) {
      String optColumn = tvs.get(0).hasColumn() ? ", column: '" + tvs.get(0).getColumn() + "'" : "";
      addLog(
          level,
          "Sanity_MultipleVals_" + prop,
          "Found multiple values for single-value property :: property: '"
              + prop
              + "'"
              + optColumn
              + ", node: '"
              + nodeId
              + "'",
          node);
      return "";
    }
    return McfUtil.stripNamespace(tvs.get(0).getValue());
  }

  private List<String> checkRequiredValueProp(
      String nodeId, Mcf.McfGraph.PropertyValues node, String typeOf, String prop) {
    List<String> vals = McfUtil.getPropVals(node, prop);
    if (vals.isEmpty()) {
      addLog(
          "Sanity_MissingOrEmpty_" + prop,
          "Found a missing or empty property value :: property: '"
              + prop
              + "', node: '"
              + nodeId
              + "', type: '"
              + typeOf
              + "'",
          node);
    }
    return vals;
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
          "Found a class reference that does not start with an upper-case :: reference: '"
              + value
              + "', property: '"
              + prop
              + ", node: '"
              + nodeId
              + "'",
          node);
    } else if (!expectInitUpper && !Character.isLowerCase(value.charAt(0))) {
      addLog(
          "Sanity_NotInitLower_" + prop + optType,
          "Found a property reference that does not start with a lower-case :: reference: '"
              + value
              + "', property: '"
              + prop
              + ", node: '"
              + nodeId
              + "'",
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
