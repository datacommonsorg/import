package org.datacommons.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;

/**
 * Utility class to generate readable names for StatisticalVariable nodes when the name property is
 * missing.
 *
 * <p>The generated name is constructed from the various schema properties of the variable:
 *
 * <p><strong>Format:</strong> {@code [Prefix] [MeasureAndPop] [Qualifiers]: [Constraints]
 * [Denominator]}
 *
 * <ul>
 *   <li><b>Prefix:</b> Derived from {@code statType} (e.g., "Median") and time-based {@code
 *       measurementQualifier}s (e.g., "Annual").
 *   <li><b>MeasureAndPop:</b> Combines {@code measuredProperty} and {@code populationType}. For
 *       example, "Count Of Person". If they overlap, redundancy is avoided.
 *   <li><b>Qualifiers:</b> Non-time {@code measurementQualifier}s in parentheses, e.g., "(Real)".
 *   <li><b>Constraints:</b> A comma-separated list of values for filtering properties like {@code
 *       gender}, {@code race}, etc., prepended with a colon.
 *   <li><b>Denominator:</b> Derived from {@code measurementDenominator}, typically resulting in
 *       "(Per capita)" or "(As fraction of ...)".
 * </ul>
 *
 * <p><strong>Example:</strong> "Annual Count Of Person (Real): Female, White (Per capita)"
 */
public class StatVarNameGenerator {

  /** Checks whether the given node types indicate a StatisticalVariable or slice. */
  public static boolean isStatVar(List<String> types) {
    if (types == null || types.isEmpty()) {
      return false;
    }
    for (String t : types) {
      String stripped = McfUtil.stripNamespace(t);
      if (Vocabulary.isStatVar(stripped)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Formats a DCID or token string into readable Title Case words without needing schema lookups.
   */
  public static String formatToken(String token) {
    if (token == null || token.isEmpty()) {
      return "";
    }
    String s = McfUtil.stripNamespace(token);
    s = s.replace('_', ' ');
    s = s.replaceAll("([a-z0-9])([A-Z])", "$1 $2");
    s = s.replaceAll("([A-Z])([A-Z][a-z])", "$1 $2");
    s = s.replaceAll("([a-zA-Z])([0-9])", "$1 $2");
    s = s.replaceAll("([0-9])([a-zA-Z])", "$1 $2");
    s = s.replaceAll("\\s+", " ").trim();

    List<String> words = new ArrayList<>();
    for (String w : Splitter.on(' ').omitEmptyStrings().split(s)) {
      if (!w.isEmpty()) {
        words.add(Character.toUpperCase(w.charAt(0)) + (w.length() > 1 ? w.substring(1) : ""));
      }
    }
    return Joiner.on(" ").join(words);
  }

  private static boolean isGenericPopType(String popType) {
    if (popType == null || popType.isEmpty()) {
      return true;
    }
    String stripped = McfUtil.stripNamespace(popType);
    return "Thing".equalsIgnoreCase(stripped);
  }

  /** Computes a readable display name for a StatisticalVariable based on its property values. */
  public static String generateName(String nodeId, PropertyValues pvs) {
    if (pvs == null || pvs.getPvsMap() == null) {
      return "";
    }
    String statType = GraphUtils.getPropVal(pvs, Vocabulary.STAT_TYPE);
    String measuredProperty = GraphUtils.getPropVal(pvs, Vocabulary.MEASURED_PROP);
    String populationType = GraphUtils.getPropVal(pvs, Vocabulary.POPULATION_TYPE);
    String measurementDenominator = GraphUtils.getPropVal(pvs, Vocabulary.MEASUREMENT_DENOMINATOR);

    // Build Prefix (statType + measurementQualifier periods)
    List<String> prefixList = new ArrayList<>();
    if (!statType.isEmpty()
        && !"measuredValue".equalsIgnoreCase(statType)
        && !"dcid:measuredValue".equalsIgnoreCase(statType)) {
      String st = McfUtil.stripNamespace(statType);
      // Strip redundant "Value" suffixes (e.g., "MedianValue" -> "Median")
      if (st.length() > 5 && (st.endsWith("Value") || st.endsWith("value"))) {
        st = st.substring(0, st.length() - 5);
      }
      String formattedSt = formatToken(st);
      if (!formattedSt.isEmpty()) {
        prefixList.add(formattedSt);
      }
    }

    List<String> mqualList = new ArrayList<>();
    List<String> mquals =
        GraphUtils.getPropertyValues(pvs.getPvsMap(), Vocabulary.MEASUREMENT_QUALIFIER);
    if (!mquals.isEmpty()) {
      List<String> effectiveMqualParts = new ArrayList<>();
      for (String q : mquals) {
        for (String part :
            Splitter.onPattern("[,\\s&]+").omitEmptyStrings().trimResults().split(q)) {
          if ("Daily".equalsIgnoreCase(part)
              || "Weekly".equalsIgnoreCase(part)
              || "Monthly".equalsIgnoreCase(part)
              || "Quarterly".equalsIgnoreCase(part)
              || "Annual".equalsIgnoreCase(part)) {
            prefixList.add(formatToken(part));
          } else {
            effectiveMqualParts.add(formatToken(part));
          }
        }
      }
      if (!effectiveMqualParts.isEmpty()) {
        mqualList.add("(" + Joiner.on(" & ").join(effectiveMqualParts) + ")");
      }
    }

    // Format Measure and PopType: Combine the measured property and population type.
    String formattedMeasure = formatToken(measuredProperty);
    String formattedPopType = formatToken(populationType);
    String measureAndPop = formattedMeasure;

    if (!formattedPopType.isEmpty()
        && !isGenericPopType(populationType)
        && !isGenericPopType(formattedPopType)) {
      if (measureAndPop.isEmpty()) {
        // Fallback to population type if there is no measured property.
        measureAndPop = formattedPopType;
      } else if (!measureAndPop.equalsIgnoreCase(formattedPopType)
          && !measureAndPop.toLowerCase().contains(formattedPopType.toLowerCase())
          && !formattedPopType.toLowerCase().contains(measureAndPop.toLowerCase())) {
        // If measure and popType are distinct, combine them (e.g., "Count Of Person").
        measureAndPop = measureAndPop + " Of " + formattedPopType;
      } else if (formattedPopType.toLowerCase().contains(measureAndPop.toLowerCase())
          && !measureAndPop.equalsIgnoreCase(formattedPopType)) {
        // If popType contains the measure (e.g., popType="Person With Age", measure="Age"),
        // use popType to avoid redundancy like "Age Of Person With Age".
        measureAndPop = formattedPopType;
      }
      // Implicitly, if measureAndPop contains formattedPopType (e.g., "Count Of Person"
      // contains "Person"), it skips appending to avoid "Count Of Person Of Person".
    }

    // Collect Constraint Properties & Values
    List<String> constraintVals = new ArrayList<>();
    List<String> sortedProps = new ArrayList<>(pvs.getPvsMap().keySet());
    Collections.sort(sortedProps);
    for (String rawProp : sortedProps) {
      String prop = McfUtil.stripNamespace(rawProp);
      // Skip non-constraint/core properties like statType, measuredProperty, etc.
      if (Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(prop)
          || "constraintProperties".equals(prop)) {
        continue;
      }
      List<String> vals = GraphUtils.getPropertyValues(pvs.getPvsMap(), rawProp);
      for (String val : vals) {
        // Special case boolean properties to print "PropName" or "PropName (False)"
        if ("true".equalsIgnoreCase(val)) {
          constraintVals.add(formatToken(prop));
        } else if ("false".equalsIgnoreCase(val)) {
          constraintVals.add(formatToken(prop) + " (False)");
        } else {
          String formattedVal = formatToken(val);
          if (!formattedVal.isEmpty()) {
            constraintVals.add(formattedVal);
          }
        }
      }
    }

    // Measurement Denominator
    List<String> mdenomList = new ArrayList<>();
    if (!measurementDenominator.isEmpty()) {
      String strippedDenom = McfUtil.stripNamespace(measurementDenominator);
      // Simplify human population denominators to "Per capita"
      if ("Count_Person".equalsIgnoreCase(strippedDenom)
          || "Person".equalsIgnoreCase(strippedDenom)) {
        mdenomList.add("(Per capita)");
      } else {
        List<String> denomParts = new ArrayList<>();
        for (String p : Splitter.on('_').omitEmptyStrings().trimResults().split(strippedDenom)) {
          denomParts.add(formatToken(p));
        }
        mdenomList.add("(As fraction of " + Joiner.on(' ').join(denomParts) + ")");
      }
    }

    // Assemble Display Name
    List<String> parts = new ArrayList<>();
    if (!prefixList.isEmpty()) {
      parts.add(Joiner.on(" ").join(prefixList));
    }
    if (!measureAndPop.isEmpty()) {
      parts.add(measureAndPop);
    }
    if (!mqualList.isEmpty()) {
      parts.add(Joiner.on(" ").join(mqualList));
    }
    String baseName = Joiner.on(" ").join(parts);
    if (!constraintVals.isEmpty()) {
      if (!baseName.isEmpty()) {
        baseName += ": " + Joiner.on(", ").join(constraintVals);
      } else {
        baseName = Joiner.on(", ").join(constraintVals);
      }
    }
    if (!mdenomList.isEmpty()) {
      if (!baseName.isEmpty()) {
        baseName += " " + Joiner.on(" ").join(mdenomList);
      } else {
        baseName = Joiner.on(" ").join(mdenomList);
      }
    }

    return baseName.trim();
  }
}
