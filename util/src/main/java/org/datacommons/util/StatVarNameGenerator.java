package org.datacommons.util;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
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

  private static final Pattern CAMEL_CASE_1 = Pattern.compile("([a-z0-9])([A-Z])");
  private static final Pattern CAMEL_CASE_2 = Pattern.compile("([A-Z])([A-Z][a-z])");
  private static final Pattern LETTER_NUMBER = Pattern.compile("([a-zA-Z])([0-9])");
  private static final Pattern NUMBER_LETTER = Pattern.compile("([0-9])([a-zA-Z])");
  private static final Pattern MULTI_SPACE = Pattern.compile("\\s+");

  private static final Splitter SPACE_SPLITTER = Splitter.on(' ').omitEmptyStrings();
  private static final Splitter UNDERSCORE_SPLITTER =
      Splitter.on('_').omitEmptyStrings().trimResults();
  private static final Splitter MQUAL_SPLITTER =
      Splitter.onPattern("[,\\s&]+").omitEmptyStrings().trimResults();

  private static final Joiner SPACE_JOINER = Joiner.on(" ");
  private static final Joiner COMMA_JOINER = Joiner.on(", ");
  private static final Joiner AND_JOINER = Joiner.on(" & ");

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
    s = CAMEL_CASE_1.matcher(s).replaceAll("$1 $2");
    s = CAMEL_CASE_2.matcher(s).replaceAll("$1 $2");
    s = LETTER_NUMBER.matcher(s).replaceAll("$1 $2");
    s = NUMBER_LETTER.matcher(s).replaceAll("$1 $2");
    s = MULTI_SPACE.matcher(s).replaceAll(" ").trim();

    List<String> words = new ArrayList<>();
    for (String w : SPACE_SPLITTER.split(s)) {
      words.add(Character.toUpperCase(w.charAt(0)) + (w.length() > 1 ? w.substring(1) : ""));
    }
    return SPACE_JOINER.join(words);
  }

  private static boolean isGenericPopType(String popType) {
    return popType == null || popType.isEmpty() || "Thing".equalsIgnoreCase(popType);
  }

  /** Computes a readable display name for a StatisticalVariable based on its property values. */
  public static String generateName(PropertyValues pvs) {
    if (pvs == null || pvs.getPvsMap() == null) {
      return "";
    }
    String statType = GraphUtils.getPropVal(pvs, Vocabulary.STAT_TYPE);
    String measuredProperty = GraphUtils.getPropVal(pvs, Vocabulary.MEASURED_PROP);
    String populationType = GraphUtils.getPropVal(pvs, Vocabulary.POPULATION_TYPE);
    String measurementDenominator = GraphUtils.getPropVal(pvs, Vocabulary.MEASUREMENT_DENOMINATOR);

    List<String> prefixList = buildPrefixList(statType);
    List<String> mqualList = buildMeasurementQualifiers(pvs, prefixList);
    String measureAndPop = buildMeasureAndPop(measuredProperty, populationType);
    List<String> constraintVals = buildConstraintVals(pvs);
    List<String> mdenomList = buildMeasurementDenominator(measurementDenominator);

    return assembleDisplayName(prefixList, mqualList, measureAndPop, constraintVals, mdenomList);
  }

  private static List<String> buildPrefixList(String statType) {
    List<String> prefixList = new ArrayList<>();
    if (!statType.isEmpty()
        && !"measuredValue".equalsIgnoreCase(statType)
        && !"dcid:measuredValue".equalsIgnoreCase(statType)) {
      String st = statType;
      // Strip redundant "Value" suffixes (e.g., "MedianValue" -> "Median")
      if (st.length() > 5 && (st.endsWith("Value") || st.endsWith("value"))) {
        st = st.substring(0, st.length() - 5);
      }
      String formattedSt = formatToken(st);
      if (!formattedSt.isEmpty()) {
        prefixList.add(formattedSt);
      }
    }
    return prefixList;
  }

  private static List<String> buildMeasurementQualifiers(
      PropertyValues pvs, List<String> prefixList) {
    List<String> mqualList = new ArrayList<>();
    List<String> mquals =
        GraphUtils.getPropertyValues(pvs.getPvsMap(), Vocabulary.MEASUREMENT_QUALIFIER);
    if (!mquals.isEmpty()) {
      List<String> effectiveMqualParts = new ArrayList<>();
      for (String q : mquals) {
        for (String part : MQUAL_SPLITTER.split(q)) {
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
        mqualList.add("(" + AND_JOINER.join(effectiveMqualParts) + ")");
      }
    }
    return mqualList;
  }

  private static String buildMeasureAndPop(String measuredProperty, String populationType) {
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
        // If popType contains the measure (e.g., popType="Person With Age",
        // measure="Age"),
        // use popType to avoid redundancy like "Age Of Person With Age".
        measureAndPop = formattedPopType;
      }
      // Implicitly, if measureAndPop contains formattedPopType (e.g., "Count Of
      // Person"
      // contains "Person"), it skips appending to avoid "Count Of Person Of Person".
    }
    return measureAndPop;
  }

  private static List<String> buildConstraintVals(PropertyValues pvs) {
    List<String> constraintVals = new ArrayList<>();
    List<String> sortedProps = new ArrayList<>(pvs.getPvsMap().keySet());
    Collections.sort(sortedProps);
    for (String rawProp : sortedProps) {
      String prop = McfUtil.stripNamespace(rawProp);
      // Skip non-constraint/core properties like statType, measuredProperty, etc.
      if (Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(prop)) {
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
    return constraintVals;
  }

  private static List<String> buildMeasurementDenominator(String measurementDenominator) {
    // Measurement Denominator
    List<String> mdenomList = new ArrayList<>();
    if (!measurementDenominator.isEmpty()) {
      // Simplify human population denominators to "Per capita"
      if ("Count_Person".equalsIgnoreCase(measurementDenominator)
          || "Person".equalsIgnoreCase(measurementDenominator)) {
        mdenomList.add("(Per capita)");
      } else {
        List<String> denomParts = new ArrayList<>();
        for (String p : UNDERSCORE_SPLITTER.split(measurementDenominator)) {
          denomParts.add(formatToken(p));
        }
        mdenomList.add("(As fraction of " + SPACE_JOINER.join(denomParts) + ")");
      }
    }
    return mdenomList;
  }

  private static String assembleDisplayName(
      List<String> prefixList,
      List<String> mqualList,
      String measureAndPop,
      List<String> constraintVals,
      List<String> mdenomList) {
    List<String> parts = new ArrayList<>();
    if (!prefixList.isEmpty()) {
      parts.add(SPACE_JOINER.join(prefixList));
    }
    if (!measureAndPop.isEmpty()) {
      parts.add(measureAndPop);
    }
    if (!mqualList.isEmpty()) {
      parts.add(SPACE_JOINER.join(mqualList));
    }
    String baseName = SPACE_JOINER.join(parts);
    if (!constraintVals.isEmpty()) {
      if (!baseName.isEmpty()) {
        baseName += ": " + COMMA_JOINER.join(constraintVals);
      } else {
        baseName = COMMA_JOINER.join(constraintVals);
      }
    }
    if (!mdenomList.isEmpty()) {
      if (!baseName.isEmpty()) {
        baseName += " " + SPACE_JOINER.join(mdenomList);
      } else {
        baseName = SPACE_JOINER.join(mdenomList);
      }
    }

    return baseName.trim();
  }
}
