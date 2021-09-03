package org.datacommons.util;

import com.google.common.base.Charsets;
import com.google.common.hash.Hashing;
import java.nio.file.Path;
import java.util.*;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Helper functions to generate dcids for various class types.
// Unless a node passes checks, its dcid cannot be generated. The functions in this class assume
// those checks have already happened. So they do not report anything in LogWrapper. If a dcid
// was not successfully generated, then a prior check will say why that is. This is to avoid
// double counting failures.
//
// TODO: Have a way to assert that node whose dcid cannot be generated has an ERROR entry in
//  LogWrapper.
// TODO: Support hashed short-IDs for places. farmhash32 doesn't seem to exist in Java.
public class DcidGenerator {
  public static boolean TEST_MODE = false;

  private static final String DC_NAMESPACE = "dc/";
  private static final String OBS_NAMESPACE = "o/";
  private static final String SVOBS_NAMESPACE = "o/";
  private static final String POP_NAMESPACE = "p/";

  // This is an int->char map of 32 chars.
  private static final char[] DCID_BASE32_MAP = {
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'b', 'c', 'd', 'f', 'g', 'h', 'j', 'k', 'l',
    'm', 'n', 'p', 'q', 'r', 's', 't', 'v', 'w', 'x', 'y', 'z', 'e'
  };
  // Max length of a base 32 encoded version of a 64-bit string: ceil(64 / 5)
  private static final int MAX_LONG_ID_LEN = 13;
  // Max length of a base 32 encoded version of a 32-bit string: ceil(32 / 5)
  private static final int MAX_SHORT_ID_LEN = 7;
  // Max number of bits of a 32-bit encoding
  private static final int MAX_NUM_BITS = 5;

  // Set of place types that use random short-ID. Typically though their IDs are derived from
  // external IDs (like geoId/06).
  private static final Set<String> TYPES_USING_SHORT_ID =
      Set.of(
          "City",
          "County",
          "Province",
          "State",
          "Country",
          "Continent",
          "CollegeOrUniversity",
          "Source",
          "Curator",
          "Provenance",
          "AdministrativeArea",
          "AdministrativeArea1",
          "AdministrativeArea2",
          "AdministrativeArea3",
          "AdministrativeArea4",
          "AdministrativeArea5",
          "Place",
          "Town",
          "Village",
          "Neighborhood");

  private static final Set<String> FIXED_STAT_VAR_PROPS =
      Set.of(
          Vocabulary.TYPE_OF,
          Vocabulary.POPULATION_TYPE,
          Vocabulary.MEASURED_PROP,
          Vocabulary.STAT_TYPE,
          Vocabulary.MEASUREMENT_QUALIFIER,
          Vocabulary.MEASUREMENT_DENOMINATOR);

  private static final List<String> ORDERED_STAT_VAR_OBS_KEY_PROPS =
      List.of(
          Vocabulary.OBSERVATION_ABOUT,
          Vocabulary.VARIABLE_MEASURED,
          Vocabulary.OBSERVATION_DATE,
          Vocabulary.GENERIC_VALUE,
          Vocabulary.OBSERVATION_PERIOD,
          Vocabulary.UNIT,
          Vocabulary.MEASUREMENT_METHOD,
          Vocabulary.SCALING_FACTOR);
  static final int LAST_REQUIRED_STAT_VAR_OBS_PROP_INDEX =
      ORDERED_STAT_VAR_OBS_KEY_PROPS.indexOf(Vocabulary.GENERIC_VALUE);

  private static final List<String> ORDERED_LEGACY_OBS_KEY_PROPS =
      List.of(
          Vocabulary.OBSERVED_NODE,
          Vocabulary.OBSERVATION_DATE,
          Vocabulary.MEASURED_PROP,
          Vocabulary.OBSERVATION_PERIOD,
          Vocabulary.UNIT,
          Vocabulary.MEASURED_VALUE,
          Vocabulary.MIN_VALUE,
          Vocabulary.MAX_VALUE,
          Vocabulary.SUM_VALUE,
          Vocabulary.MEAN_VALUE,
          Vocabulary.MEDIAN_VALUE,
          Vocabulary.MARGIN_OF_ERROR,
          Vocabulary.MEASUREMENT_METHOD,
          Vocabulary.MEASUREMENT_RESULT,
          Vocabulary.STD_ERROR,
          Vocabulary.SAMPLE_SIZE,
          Vocabulary.PERCENTILE_10,
          Vocabulary.PERCENTILE_25,
          Vocabulary.PERCENTILE_75,
          Vocabulary.PERCENTILE_90,
          Vocabulary.MEAN_STD_ERROR,
          Vocabulary.GROWTH_RATE,
          Vocabulary.STD_DEVIATION_VALUE,
          Vocabulary.MEASUREMENT_DENOMINATOR,
          Vocabulary.MEASUREMENT_QUALIFIER,
          Vocabulary.SCALING_FACTOR);
  static final int LAST_REQUIRED_LEGACY_OBS_PROP_INDEX =
      ORDERED_LEGACY_OBS_KEY_PROPS.indexOf(Vocabulary.MEASURED_PROP);

  private static final LogWrapper dummyLogCtx =
      new LogWrapper(Debug.Log.newBuilder(), Path.of("/tmp/report.html"));

  public static class Result {
    // The dcid will be empty if there were any errors in the node. These should have been
    // reported earlier by McfChecker.
    public String dcid = new String();
    // For SVObs/Pop types, dcid is a content hash. This is the string that went into the ID, and
    // it is used for debugging purposes.
    public String keyString = new String();
  }

  // The logic generally assumes "pvs" have already been sanity-checked.
  public static Result forStatVar(String nodeId, Mcf.McfGraph.PropertyValues pvs) {
    List<String> props = new ArrayList<>();
    for (Map.Entry<String, Mcf.McfGraph.Values> pv : pvs.getPvsMap().entrySet()) {
      String p = pv.getKey();
      // We should include in the key all fixed properties of StatVar and all
      // constraint-props.  "typeOf" is included because there can be multiple
      // StatVar-like types (StatisticalVariable, StatisticalVariableSlice) and it
      // helps disambiguating.
      if (FIXED_STAT_VAR_PROPS.contains(p)
          || !Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(p)) {
        props.add(p);
      }
    }
    Collections.sort(props);

    Result result = new Result();
    String typeOf = McfUtil.getPropVal(pvs, Vocabulary.TYPE_OF);
    if (typeOf.isEmpty()) {
      return result;
    }
    List<String> keyParts = new ArrayList<>();
    var pvMap = pvs.getPvsMap();
    for (var prop : props) {
      if (!pvMap.containsKey(prop)) continue;
      var vals = pvMap.get(prop);
      if (vals.getTypedValuesCount() == 1) {
        var tv = vals.getTypedValues(0);
        if (tv.getType() == Mcf.ValueType.COMPLEX_VALUE) {
          ComplexValueParser cvp =
              new ComplexValueParser(nodeId, pvs, prop, tv.getValue(), null, dummyLogCtx);
          if (!cvp.parse()) {
            // Failed. Return empty Result.
            return result;
          }
          keyParts.add(prop + "=" + cvp.getDcid());
        } else {
          keyParts.add(prop + "=" + tv.getValue());
        }
      }
    }
    result.keyString = String.join("", keyParts);
    result.dcid = forSerializedPropVals(typeOf, result.keyString);
    return result;
  }

  public static Result forStatVarObs(String nodeId, Mcf.McfGraph.PropertyValues pvs) {
    String typeOf = McfUtil.getPropVal(pvs, Vocabulary.TYPE_OF);
    Result result = new Result();
    if (typeOf.isEmpty()) {
      return result;
    }

    List<String> keyParts = new ArrayList<>();
    for (int i = 0; i < ORDERED_STAT_VAR_OBS_KEY_PROPS.size(); i++) {
      var prop = ORDERED_STAT_VAR_OBS_KEY_PROPS.get(i);
      var tvs = McfUtil.getPropTvs(pvs, prop);
      if (tvs == null || tvs.isEmpty()) {
        if (i <= LAST_REQUIRED_STAT_VAR_OBS_PROP_INDEX) {
          // Failure.  A missing property is required, and this should have been reported earlier.
          return result;
        }
        continue;
      }
      if (tvs.size() != 1) {
        // Rest of the properties should be singleton.
        return result;
      }
      var val = tvs.get(0).getValue();
      if ((prop.equals(Vocabulary.VARIABLE_MEASURED) || prop.equals(Vocabulary.OBSERVATION_ABOUT))
          && val.startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
        // One of the values is still a local-ref, cannot assign DCID.
        return result;
      }
      keyParts.add(prop + "=" + val);
    }
    result.keyString = String.join("", keyParts);
    result.dcid = forSerializedPropVals(typeOf, result.keyString);
    return result;
  }

  public static Result forPlace(Mcf.McfGraph.PropertyValues pvs) {
    Result result = new Result();
    for (var prop : Vocabulary.PLACE_RESOLVABLE_AND_ASSIGNABLE_IDS) {
      var val = McfUtil.getPropVal(pvs, prop);
      if (!val.isEmpty()) {
        String prefix = prop;
        if (prop.equals(Vocabulary.ISO_CODE)) {
          prefix = "iso";
        } else if (prop.equals(Vocabulary.NUTS_CODE)) {
          prefix = "nuts";
        }
        result.dcid = prefix + "/" + val;
        break;
      }
    }
    return result;
  }

  public static String getRandomDcid(String schemaType) {
    String randomStr =
        TEST_MODE ? schemaType + "22" : String.valueOf(UUID.randomUUID().getLeastSignificantBits());
    return forSerializedPropVals(schemaType, randomStr);
  }

  public static Result forPopulation(String nodeId, Mcf.McfGraph.PropertyValues pvs) {
    Result result = new Result();
    // Do a pass to build pvConstraints if provided. Also strip refs.
    Map<String, String> pvMap = new HashMap<>();
    Set<String> constraints = new HashSet<>();
    for (var pv : pvs.getPvsMap().entrySet()) {
      var prop = pv.getKey();
      var vals = pv.getValue();
      if (vals.getTypedValuesCount() == 0) continue;
      if (prop.equals(Vocabulary.CONSTRAINT_PROPS)) {
        for (var tv : vals.getTypedValuesList()) {
          constraints.add(tv.getValue());
        }
      } else {
        var tv = vals.getTypedValues(0);
        if (tv.getType() == Mcf.ValueType.COMPLEX_VALUE) {
          ComplexValueParser cvp =
              new ComplexValueParser(nodeId, pvs, prop, tv.getValue(), null, dummyLogCtx);
          if (!cvp.parse()) {
            // Failed. Return empty Result.
            return result;
          }
          pvMap.put(prop, cvp.getDcid());
        } else {
          if (prop.equals(Vocabulary.LOCATION)
              && tv.getValue().startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
            // Location has local-ref, so return empty dcid.
            return result;
          }
          pvMap.put(prop, tv.getValue());
        }
      }
    }

    List<String> ordered_cprops = new ArrayList<>();
    for (var pv : pvMap.entrySet()) {
      var prop = pv.getKey();
      var val = pv.getValue();
      if (Vocabulary.NON_CONSTRAINT_STAT_VAR_PROPERTIES.contains(prop)) {
        // Avoid the well-known props that don't go into PV.
        continue;
      }
      if (!constraints.isEmpty() && !constraints.contains(prop)) {
        // 'constraintProperties' was provided and it doesn't include this prop.
        continue;
      }
      ordered_cprops.add(prop);
    }
    Collections.sort(ordered_cprops);

    List<String> keyParts = new ArrayList<>();

    var popType = McfUtil.getPropVal(pvs, Vocabulary.POPULATION_TYPE);
    if (popType.isEmpty()) return result;
    keyParts.add(popType);

    var location = McfUtil.getPropVal(pvs, Vocabulary.LOCATION);
    if (location.isEmpty()) return result;
    keyParts.add(location);

    keyParts.add(McfUtil.getPropVal(pvs, Vocabulary.MEMBER_OF));
    for (var cprop : ordered_cprops) {
      keyParts.add(cprop + pvMap.get(cprop));
    }
    result.keyString = String.join("", keyParts);
    result.dcid = forSerializedPropVals(Vocabulary.LEGACY_POPULATION_TYPE_SUFFIX, result.keyString);
    return result;
  }

  public static Result forObservation(String nodeId, Mcf.McfGraph.PropertyValues pvs) {
    Result result = new Result();
    List<String> keyParts = new ArrayList<>();
    for (int i = 0; i < ORDERED_LEGACY_OBS_KEY_PROPS.size(); i++) {
      var prop = ORDERED_LEGACY_OBS_KEY_PROPS.get(i);
      var tvs = McfUtil.getPropTvs(pvs, prop);
      if (tvs == null || tvs.isEmpty()) {
        if (i <= LAST_REQUIRED_LEGACY_OBS_PROP_INDEX) {
          // Failure.  A missing property is required, and this should have been reported earlier.
          return result;
        }
        continue;
      }
      if (tvs.size() != 1) {
        // Rest of the properties should be singleton.
        return result;
      }
      var val = tvs.get(0).getValue();
      if (prop.equals(Vocabulary.LOCATION) && val.startsWith(Vocabulary.INTERNAL_REF_PREFIX)) {
        // One of the values is still a local-ref, cannot assign DCID.
        return result;
      }
      keyParts.add(prop + "=" + val);
    }
    result.keyString = String.join("", keyParts);
    result.dcid =
        forSerializedPropVals(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX, result.keyString);
    return result;
  }

  private static String forSerializedPropVals(String schemaType, String serializedPropVals) {
    if (schemaType.endsWith(Vocabulary.LEGACY_POPULATION_TYPE_SUFFIX)) {
      return DC_NAMESPACE + POP_NAMESPACE + getLongId(serializedPropVals);
    } else if (schemaType.equals(Vocabulary.STAT_VAR_OBSERVATION_TYPE)) {
      return DC_NAMESPACE + SVOBS_NAMESPACE + getLongId(serializedPropVals);
    } else if (schemaType.endsWith(Vocabulary.LEGACY_OBSERVATION_TYPE_SUFFIX)) {
      return DC_NAMESPACE + OBS_NAMESPACE + getLongId(serializedPropVals);
    } else if (TYPES_USING_SHORT_ID.contains(schemaType)) {
      throw new UnsupportedOperationException(
          "Opaque short ID for type "
              + schemaType
              + " is "
              + "unsupported! Please use the DC prod tooling!");
    } else {
      return DC_NAMESPACE + getLongId(serializedPropVals);
    }
  }

  // Performs base32 encoding and returns the number of bytes of id_buf filled up.
  private static int base32Encode(char[] id_buf, int buf_sz, Long id) {
    int i = 0;
    for (; i < buf_sz; ++i) {
      int v = Math.toIntExact(id & 0x1f);
      if (v < 0 || v >= DCID_BASE32_MAP.length) {
        throw new NumberFormatException("Unexpected integer arithmetic error: " + v);
      }
      id_buf[i] = DCID_BASE32_MAP[v];
      // NOTE: It is important that this is a logical right shift (>>>) without sign extension.
      id = id >>> MAX_NUM_BITS;
      if (id == 0) return i + 1;
    }
    throw new ArithmeticException("Unexpected base32Encoding error: '" + id_buf + "' : " + i);
  }

  private static String getLongId(String input) {
    char[] buf = new char[MAX_LONG_ID_LEN];
    Long fp = Hashing.farmHashFingerprint64().hashString(input, Charsets.UTF_8).asLong();
    int l = base32Encode(buf, MAX_LONG_ID_LEN, fp);
    return new String(buf, 0, l);
  }
}
