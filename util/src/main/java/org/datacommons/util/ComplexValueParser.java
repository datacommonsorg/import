package org.datacommons.util;

import static org.datacommons.proto.Mcf.ValueType.*;

import java.util.List;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Parse complex value strings into nodes.
//
// 1. a Quantity/QuantityRange value, coded as one of:
//  [<unit> <val>]
//  [<unit> <startval> <endval>]
//  [<unit> - <endval>]
//  [<unit> <startval> -]
//
// 2. a GeoCoordinates type, coded as one of:
//  [LatLong <lat_value> <long_value>]
//  [<lat_value> <long_value> LatLong]
//
// Computes the dcid and (optionally) populates the PVs for the complex node.
public class ComplexValueParser {
  public String mainNodeId;
  public Mcf.McfGraph.PropertyValues mainNode;
  public String prop;
  public String complexValue;
  Mcf.McfGraph.PropertyValues.Builder complexNode;
  public LogWrapper logCtx;
  private String dcid;
  private String name;

  // NOTE: complexValue is assumed to be trimmed. Since it comes from already parsed TypedValue
  // .value
  public ComplexValueParser(
      String mainNodeId,
      Mcf.McfGraph.PropertyValues mainNode,
      String prop,
      String complexValue,
      Mcf.McfGraph.PropertyValues.Builder complexNode,
      LogWrapper logCtx) {
    this.mainNode = mainNode;
    this.mainNodeId = mainNodeId;
    this.prop = prop;
    this.complexValue = complexValue;
    this.complexNode = complexNode;
    this.logCtx = logCtx;
    dcid = new String();
    name = new String();
  }

  public String getDcid() {
    return dcid;
  }

  public boolean parse() {
    if (!complexValue.startsWith("[") || !complexValue.endsWith("]")) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_UnenclosedComplexValue",
          "Bad complex value '"
              + complexValue
              + "' not enclosed in brackets in "
              + "property "
              + prop
              + " in node "
              + mainNodeId,
          mainNode.getLocationsList());
      return false;
    }

    String trimmedComplexValue = complexValue.substring(1, complexValue.length() - 1);
    McfParser.SplitAndStripArg arg = new McfParser.SplitAndStripArg();
    arg.delimiter = ' ';
    arg.includeEmpty = false;
    arg.stripEnclosingQuotes = false;
    // TODO: Passthru errCb
    List<String> fields = McfParser.splitAndStripWithQuoteEscape(trimmedComplexValue, arg, null);
    if (fields.isEmpty()) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_MalformedComplexValueString",
          "Found malformed complex value '"
              + complexValue
              + "' in property "
              + prop
              + " in "
              + "node "
              + mainNodeId,
          mainNode.getLocationsList());
      return false;
    }
    if (fields.size() != 2 && fields.size() != 3) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_MalformedComplexValueParts",
          "Complex value must have 2 (e.g., [Years 10]) or 3 (e.g., [Years 10 20]) "
              + "components but '"
              + complexValue
              + "' in property "
              + prop
              + " in "
              + "node "
              + mainNodeId
              + " has "
              + fields.size(),
          mainNode.getLocationsList());
      return false;
    }
    boolean isRange = fields.size() == 3;

    // Find out the offset of the various fields.
    int startIdx = -1, endIdx = -1, valueIdx = -1, unitIdx = -1;
    if (fields.get(0).charAt(0) == '-' || Character.isDigit(fields.get(0).charAt(0))) {
      // First field is value
      if (isRange) {
        unitIdx = 2;
        startIdx = 0;
        endIdx = 1;
      } else {
        unitIdx = 1;
        valueIdx = 0;
      }
    } else {
      // First field is unit
      if (isRange) {
        unitIdx = 0;
        startIdx = 1;
        endIdx = 2;
      } else {
        unitIdx = 0;
        valueIdx = 1;
      }
    }

    // Get unit.
    String unit;
    int colonIndex = fields.get(unitIdx).indexOf(Vocabulary.REFERENCE_DELIMITER);
    if (colonIndex != -1) {
      unit = fields.get(unitIdx).substring(colonIndex + 1);
    } else {
      unit = fields.get(unitIdx);
    }

    // Compute DCID.
    boolean isLatLng = false;
    if (fields.size() == 2) {
      if (!McfUtil.isNumber(fields.get(valueIdx))) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "MCF_QuantityMalformedValue",
            "Quantity value '"
                + complexValue
                + "' in property "
                + prop
                + " in node "
                + mainNodeId
                + " must be a number",
            mainNode.getLocationsList());
        return false;
      }
      dcid = unit + fields.get(valueIdx);
      name = unit + " " + fields.get(valueIdx);
    } else {
      assert fields.size() == 3;
      if (unit.toLowerCase().equals(Vocabulary.LAT_AND_LONG.toLowerCase())) {
        isLatLng = true;
        if (!parseLatLng(fields.get(startIdx), fields.get(endIdx))) {
          // On error parseLatLng would have updated logCtx
          return false;
        }
      } else {
        if (!parseQuantityRange(fields.get(startIdx), fields.get(endIdx), unit)) {
          // On error parseQuantityRange would have updated logCtx
          return false;
        }
      }
    }

    if (complexNode != null) {
      complexNode.putPvs(Vocabulary.DCID, McfUtil.newValues(TEXT, dcid));
      complexNode.putPvs(Vocabulary.NAME, McfUtil.newValues(TEXT, name));
      if (isLatLng) {
        complexNode.putPvs(
            Vocabulary.TYPE_OF, McfUtil.newValues(RESOLVED_REF, Vocabulary.GEO_COORDINATES_TYPE));
        complexNode.putPvs(Vocabulary.LATITUDE, McfUtil.newValues(TEXT, fields.get(startIdx)));
        complexNode.putPvs(Vocabulary.LONGITUDE, McfUtil.newValues(TEXT, fields.get(endIdx)));
      } else {
        if (fields.size() == 2) {
          complexNode.putPvs(
              Vocabulary.TYPE_OF, McfUtil.newValues(RESOLVED_REF, Vocabulary.QUANTITY_TYPE));
          complexNode.putPvs(Vocabulary.VALUE, McfUtil.newValues(NUMBER, fields.get(valueIdx)));
        } else {
          complexNode.putPvs(
              Vocabulary.TYPE_OF, McfUtil.newValues(RESOLVED_REF, Vocabulary.QUANTITY_RANGE_TYPE));
          complexNode.putPvs(
              Vocabulary.START_VALUE,
              McfUtil.newValues(
                  fields.get(startIdx).equals("-") ? TEXT : NUMBER, fields.get(startIdx)));
          complexNode.putPvs(
              Vocabulary.END_VALUE,
              McfUtil.newValues(
                  fields.get(endIdx).equals("-") ? TEXT : NUMBER, fields.get(endIdx)));
        }
        complexNode.putPvs(Vocabulary.UNIT, McfUtil.newValues(RESOLVED_REF, unit));
      }
      if (mainNode.getLocationsCount() > 0) {
        complexNode.addAllLocations(mainNode.getLocationsList());
      }
    }
    return true;
  }

  private boolean parseLatLng(String latStr, String lngStr) {
    double lat, lng;

    if (latStr.toUpperCase().endsWith("N")) {
      latStr = latStr.substring(0, latStr.length() - 1);
    } else if (latStr.toUpperCase().endsWith("S")) {
      latStr = "-" + latStr.substring(0, latStr.length() - 1);
    }
    try {
      lat = Double.parseDouble(latStr);
      if (lat < -90.0 || lat > 90.0) {
        throw new NumberFormatException("Invalid latitude");
      }
    } catch (NumberFormatException ex) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_InvalidLatitude",
          "Invalid latitude value ("
              + latStr
              + ") in property "
              + prop
              + " of node "
              + mainNodeId
              + ". The expected format is decimal degrees, with an optional "
              + "N/S suffix.",
          mainNode.getLocationsList());
      return false;
    }

    if (lngStr.toUpperCase().endsWith("E")) {
      lngStr = lngStr.substring(0, lngStr.length() - 1);
    } else if (lngStr.toUpperCase().endsWith("W")) {
      lngStr = "-" + lngStr.substring(0, lngStr.length() - 1);
    }
    try {
      lng = Double.parseDouble(lngStr);
      if (lng < -180.0 || lng > 180.0) {
        throw new NumberFormatException("Invalid longitude");
      }
    } catch (NumberFormatException ex) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_InvalidLongitude",
          "Invalid longitude value ("
              + lngStr
              + ") in property "
              + prop
              + " of node "
              + mainNodeId
              + ". The expected format is decimal degrees, with an optional "
              + "N/S suffix.",
          mainNode.getLocationsList());
      return false;
    }

    // E5 (1/100000th of a degree) or 1 meter is the maximum resolution we
    // support.
    long lat_e5 = Math.round(1e5 * lat);
    long lng_e5 = Math.round(1e5 * lng);
    latStr = String.format("%.5f", ((double) lat_e5 / 1e5));
    lngStr = String.format("%.5f", ((double) lng_e5 / 1e5));

    dcid = Vocabulary.GEO_DCID_PREFIX + "/" + lat_e5 + "_" + lng_e5;
    name = latStr + "," + lngStr;
    return true;
  }

  private boolean parseQuantityRange(String startVal, String endVal, String unit) {
    if (!(McfUtil.isNumber(startVal) || startVal.equals("-"))) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_QuantityRangeMalformedValues",
          "Malformed start component in QuantityRange value ("
              + startVal
              + ") "
              + " in property "
              + prop
              + " in node "
              + mainNodeId
              + ". The start component must be a number or '-'",
          mainNode.getLocationsList());
      return false;
    }
    if (!(McfUtil.isNumber(endVal) || endVal.equals("-"))) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_QuantityRangeMalformedValues",
          "Malformed end component in QuantityRange value ("
              + endVal
              + ") "
              + " in property "
              + prop
              + " in node "
              + mainNodeId
              + ". The end component must be a number or '-'",
          mainNode.getLocationsList());
      return false;
    }
    if (startVal.equals("-") && endVal.equals("-")) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_QuantityRangeMalformedValues",
          "Malformed start+end components in QuantityRange value ("
              + startVal
              + ", "
              + endVal
              + ") in property "
              + prop
              + " in node "
              + mainNodeId
              + ". Both start and end cannot be '-', one of them must be a number",
          mainNode.getLocationsList());
      return false;
    }
    if (startVal.equals("-")) {
      dcid = unit + "Upto" + endVal;
      name = unit + " UpTo " + endVal;
    } else if (endVal.equals("-")) {
      dcid = unit + startVal + "Onwards";
      name = unit + " " + startVal + " Onwards";
    } else {
      dcid = unit + startVal + "To" + endVal;
      name = unit + " " + startVal + " To " + endVal;
    }
    return true;
  }
}
