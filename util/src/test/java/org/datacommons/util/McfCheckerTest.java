package org.datacommons.util;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Set;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

public class McfCheckerTest {

  @Test
  public void checkBasic() throws IOException {
    // Missing typeOf.
    String mcf = "Node: USState\n" + "name: \"California\"";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_typeOf", "property 'typeOf' in node USState"));

    // Good node.
    mcf = "Node: USState\n" + "typeOf: schema:State\n" + "name: \"California\"";
    assertTrue(success(mcf));

    // A valid node.
    mcf =
        "Node: USState\n"
            + "typeOf: schema:State\n"
            + "name: California\n"
            + "\n"
            + "Node: USCity\n"
            + "typeOf: City\n"
            + "name: San Francisco\n"
            + "containedIn: \"USState\"\n";
    assertTrue(success(mcf));

    // A valid node with complex value.
    mcf =
        "Node: EQ\n"
            + "typeOf: dcs:Earthquake\n"
            + "name: \"The Large One\"\n"
            + "location: [LatLong 41.4 -81.9]\n";
    assertTrue(success(mcf));
  }

  @Test
  public void checkDcid() throws IOException {
    // DCID must have exactly one value.
    String mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: geoId/06, geoId/07\n";
    assertTrue(failure(mcf, "Sanity_MultipleDcidValues", "dcid must have exactly one"));

    // DCID must not be too long.
    mcf =
        "Node: USState\n"
            + "typeOf: schema:State\n"
            + "dcid: "
            + new String(new char[257]).replace('\0', 'x');
    assertTrue(failure(mcf, "Sanity_VeryLongDcid", "a very long dcid value"));

    // DCID must not have space.
    mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: \"dc/Not Allowed\"\n";
    assertTrue(failure(mcf, "Sanity_SpaceInDcid", "whitespace in dcid"));

    // Temporarily, bio/ DCIDs can have space.
    mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: \"bio/For Now Allowed\"\n";
    assertTrue(success(mcf));
  }

  @Test
  public void checkPopObs() throws IOException {
    // A valid Pop node.
    String mcf =
        "Node: USState\n"
            + "typeOf: schema:State\n"
            + "name: \"California\"\n"
            + "\n"
            + "Node: USStateAll\n"
            + "typeOf: schema:Population\n"
            + "populationType: schema:Person\n"
            + "location: l:USState\n";
    assertTrue(success(mcf));

    // A bad observation node with no value.
    mcf =
        "Node: USStateFemalesIncome\n"
            + "typeOf: schema:Observation\n"
            + "measuredProperty: schema:income\n"
            + "observedNode: l:USStateFemales\n"
            + "observationDate: \"2010\"\n"
            + "observationPeriod: \"P1M\"\n"
            + "unit: \"AnnualUSDollars\"\n";
    assertTrue(failure(mcf, "Sanity_ObsMissingValueProp", "Missing any value property"));

    // Non-double measured value.
    mcf =
        "Node: USStateFemalesIncome\n"
            + "typeOf: schema:Observation\n"
            + "measuredProperty: schema:income\n"
            + "observedNode: l:USStateFemales\n"
            + "measuredValue: \"nonqualified\"\n"
            + "observationDate: \"2017\"\n"
            + "observationPeriod: \"P1M\"\n"
            + "unit: \"AnnualUSDollars\"\n";
    assertTrue(failure(mcf, "Sanity_NonDoubleObsValue", "double value"));

    // No date.
    mcf =
        "Node: USStateFemalesIncome\n"
            + "typeOf: schema:Observation\n"
            + "measuredProperty: schema:income\n"
            + "observedNode: l:USStateFemales\n"
            + "unit: \"AnnualUSDollars\"\n"
            + "medianValue: 200000\n";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_observationDate", "observationDate"));

    // An observation with nan.
    mcf =
        "Node: USStateFemalesIncome\n"
            + "typeOf: schema:Observation\n"
            + "measuredProperty: schema:income\n"
            + "observedNode: l:USStateFemales\n"
            + "observationDate: \"2017-07\"\n"
            + "medianValue: nan\n";
    assertTrue(failure(mcf, "Sanity_NonDoubleObsValue", "double value"));

    // Measured property name should start with lower case.
    mcf =
        "Node: USStateFemalesIncome\n"
            + "typeOf: schema:Observation\n"
            + "measuredProperty: schema:Income\n"
            + "observedNode: dcid:dc/p/0x34534\n"
            + "observationDate: \"2017-07\"\n"
            + "measuredValue: 200000\n";
    assertTrue(failure(mcf, "Sanity_NotInitLower_measuredProperty", "Income"));

    // popType should start with upper case.
    mcf =
        "Node: USStateFemales\n"
            + "typeOf: schema:Population\n"
            + "populationType: schema:mortalityEvent\n"
            + "location: l:USState\n";
    assertTrue(failure(mcf, "Sanity_NotInitUpper_populationType", "mortalityEvent"));
  }

  @Test
  public void checkStatVar() throws IOException {
    // Missing popType
    String mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "measuredProperty: dcs:count\n"
            + "statType: dcs:measuredValue";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_populationType", "populationType in node SV"));

    // Non-class popType
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: dcs:person\n"
            + "measuredProperty: dcs:count\n"
            + "statType: dcs:measuredValue";
    assertTrue(failure(mcf, "Sanity_NotInitUpper_populationType", "person"));

    // Missing mprop
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:measuredValue";
    assertTrue(
        failure(mcf, "Sanity_MissingOrEmpty_measuredProperty", "measuredProperty in node SV"));

    // Non-prop mprop
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:measuredValue\n"
            + "measuredProperty: dcs:Income";
    assertTrue(failure(mcf, "Sanity_NotInitLower_measuredProperty", "Income"));

    // Unknown statType
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:projection\n"
            + "measuredProperty: dcs:income";
    assertTrue(failure(mcf, "Sanity_UnknownStatType", "projection"));
  }

  @Test
  public void checkSVObs() throws IOException {
    // A good StatVarObs node with double value.
    String mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n";
    assertTrue(success(mcf));

    // Another good StatVarObs node with string value.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: \"DataSuppressed\"\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n";
    assertTrue(success(mcf));

    // A bad StatVarObs node with no value.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_value", "value in node SFWomenIncome2020"));

    // A bad StatVarObs node with no variableMeasured.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n";
    assertTrue(
        failure(
            mcf,
            "Sanity_MissingOrEmpty_variableMeasured",
            "variableMeasured in node SFWomenIncome2020"));

    // A bad StatVarObs node with no observationAbout.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "value: 10000000.0\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "observationDate: \"2020\"\n";
    assertTrue(
        failure(
            mcf,
            "Sanity_MissingOrEmpty_observationAbout",
            "observationAbout in node SFWomenIncome2020"));

    // A bad StatVarObs node with no date.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "value: 10000000.0\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "observationAbout: dcid:geoId/SF\n";
    assertTrue(
        failure(
            mcf,
            "Sanity_MissingOrEmpty_observationDate",
            "observationDate in node SFWomenIncome2020"));

    // A bad StatVarObs node with bogus date.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "value: 10000000.0\n"
            + "observationDate: \"January, 2020\"\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "observationAbout: dcid:geoId/SF\n";
    assertTrue(failure(mcf, "Sanity_InvalidObsDate", "non-ISO8601 compliant value"));
  }

  @Test
  public void checkSchema() throws IOException {
    // Property names must start with lower case.
    String mcf =
        "Node: dcid:Place\n"
            + "typeOf: schema:Class\n"
            + "Name: \"Place\"\n"
            + "description: \"Physical location.\"\n";
    assertTrue(
        failure(mcf, "Sanity_NotInitLowerPropName", "'Name' does not start with a lower-case"));

    // Class types must have upper-case names/IDs.
    mcf =
        "Node: dcid:place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "description: \"Physical location.\"\n";
    assertTrue(failure(mcf, "Sanity_NotInitUpper_dcidInClass", "place"));

    // No non-ascii values for non-text items.
    mcf = "Node: dcid:geoId/06\n" + "typeOf: schema:Plaçe\n" + "capital: \"Sacramento\"\n";
    assertTrue(failure(mcf, "Sanity_NonAsciiValueInNonText", "Plaçe"));

    // No non-ascii values for non-text items.
    mcf =
        "Node: dcid:Place\n"
            + "typeOf: schema:Class\n"
            + "subClassOf: schema:Thing\n"
            + "name: “Place”\n"
            + "description: \"Physical location.\"\n";
    assertTrue(failure(mcf, "Sanity_NonAsciiValueInSchema", "“Place”"));

    // Property types must have lower-case names/IDs.
    mcf =
        "Node: dcid:age\n"
            + "typeOf: schema:Property\n"
            + "name: \"Age\"\n"
            + "domainIncludes: schema:Person\n"
            + "rangeIncludes: schema:Number\n"
            + "description: \"Person Age.\"\n";
    assertTrue(failure(mcf, "Sanity_NotInitLower_nameInProperty", "Age"));

    // Property types must not have subClassOf.
    mcf =
        "Node: dcid:age\n"
            + "typeOf: schema:Property\n"
            + "name: \"age\"\n"
            + "subClassOf: schema:Person\n";
    assertTrue(failure(mcf, "Sanity_UnexpectedPropInProperty", "subClassOf"));

    // Class types must include subClassOf.
    mcf =
        "Node: dcid:Place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "description: \"Physical location.\"\n";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_subClassOf", "subClassOf"));

    // Class types must not have domainIncludes|rangeIncludes|subPropertyOf
    mcf =
        "Node: schema:Place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "domainIncludes: schema:Person\n";
    assertTrue(failure(mcf, "Sanity_UnexpectedPropInClass", "domainIncludes"));

    mcf =
        "Node: schema:Place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "rangeIncludes: schema:Person\n";
    assertTrue(failure(mcf, "Sanity_UnexpectedPropInClass", "rangeIncludes"));

    mcf =
        "Node: schema:Place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "subPropertyOf: schema:sibling\n";
    assertTrue(failure(mcf, "Sanity_UnexpectedPropInClass", "subPropertyOf"));
  }

  @Test
  public void checkTemplate() throws IOException {
    // Incorrect column name.
    String mcf =
        "Node: E:CityStats->E1\n"
            + "typeOf: State\n"
            + "dcid: C:CityStats->StateId\n"
            + "name: C:CityStats->StateNam\n";
    assertTrue(
        failure(
            mcf,
            "Sanity_TmcfMissingColumn",
            "'StateNam' referred",
            Set.of("StateId", "StateName")));

    mcf =
        "Node: E:CityStats->E1\n"
            + "typeOf: State\n"
            + "dcid: C:CityStats->StateId\n"
            + "name: C:CityStats->StateName\n"
            + "\n"
            + "Node: E:CityStats->E2\n"
            + "typeOf: City\n"
            + "name: C:CityStats->CityName\n"
            + "containedIn: E:CityStats->E3\n";
    assertTrue(
        failure(
            mcf,
            "Sanity_TmcfMissingEntityDef",
            "'E:CityStats->E3'",
            Set.of("StateId", "StateName", "CityName")));

    // A legitimate mapping both with and without the columns.
    mcf =
        "Node: E:CityStats->E1\n"
            + "typeOf: State\n"
            + "dcid: C:CityStats->StateId\n"
            + "name: C:CityStats->StateName\n"
            + "\n"
            + "Node: E:CityStats->E2\n"
            + "typeOf: City\n"
            + "name: C:CityStats->City\n"
            + "containedIn: E:CityStats->E1\n";
    assertTrue(success(mcf, Set.of("StateId", "StateName", "City")));
    assertTrue(success(mcf));
  }

  private static boolean failure(
      String mcfString, String counter, String message, Set<String> columns) throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("InMemory"));
    Mcf.McfGraph graph;
    if (columns != null) {
      graph = McfParser.parseTemplateMcfString(mcfString, lw);
    } else {
      graph = McfParser.parseInstanceMcfString(mcfString, false, lw);
    }
    if (McfChecker.check(graph, columns, lw)) {
      System.err.println("Check unexpectedly passed for " + mcfString);
      return false;
    }
    return TestUtil.checkLog(log.build(), counter, message);
  }

  private static boolean failure(String mcfString, String counter, String message)
      throws IOException {
    return failure(mcfString, counter, message, null);
  }

  private static boolean success(String mcfString, Set<String> columns) throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("InMemory"));
    Mcf.McfGraph graph;
    if (columns != null) {
      graph = McfParser.parseTemplateMcfString(mcfString, lw);
    } else {
      graph = McfParser.parseInstanceMcfString(mcfString, false, lw);
    }
    if (!McfChecker.check(graph, columns, lw)) {
      System.err.println("Check unexpectedly failed for \n" + mcfString + "\n :: \n" + log);
      return false;
    }
    return true;
  }

  private static boolean success(String mcfString) throws IOException {
    return success(mcfString, null);
  }
}
