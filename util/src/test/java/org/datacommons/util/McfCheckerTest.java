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

import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class McfCheckerTest {

  @Test
  public void checkBasic() throws IOException, InterruptedException {
    // Missing typeOf.
    String mcf = "Node: USState\n" + "name: \"California\"";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_typeOf", "'typeOf', node: 'USState'"));

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
  public void checkDcid() throws IOException, InterruptedException {
    // DCID must have exactly one value.
    String mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: geoId/06, geoId/07\n";
    assertTrue(failure(mcf, "Sanity_MultipleDcidValues", "dcid with more than one value"));

    // DCID must not be too long.
    mcf =
        "Node: USState\n"
            + "typeOf: schema:State\n"
            + "dcid: "
            + new String(new char[257]).replace('\0', 'x');
    assertTrue(failure(mcf, "Sanity_VeryLongDcid", "a very long dcid value"));

    // DCID must not have space.
    mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: \"dc/Not Allowed\"\n";
    assertTrue(failure(mcf, "Sanity_InvalidChars_dcid", "invalid chars in dcid"));

    // Temporarily, bio/ DCIDs can have space.
    mcf = "Node: USState\n" + "typeOf: schema:State\n" + "dcid: \"bio/For Now Allowed\"\n";
    assertTrue(success(mcf));

    String okCharsId = "A_B&C/D.F-G%H)I(J+K:L";
    mcf = "Node: ID\n" + "typeOf: schema:State\n" + "dcid: \"" + okCharsId + "\"\n";
    assertTrue(success(mcf));

    String bioOnlyBadCharsId = "A*B<C>D]E[F|G;I J'K";
    mcf = "Node: ID\n" + "typeOf: schema:State\n" + "dcid: \"bio/" + bioOnlyBadCharsId + "\"\n";
    assertTrue(success(mcf));
    // Without bio/ prefix, it should fail
    mcf = "Node: ID\n" + "typeOf: schema:State\n" + "dcid: \"" + bioOnlyBadCharsId + "\"\n";
    assertTrue(failure(mcf, "Sanity_InvalidChars_dcid", "invalid-chars: '*<>][|; ''"));

    String badCharsId = "A^B#C~D\\E`F";
    for (var id : List.of(badCharsId, "bio/" + badCharsId)) {
      mcf = "Node: ID\n" + "typeOf: schema:State\n" + "dcid: \"" + id + "\"\n";
      assertTrue(failure(mcf, "Sanity_InvalidChars_dcid", "invalid-chars: '^#~\\`'"));
    }
  }

  @Test
  public void checkPopObs() throws IOException, InterruptedException {
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
    assertTrue(failure(mcf, "Sanity_ObsMissingValueProp", "missing value property"));

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
    assertTrue(failure(mcf, "Sanity_NonDoubleObsValue", "non-double Observation value"));

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
    assertTrue(failure(mcf, "Sanity_NonDoubleObsValue", "non-double Observation value"));

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
  public void checkStatVar() throws IOException, InterruptedException {
    // Missing popType
    String mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "measuredProperty: dcs:count\n"
            + "dcid: \"Count_Person\"\n"
            + "statType: dcs:measuredValue";
    assertTrue(
        failure(
            mcf,
            "Sanity_MissingOrEmpty_populationType",
            "property: 'populationType', " + "node: 'SV'"));

    // Non-class popType
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: dcs:person\n"
            + "measuredProperty: dcs:count\n"
            + "dcid: \"Count_Person\"\n"
            + "statType: dcs:measuredValue";
    assertTrue(failure(mcf, "Sanity_NotInitUpper_populationType", "person"));

    // Missing mprop
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "dcid: \"Count_Person\"\n"
            + "statType: dcs:measuredValue";
    assertTrue(
        failure(mcf, "Sanity_MissingOrEmpty_measuredProperty", "'measuredProperty', node: 'SV'"));

    // Non-prop mprop
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:measuredValue\n"
            + "dcid: \"Income_Person\"\n"
            + "measuredProperty: dcs:Income";
    assertTrue(failure(mcf, "Sanity_NotInitLower_measuredProperty", "Income"));

    // Unknown statType
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:projection\n"
            + "dcid: \"Income_Person\"\n"
            + "measuredProperty: dcs:income";
    assertTrue(failure(mcf, "Sanity_UnknownStatType", "projection"));

    // Missing DCID
    mcf =
        "Node: SV\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "statType: dcs:measuredValue\n"
            + "measuredProperty: dcs:income";
    assertTrue(failure(mcf, "Sanity_MissingOrEmpty_dcid", "SV"));
  }

  @Test
  public void checkSVObs() throws IOException, InterruptedException {
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
    assertTrue(
        failure(
            mcf,
            "Sanity_MissingOrEmpty_value",
            "property: 'value', node: '" + "SFWomenIncome2020'"));

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
            "'variableMeasured', node: 'SFWomenIncome2020'"));

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
            "'observationAbout', node: 'SFWomenIncome2020'"));

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
            "'observationDate', node: 'SFWomenIncome2020'"));

    // A bad StatVarObs node with bogus date.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "value: 10000000.0\n"
            + "observationDate: \"January, 2020\"\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "observationAbout: dcid:geoId/SF\n";
    assertTrue(failure(mcf, "Sanity_InvalidObsDate", "non-ISO8601 compliant"));
  }

  @Test
  public void checkSchema() throws IOException, InterruptedException {
    // Property names must start with lower case.
    String mcf =
        "Node: dcid:Place\n"
            + "typeOf: schema:Class\n"
            + "Name: \"Place\"\n"
            + "description: \"Physical location.\"\n";
    assertTrue(failure(mcf, "Sanity_NotInitLowerPropName", "does not start with a lower-case"));

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
        "Node: dcid:Age\n"
            + "typeOf: schema:Property\n"
            + "name: \"age\"\n"
            + "domainIncludes: schema:Person\n"
            + "rangeIncludes: schema:Number\n"
            + "description: \"Person Age.\"\n";
    assertTrue(failure(mcf, "Sanity_NotInitLower_dcidInProperty", "Age"));

    // Property types must have names and IDs matching.
    mcf =
        "Node: dcid:age\n"
            + "typeOf: schema:Property\n"
            + "name: \"aGe\"\n"
            + "domainIncludes: schema:Person\n"
            + "rangeIncludes: schema:Number\n"
            + "description: \"Person Age.\"\n";
    assertTrue(failure(mcf, "Sanity_DcidNameMismatchInSchema", "aGe"));

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

    // Class types can include multiple subClassOf.
    mcf =
        "Node: dcid:Place\n"
            + "typeOf: schema:Class\n"
            + "name: \"Place\"\n"
            + "description: \"Physical location.\"\n"
            + "subClassOf: schema:Thing, schema:Tangible\n";
    assertTrue(success(mcf));

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
  public void checkTemplate() throws IOException, InterruptedException {
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
            "column: 'StateNam', node: 'E:CityStats->E1'",
            Set.of("StateId", "StateName"),
            false));

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
            Set.of("StateId", "StateName", "CityName"),
            false));

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
    assertTrue(success(mcf, Set.of("StateId", "StateName", "City"), false, true));
    assertTrue(success(mcf, null, false, true));
  }

  // NOTE: This test actually makes RPCs to staging mixer.
  @Test
  public void checkExistence() throws IOException, InterruptedException {
    // SVObs with a legitimate StatVar (Count_Person_Male)
    String mcf =
        "Node: SVO1\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:Count_Person_Male\n"
            + "observationAbout: dcid:country/IdontKnow\n"
            + "observationDate: \"2019\"\n"
            + "value: 10000\n";
    assertTrue(success(mcf, null, true, false));

    // SVObs with a bogus StatVar.
    mcf =
        "Node: SVO2\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:Count_Person_NonExistent\n"
            + "observationAbout: dcid:country/IdontKnow\n"
            + "observationDate: \"2019\"\n"
            + "value: 10000\n";
    assertTrue(
        failure(
            mcf,
            "Existence_MissingValueRef_variableMeasured",
            "Count_Person_NonExistent",
            null,
            true));

    // StatVar with missing popType.
    mcf =
        "Node: SV1\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: dcs:PersonFromMars\n"
            + "measuredProperty: dcs:count\n"
            + "statType: dcs:measuredValue\n";
    assertTrue(
        failure(mcf, "Existence_MissingValueRef_populationType", "PersonFromMars", null, true));

    // StatVar where mprop is not domain of popType
    mcf =
        "Node: SV2\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: dcs:Person\n"
            + "measuredProperty: dcs:receipts\n"
            + "statType: dcs:measuredValue\n";
    assertTrue(
        failure(
            mcf,
            "Existence_MissingPropertyDomainDefinition",
            "property: 'receipts', class: 'Person'",
            null,
            true));

    // StatVar where constrainrProp doesn't exist.
    mcf =
        "Node: SV3\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: dcs:Person\n"
            + "measuredProperty: dcs:count\n"
            + "fooBarRandomProp: dcs:Male\n"
            + "statType: dcs:measuredValue\n";
    assertTrue(
        failure(
            mcf,
            "Existence_MissingPropertyRef",
            "property: 'fooBarRandomProp', node: 'SV3'",
            null,
            true));

    // StatVar where constraintProp's value doesn't exist.
    mcf =
        "Node: SV4\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: dcs:Person\n"
            + "measuredProperty: dcs:count\n"
            + "race: dcs:Jupiterian\n"
            + "statType: dcs:measuredValue\n";
    assertTrue(
        failure(
            mcf,
            "Existence_MissingValueRef_race",
            "reference: 'Jupiterian', property: 'race'",
            null,
            true));

    // Space in a value should fail local sanity check.
    mcf =
        "Node: SV5\n"
            + "typeOf: dcs:RaceCodeEnum\n"
            + "measuredProperty: dcs:count\n"
            + "race: dcs:Jupiterian Saturnarian\n"
            + "statType: dcs:measuredValue\n";
    assertTrue(failure(mcf, "Sanity_InvalidChars_race", "invalid-chars: ' '", null, true));
  }

  private static boolean failure(
      String mcfString,
      String counter,
      String message,
      Set<String> columns,
      boolean doExistenceCheck)
      throws IOException, InterruptedException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("InMemory"));
    ExistenceChecker ec = null;
    if (doExistenceCheck) {
      ec = new ExistenceChecker(HttpClient.newHttpClient(), false, lw);
    }
    Mcf.McfGraph graph;
    if (columns != null) {
      graph = McfParser.parseTemplateMcfString(mcfString, lw);
    } else {
      graph = McfParser.parseInstanceMcfString(mcfString, false, lw);
    }
    if (McfChecker.check(graph, columns, ec, lw)) {
      System.err.println("Check unexpectedly passed for " + mcfString);
      return false;
    }
    return TestUtil.checkLog(log.build(), counter, message);
  }

  private static boolean failure(String mcfString, String counter, String message)
      throws IOException, InterruptedException {
    return failure(mcfString, counter, message, null, false);
  }

  private static boolean success(
      String mcfString, Set<String> columns, boolean doExistenceCheck, boolean isTemplate)
      throws IOException, InterruptedException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("InMemory"));
    Mcf.McfGraph graph;
    if (isTemplate) {
      graph = McfParser.parseTemplateMcfString(mcfString, lw);
    } else {
      graph = McfParser.parseInstanceMcfString(mcfString, false, lw);
    }
    ExistenceChecker ec = null;
    if (doExistenceCheck) {
      ec = new ExistenceChecker(HttpClient.newHttpClient(), false, lw);
    }
    if (!McfChecker.check(graph, columns, ec, lw)) {
      System.err.println("Check unexpectedly failed for \n" + mcfString + "\n :: \n" + log);
      return false;
    }
    return true;
  }

  private static boolean success(String mcfString) throws IOException, InterruptedException {
    return success(mcfString, null, false, false);
  }
}
