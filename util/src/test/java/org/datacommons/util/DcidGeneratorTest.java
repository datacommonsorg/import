package org.datacommons.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.datacommons.proto.Mcf;
import org.junit.Test;

public class DcidGeneratorTest {
  @Test
  public void funcPop() throws IOException {
    String mcf =
        "    Node: Pop1\n"
            + "    localCuratorLevelId: \"Foo\"\n"
            + "    typeOf: dcs:StatisticalPopulation\n"
            + "    populationType: schema:Person\n"
            + "    populationGroup: schema:PersonGroup\n"
            + "    location: dcid:dc/b72vdv\n"
            + "    gender: schema:Male\n"
            + "    age: [Years 18 -]\n"
            + "    label: \"Pop1\"\n";
    var result = DcidGenerator.forPopulation("Pop1", string2Node(mcf));
    assertEquals("Persondc/b72vdvageYears18OnwardsgenderMale", result.keyString);
    assertEquals("dc/p/eekggjy2cqvw3", result.dcid);
  }

  @Test
  public void funcObs() throws IOException {
    String mcf =
        "    Node: Obs\n"
            + "    typeOf: dcs:Observation\n"
            + "    observationDate: \"2017-01\"\n"
            + "    observationPeriod: \"P1M\"\n"
            + "    observedNode: dcid:dc/p/8qws200bgld46\n"
            + "    measuredProperty: dcs:count\n"
            + "    measurementDenominator: dcid:PerCapita\n"
            + "    measurementQualifier: dcs:Nominal\n"
            + "    scalingFactor: \"100\"\n"
            + "    unit: \"$\"\n";
    var result = DcidGenerator.forObservation("Obs", string2Node(mcf));
    assertEquals(
        "observedNode=dc/p/"
            + "8qws200bgld46observationDate=2017-01"
            + "measuredProperty=countobservationPeriod=P1Munit=$"
            + "measurementDenominator=PerCapitameasurementQualifier=Nominal"
            + "scalingFactor=100",
        result.keyString);
    assertEquals("dc/o/57hrlcgch470h", result.dcid);
  }

  @Test
  public void funcObsWithTime() throws IOException {
    String mcf =
        "    Node: Obs\n"
            + "    typeOf: dcs:Observation\n"
            + "    observationDate: \"2018-08-22T12:00Z\"\n"
            + "    observedNode: dcid:dc/p/8qws200bgld46\n"
            + "    measuredProperty: dcs:count\n"
            + "    observedValue: 100\n"
            + "    unit: \"$\"\n";
    var result = DcidGenerator.forObservation("Obs", string2Node(mcf));
    assertEquals("dc/o/yr8wxpd7e1h07", result.dcid);
    assertEquals(
        "observedNode=dc/p/8qws200bgld46"
            + "observationDate=2018-08-22T12:00Z"
            + "measuredProperty=countunit=$",
        result.keyString);
  }

  @Test
  public void funcSVObsSuccess() throws IOException {
    String mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person\n"
            + "    observationAbout: dcid:geoId/06001\n"
            + "    value: 1000\n"
            + "    observationDate: \"2017-01\"\n"
            + "    observationPeriod: \"P1M\"\n"
            + "    measurementMethod: dcid:CACensus\n"
            + "    scalingFactor: \"100\"\n"
            + "    unit: \"$\"\n";
    var result = DcidGenerator.forStatVarObs("SVObs", string2Node(mcf));
    assertEquals("dc/o/3mptcb1vdfvr5", result.dcid);
    assertEquals(
        "observationAbout=geoId/06001"
            + "variableMeasured=Count_Person"
            + "observationDate=2017-01"
            + "value=1000"
            + "observationPeriod=P1M"
            + "unit=$"
            + "measurementMethod=CACensus"
            + "scalingFactor=100",
        result.keyString);
  }

  @Test
  public void funcSVObsHashCollision() throws IOException {
    String mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person_18OrMoreYears_Civilian_Male\n"
            + "    observationAbout: dcid:geoId/41051002901\n"
            + "    value: 1654\n"
            + "    observationDate: \"2014\"\n"
            + "    measurementMethod: dcid:CensusACS5yrSurvey\n";
    var result = DcidGenerator.forStatVarObs("SVObs", string2Node(mcf));
    assertEquals("dc/o/pvb91vhnpw9r3", result.dcid);
    assertEquals(
        "observationAbout=geoId/"
            + "41051002901variableMeasured=Count_Person_18OrMoreYears_Civilian_"
            + "MaleobservationDate=2014value=1654measurementMethod=CensusACS5yrSurvey",
        result.keyString);

    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:dc/25mvfy1h7yws9\n"
            + "    observationAbout: dcid:geoId/3152960\n"
            + "    value: 10\n"
            + "    observationDate: \"2012\"\n"
            + "    measurementMethod: dcid:CensusACS5yrSurvey\n";
    result = DcidGenerator.forStatVarObs("SVObs", string2Node(mcf));
    assertEquals("dc/o/pvb91vhnpw9r3", result.dcid);
    assertEquals(
        "observationAbout=geoId/3152960variableMeasured=dc/"
            + "25mvfy1h7yws9observationDate=2012value=10measurementMethod="
            + "CensusACS5yrSurvey",
        result.keyString);
  }

  @Test
  public void funcSVObsFailure() throws IOException {
    // All these below errors should already fail checker.

    // Missing observationAbout
    String mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person\n"
            + "    value: 1000\n"
            + "    observationDate: \"2017-01\"\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);

    // Missing variableMeasured
    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    observationAbout: dcid:geoId/06001\n"
            + "    value: 1000\n"
            + "    observationDate: \"2017-01\"\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);

    // Missing observationDate
    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person\n"
            + "    observationAbout: dcid:geoId/06001\n"
            + "    value: 1000\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);

    // Missing value
    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person\n"
            + "    observationAbout: dcid:geoId/06001\n"
            + "    observationDate: \"2017-01\"\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);

    // Unresolved variableMeasured
    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: l:LocalStatVarId\n"
            + "    observationAbout: dcid:geoId/06001\n"
            + "    observationDate: \"2017-01\"\n"
            + "    value: 1000\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);

    // Unresolved observationAbout
    mcf =
        "    Node: SVObs\n"
            + "    typeOf: dcs:StatVarObservation\n"
            + "    variableMeasured: dcid:Count_Person\n"
            + "    observationAbout: l:LocalPlaceId\n"
            + "    observationDate: \"2017-01\"\n"
            + "    value: 1000\n";
    assertEquals("", DcidGenerator.forStatVarObs("SVObs", string2Node(mcf)).dcid);
  }

  @Test
  public void funcPlace() throws IOException {
    String mcf =
        "    Node: node_0\n"
            + "    typeOf: dcs:City\n"
            + "    nutsCode: \"nutsCode_node_0\"\n"
            + "    wikidataId: \"wikidataId_node_0\"\n";
    var result = DcidGenerator.forPlace(string2Node(mcf));
    assertEquals("nuts/nutsCode_node_0", result.dcid);

    mcf = "    Node: node_1\n" + "    typeOf: dcs:City\n" + "    name: \"name_node_1\"\n";
    result = DcidGenerator.forPlace(string2Node(mcf));
    assertEquals("", result.dcid);
  }

  @Test
  public void funcPlaceMakesDcidFromOtherIds() throws IOException {
    String mcf =
        "    Node: node_0\n"
            + "    typeOf: dcs:City\n"
            + "    wikidataId: someWikidataId\n"
            + "    mid: someMid\n";
    assertEquals("wikidataId/someWikidataId", DcidGenerator.forPlace(string2Node(mcf)).dcid);

    mcf =
        "    Node: node_1\n"
            + "    typeOf: dcs:City\n"
            + "    geoNamesId: someGeoNamesId\n"
            + "    mid: someMid\n";
    assertEquals("geoNamesId/someGeoNamesId", DcidGenerator.forPlace(string2Node(mcf)).dcid);

    mcf = "    Node: node_2\n" + "    typeOf: dcs:City\n" + "    istatId: someIstatId\n";
    assertEquals("istatId/someIstatId", DcidGenerator.forPlace(string2Node(mcf)).dcid);
  }

  // NOTE: This is not supported yet, because of lack of farmhash32 support.
  @Test(expected = UnsupportedOperationException.class)
  public void funcRandomDcidShort() throws IOException {
    DcidGenerator.TEST_MODE = true;
    DcidGenerator.getRandomDcid("Place");
  }

  // NOTE: This is not supported yet, because of lack of farmhash32 support.
  @Test
  public void funcRandomDcidLong() throws IOException {
    DcidGenerator.TEST_MODE = true;
    assertEquals("dc/bsxxcsbwvey61", DcidGenerator.getRandomDcid("NewType"));
  }

  @Test
  public void funcStatVar() throws IOException {
    String mcf =
        "    Node: SomeNode\n"
            + "    typeOf: dcs:StatisticalVariable\n"
            + "    measuredProperty: dcs:count\n"
            + "    popType: dcid:Person\n"
            + "    race: dcid:Asian\n"
            + "    gender: dcid:Male\n"
            + "    statType: dcs:measuredValue\n"
            + "    dcid: \"manuallyCuratedDcid\"\n"
            + "    isPublic: \"true\"\n"
            + "    provenance: \"Jupiter\"\n"
            + "    keyString: \"k1=v1k2=v2\"\n"
            + "    resMCFFile: \"/path/to/the/black/hole\"\n"
            + "    label: \"SomeNode\"\n"
            + "    name: \"SomeNode\"\n"
            + "    description: \"Description of SomeNode\"\n"
            + "    descriptionUrl: \"http://wikipedia.org\"\n"
            + "    alternateName: \"SomeNode\"\n"
            + "    censusACSTableId: \"B01003\"\n"
            + "    memberOf: dcid:dc/g/FooSVG\n";
    DcidGenerator.Result result = DcidGenerator.forStatVar("SomeNode", string2Node(mcf));
    assertEquals(
        "gender=MalemeasuredProperty=countpopType="
            + "Personrace=AsianstatType=measuredValuetypeOf=StatisticalVariable",
        result.keyString);
    assertEquals("dc/g9y3wr60lwcg", result.dcid);
  }

  @Test
  public void funcStatVarWithComplexValue() throws IOException {
    String mcf0 =
        "    Node: unresolved\n"
            + "    typeOf: dcs:StatisticalVariable\n"
            + "    measuredProperty: dcs:count\n"
            + "    popType: dcid:Person\n"
            + "    age: [Years 16 -]\n";
    String mcf1 =
        "    Node: resolved\n"
            + "    typeOf: dcs:StatisticalVariable\n"
            + "    measuredProperty: dcs:count\n"
            + "    popType: dcid:Person\n"
            + "    age: dcid:Years16Onwards\n";
    DcidGenerator.Result result0 = DcidGenerator.forStatVar("unresolved", string2Node(mcf0));
    DcidGenerator.Result result1 = DcidGenerator.forStatVar("resolved", string2Node(mcf1));
    assertEquals(
        "age=Years16OnwardsmeasuredProperty=countpopType=PersontypeOf=" + "StatisticalVariable",
        result0.keyString);
    assertEquals("dc/2v3xz3y0dbd8b", result0.dcid);
    assertEquals(result0.dcid, result1.dcid);
  }

  private Mcf.McfGraph.PropertyValues string2Node(String mcf) throws IOException {
    for (var pv : TestUtil.graphFromMcf(mcf).getNodesMap().entrySet()) {
      return pv.getValue();
    }
    return Mcf.McfGraph.PropertyValues.newBuilder().build();
  }
}
