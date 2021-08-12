package org.datacommons.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.junit.Test;

public class McfMutatorTest {
  @Test
  public void testComplex() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_18Years_1000To2000USD\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: schema:count\n"
            + "statType: dcs:measuredValue\n"
            + "age: [dcs:Year 18]\n"
            + "income: [dcs:USDollar 1000 2000]\n"
            + "bogusProp: [LatLong 37.3884812 -122.0834373]";
    McfMutator mutator =
        new McfMutator(TestUtil.graphFromMcf(mcf).toBuilder(), TestUtil.newLogCtx("InMemory"));

    String want =
        "Node: USDollar1000To2000\n"
            + "dcid: \"USDollar1000To2000\"\n"
            + "endValue: 2000\n"
            + "name: \"USDollar 1000 To 2000\"\n"
            + "startValue: 1000\n"
            + "typeOf: dcid:QuantityRange\n"
            + "unit: dcid:USDollar\n"
            + "\n"
            + "Node: Year18\n"
            + "dcid: \"Year18\"\n"
            + "name: \"Year 18\"\n"
            + "typeOf: dcid:Quantity\n"
            + "unit: dcid:Year\n"
            + "value: 18\n"
            + "\n"
            + "Node: dcid:Count_Person_18Years_1000To2000USD\n"
            + "age: dcid:Year18\n"
            + "bogusProp: dcid:latLong/3738848_-12208344\n"
            + "dcid: \"Count_Person_18Years_1000To2000USD\"\n"
            + "income: dcid:USDollar1000To2000\n"
            + "measuredProperty: dcid:count\n"
            + "populationType: dcid:Person\n"
            + "statType: dcid:measuredValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n"
            + "Node: latLong/3738848_-12208344\n"
            + "dcid: \"latLong/3738848_-12208344\"\n"
            + "latitude: \"37.3884812\"\n"
            + "longitude: \"-122.0834373\"\n"
            + "name: \"37.38848,-122.08344\"\n"
            + "typeOf: dcid:GeoCoordinates\n\n";
    assertEquals(McfUtil.serializeMcfGraph(mutator.apply(), true), want);
  }

  @Test
  public void testLegacyObsValue() throws IOException {
    String mcf =
        "Node: LegacyObs\n"
            + "typeOf: schema:Observation\n"
            + "observedNode: dcid:country/USA\n"
            + "measuredValue: \"1000,0000.0%\"\n"
            + "observationDate: \"2009\"\n";
    McfMutator mutator =
        new McfMutator(TestUtil.graphFromMcf(mcf).toBuilder(), TestUtil.newLogCtx("InMemory"));
    String want =
        "Node: LegacyObs\n"
            + "measuredValue: \"10000000.0\"\n"
            + "observationDate: 2009\n"
            + "observedNode: dcid:country/USA\n"
            + "typeOf: dcid:Observation\n"
            + "\n";
    assertEquals(McfUtil.serializeMcfGraph(mutator.apply(), true), want);
  }

  @Test
  public void testSVObsValue() throws IOException {
    String mcf =
        "Node: SVObs\n"
            + "observationAbout: dcid:country/USA\n"
            + "observationDate: 2009\n"
            + "typeOf: dcid:StatVarObservation\n"
            + "value: \"10000000.0%\"\n"
            + "variableMeasured: dcid:Count_Male_18Years_1000To2000USD\n"
            + "\n";
    McfMutator mutator =
        new McfMutator(TestUtil.graphFromMcf(mcf).toBuilder(), TestUtil.newLogCtx("InMemory"));
    assertEquals(McfUtil.serializeMcfGraph(mutator.apply(), true), mcf);
  }
}
