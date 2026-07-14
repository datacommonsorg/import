package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class StatVarNameGeneratorTest {

  @Test
  public void testFormatToken() {
    assertEquals("Cumulative Count", StatVarNameGenerator.formatToken("cumulativeCount"));
    assertEquals(
        "Medical Condition Incident", StatVarNameGenerator.formatToken("MedicalConditionIncident"));
    assertEquals("COVID 19", StatVarNameGenerator.formatToken("COVID_19"));
    assertEquals("Years 18 To 24", StatVarNameGenerator.formatToken("Years18To24"));
    assertEquals("Count Person", StatVarNameGenerator.formatToken("dcid:Count_Person"));
    assertEquals("", StatVarNameGenerator.formatToken(""));
  }

  @Test
  public void testIsStatVar() {
    assertTrue(StatVarNameGenerator.isStatVar(List.of("StatisticalVariable")));
    assertTrue(StatVarNameGenerator.isStatVar(List.of("Topic", "StatisticalVariable")));
    assertFalse(StatVarNameGenerator.isStatVar(List.of("Place")));
    assertFalse(StatVarNameGenerator.isStatVar(List.of()));
  }

  @Test
  public void testGenerateName_basicStatVar() {
    PropertyValues pvs =
        PropertyValues.newBuilder()
            .putPvs("typeOf", Values.newBuilder().addTypedValues(tv("StatisticalVariable")).build())
            .putPvs("statType", Values.newBuilder().addTypedValues(tv("growthRate")).build())
            .putPvs("measuredProperty", Values.newBuilder().addTypedValues(tv("count")).build())
            .putPvs("populationType", Values.newBuilder().addTypedValues(tv("Person")).build())
            .putPvs(
                "measurementQualifier", Values.newBuilder().addTypedValues(tv("Annual")).build())
            .build();

    assertEquals(
        "Growth Rate Annual Count Of Person", StatVarNameGenerator.generateName("sv1", pvs));
  }

  @Test
  public void testGenerateName_withConstraints() {
    PropertyValues pvs =
        PropertyValues.newBuilder()
            .putPvs("typeOf", Values.newBuilder().addTypedValues(tv("StatisticalVariable")).build())
            .putPvs(
                "measuredProperty",
                Values.newBuilder().addTypedValues(tv("cumulativeCount")).build())
            .putPvs(
                "populationType",
                Values.newBuilder().addTypedValues(tv("MedicalConditionIncident")).build())
            .putPvs("incidentType", Values.newBuilder().addTypedValues(tv("COVID_19")).build())
            .putPvs(
                "medicalStatus", Values.newBuilder().addTypedValues(tv("ConfirmedCase")).build())
            .build();

    assertEquals(
        "Cumulative Count Of Medical Condition Incident: COVID 19, Confirmed Case",
        StatVarNameGenerator.generateName("sv2", pvs));
  }

  @Test
  public void testGenerateName_withBooleanAndDenominator() {
    PropertyValues pvs =
        PropertyValues.newBuilder()
            .putPvs("typeOf", Values.newBuilder().addTypedValues(tv("StatisticalVariable")).build())
            .putPvs("measuredProperty", Values.newBuilder().addTypedValues(tv("count")).build())
            .putPvs("populationType", Values.newBuilder().addTypedValues(tv("Person")).build())
            .putPvs("isUrban", Values.newBuilder().addTypedValues(tv("true")).build())
            .putPvs(
                "measurementDenominator",
                Values.newBuilder().addTypedValues(tv("Count_Person")).build())
            .build();

    assertEquals(
        "Count Of Person: Is Urban (Per capita)", StatVarNameGenerator.generateName("sv3", pvs));
  }

  @Test
  public void testGenerateName_genericPopType() {
    PropertyValues pvs =
        PropertyValues.newBuilder()
            .putPvs("typeOf", Values.newBuilder().addTypedValues(tv("StatisticalVariable")).build())
            .putPvs("measuredProperty", Values.newBuilder().addTypedValues(tv("value")).build())
            .putPvs("populationType", Values.newBuilder().addTypedValues(tv("Thing")).build())
            .putPvs(
                "variableMeasured",
                Values.newBuilder().addTypedValues(tv("UnemploymentRate")).build())
            .build();

    assertEquals("Value: Unemployment Rate", StatVarNameGenerator.generateName("sv4", pvs));
  }

  @Test
  public void testGenerateName_popTypeContainsMeasure() {
    PropertyValues pvs =
        PropertyValues.newBuilder()
            .putPvs("typeOf", Values.newBuilder().addTypedValues(tv("StatisticalVariable")).build())
            .putPvs("measuredProperty", Values.newBuilder().addTypedValues(tv("count")).build())
            .putPvs(
                "populationType", Values.newBuilder().addTypedValues(tv("Count_Person")).build())
            .build();

    assertEquals("Count Person", StatVarNameGenerator.generateName("sv5", pvs));
  }

  private static TypedValue tv(String val) {
    return TypedValue.newBuilder().setValue(val).setType(ValueType.TEXT).build();
  }
}
