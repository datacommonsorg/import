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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.datacommons.proto.Mcf;
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
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

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
            + "unitOfMeasure: dcid:Year\n"
            + "value: 18\n"
            + "\n"
            + "Node: dcid:Count_Person_18Years_1000To2000USD\n"
            + "age: dcid:Year18\n"
            + "bogusProp: dcid:latLong/3738848_-12208344\n"
            + "constraintProperties: dcid:age, dcid:bogusProp, dcid:income\n"
            + "dcid: \"Count_Person_18Years_1000To2000USD\"\n"
            + "definition: \"mp=count,pt=Person,age=Year18,bogusProp=latLong/3738848_-12208344,income=USDollar1000To2000\"\n"
            + "income: dcid:USDollar1000To2000\n"
            + "measuredProperty: dcid:count\n"
            + "name: \"Population: Year 18, Lat Long/3738848 -12208344, US Dollar 1000 To 2000\"\n"
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
    assertEquals(McfUtil.serializeMcfGraph(got, true), want);
  }

  @Test
  public void testLegacyObsValue() throws IOException {
    String mcf =
        "Node: LegacyObs\n"
            + "typeOf: schema:Observation\n"
            + "observedNode: dcid:country/USA\n"
            + "measuredValue: \"1000,0000.0%\"\n"
            + "observationDate: \"2009\"\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);
    String want =
        "Node: LegacyObs\n"
            + "measuredValue: \"10000000.0\"\n"
            + "observationDate: \"2009\"\n"
            + "observedNode: dcid:country/USA\n"
            + "typeOf: dcid:Observation\n"
            + "\n";
    assertEquals(McfUtil.serializeMcfGraph(got, true), want);
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
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);
    assertEquals(McfUtil.serializeMcfGraph(got, true), mcf);
  }

  @Test
  public void testMultiEntityProperties() throws IOException {
    String mcf =
        "Node: dcid:FinancialAid\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: dcs:FinancialTransaction\n"
            + "measuredProperty: dcs:amount\n"
            + "observationProperties: dcs:destinationCountry\n"
            + "someActualConstraint: dcs:someValue\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:FinancialAid\n"
            + "constraintProperties: dcid:someActualConstraint\n"
            + "dcid: \"FinancialAid\"\n"
            + "definition: \"mp=amount,pt=FinancialTransaction,someActualConstraint=someValue\"\n"
            + "measuredProperty: dcid:amount\n"
            + "name: \"Amount Of Financial Transaction: Some Value\"\n"
            + "observationProperties: dcid:destinationCountry\n"
            + "populationType: dcid:FinancialTransaction\n"
            + "someActualConstraint: dcid:someValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(McfUtil.serializeMcfGraph(got, true), want);
  }

  @Test
  public void testStatVarDefinitionSkippedWhenNotBaseDc() throws IOException {
    String mcf =
        "Node: dcid:FinancialAid\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: dcs:FinancialTransaction\n"
            + "measuredProperty: dcs:amount\n"
            + "observationProperties: dcs:destinationCountry\n"
            + "someActualConstraint: dcs:someValue\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ false,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:FinancialAid\n"
            + "constraintProperties: dcid:someActualConstraint\n"
            + "dcid: \"FinancialAid\"\n"
            + "measuredProperty: dcid:amount\n"
            + "name: \"Amount Of Financial Transaction: Some Value\"\n"
            + "observationProperties: dcid:destinationCountry\n"
            + "populationType: dcid:FinancialTransaction\n"
            + "someActualConstraint: dcid:someValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(McfUtil.serializeMcfGraph(got, true), want);
  }

  @Test
  public void testStatVarExplicitDefinitionPreservedWhenNotBaseDc() throws IOException {
    String mcf =
        "Node: dcid:FinancialAid\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: dcs:FinancialTransaction\n"
            + "measuredProperty: dcs:amount\n"
            + "definition: \"custom_definition_string\"\n"
            + "someActualConstraint: dcs:someValue\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ false,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:FinancialAid\n"
            + "constraintProperties: dcid:someActualConstraint\n"
            + "dcid: \"FinancialAid\"\n"
            + "definition: \"custom_definition_string\"\n"
            + "measuredProperty: dcid:amount\n"
            + "name: \"Amount Of Financial Transaction: Some Value\"\n"
            + "populationType: dcid:FinancialTransaction\n"
            + "someActualConstraint: dcid:someValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(McfUtil.serializeMcfGraph(got, true), want);
  }

  // Example from the bug report: an explicitly declared constraintProperties list must not be
  // overwritten by every property outside NON_CONSTRAINT_STAT_VAR_PROPERTIES.
  @Test
  public void testDeclaredConstraintPropertiesPreserved() throws IOException {
    String mcf =
        "Node: dcid:Example_StatVar\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "populationType: dcid:EconomicProduction\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "statType: dcid:measuredValue\n"
            + "constraintProperties: dcid:productTaxBasis\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n";
    LogWrapper logCtx = TestUtil.newLogCtx();
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            logCtx,
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    // constraintProperties, definition and name only reflect the declared constraint.
    String want =
        "Node: dcid:Example_StatVar\n"
            + "constraintProperties: dcid:productTaxBasis\n"
            + "dcid: \"Example_StatVar\"\n"
            + "definition: \"mp=valueAdded,pt=EconomicProduction,productTaxBasis=IncludingProductTaxes\"\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "name: \"Value Added Of Economic Production: Including Product Taxes\"\n"
            + "populationType: dcid:EconomicProduction\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n"
            + "statType: dcid:measuredValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
    assertEquals(
        -1, TestUtil.getCounter(logCtx.getLog(), "Mutator_MissingConstraintPropertyValue"));
  }

  // Same node as above but without a declaration: the list is still derived from all
  // non-excluded properties (existing behavior).
  @Test
  public void testConstraintPropertiesDerivedWhenNotDeclared() throws IOException {
    String mcf =
        "Node: dcid:Example_StatVar\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "populationType: dcid:EconomicProduction\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "statType: dcid:measuredValue\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ false,
            /* generateFallbacks= */ false);

    String want =
        "Node: dcid:Example_StatVar\n"
            + "constraintProperties: dcid:productTaxBasis, dcid:sourceIndicatorCode\n"
            + "dcid: \"Example_StatVar\"\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "populationType: dcid:EconomicProduction\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n"
            + "statType: dcid:measuredValue\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }

  // Example from the review discussion, with multiple constraint and non-constraint props and
  // mixed namespace prefixes. (MCF serialization sorts values, so the output order is sorted.)
  @Test
  public void testMultipleDeclaredConstraintPropertiesPreserved() throws IOException {
    String mcf =
        "Node: dcid:MyVariable\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "constraintProperties: dcs:myConstraintB, schema:myConstraintA\n"
            + "myConstraintA: dcs:value1\n"
            + "myConstraintB: dcs:value2\n"
            + "myNonConstraintProperty: dcs:value3\n"
            + "otherMetadata: \"some text\"\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:MyVariable\n"
            + "constraintProperties: dcid:myConstraintA, dcid:myConstraintB\n"
            + "dcid: \"MyVariable\"\n"
            + "definition: \"mp=count,pt=Person,myConstraintA=value1,myConstraintB=value2\"\n"
            + "measuredProperty: dcid:count\n"
            + "myConstraintA: dcid:value1\n"
            + "myConstraintB: dcid:value2\n"
            + "myNonConstraintProperty: dcid:value3\n"
            + "name: \"Population: Value 1, Value 2\"\n"
            + "otherMetadata: \"some text\"\n"
            + "populationType: dcid:Person\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }

  // A declaration that matches what would be derived is left as-is.
  @Test
  public void testDeclaredConstraintPropertiesMatchingDerived() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_Female\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "constraintProperties: dcs:gender\n"
            + "gender: schema:Female\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ false,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:Count_Person_Female\n"
            + "constraintProperties: dcid:gender\n"
            + "dcid: \"Count_Person_Female\"\n"
            + "gender: dcid:Female\n"
            + "measuredProperty: dcid:count\n"
            + "name: \"Population: Female\"\n"
            + "populationType: dcid:Person\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }

  // A declared constraint property without a value on the node is removed from
  // constraintProperties on the node (so downstream stages like SVG hierarchy generation don't
  // fail), a warning is logged, and it is excluded from the generated name/definition.
  @Test
  public void testDeclaredConstraintPropertyMissingValue() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_Female\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "constraintProperties: dcs:gender, dcs:age\n"
            + "gender: schema:Female\n"
            + "sourceIndicatorCode: \"XYZ\"\n";
    LogWrapper logCtx = TestUtil.newLogCtx();
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            logCtx,
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:Count_Person_Female\n"
            + "constraintProperties: dcid:gender\n"
            + "dcid: \"Count_Person_Female\"\n"
            + "definition: \"mp=count,pt=Person,gender=Female\"\n"
            + "gender: dcid:Female\n"
            + "measuredProperty: dcid:count\n"
            + "name: \"Population: Female\"\n"
            + "populationType: dcid:Person\n"
            + "sourceIndicatorCode: \"XYZ\"\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
    assertTrue(
        TestUtil.checkLog(
            logCtx.getLog(),
            "Mutator_MissingConstraintPropertyValue",
            "property: 'age', node: 'dcid:Count_Person_Female'"));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), "Mutator_MissingConstraintPropertyValue"));
  }

  // Duplicate entries in constraintProperties in MCF input are collapsed on the node and in the
  // generated name/definition.
  @Test
  public void testDuplicateDeclaredConstraintProperties() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_Female_18Years\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "constraintProperties: dcid:age, dcid:gender, dcid:age\n"
            + "age: dcid:Years18\n"
            + "gender: schema:Female\n"
            + "sourceIndicatorCode: \"XYZ\"\n";
    Mcf.McfGraph gotFromMcf =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:Count_Person_Female_18Years\n"
            + "age: dcid:Years18\n"
            + "constraintProperties: dcid:age, dcid:gender\n"
            + "dcid: \"Count_Person_Female_18Years\"\n"
            + "definition: \"mp=count,pt=Person,age=Years18,gender=Female\"\n"
            + "gender: dcid:Female\n"
            + "measuredProperty: dcid:count\n"
            + "name: \"Population: Years 18, Female\"\n"
            + "populationType: dcid:Person\n"
            + "sourceIndicatorCode: \"XYZ\"\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(gotFromMcf, true));
  }

  // Duplicate entries injected directly into the proto (with mixed namespace prefixes, bypassing
  // McfParser deduplication) are also collapsed on the node and in the generated name/definition.
  @Test
  public void testDuplicateDeclaredConstraintPropertiesInProto() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_Female_18Years\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "age: dcid:Years18\n"
            + "gender: schema:Female\n"
            + "sourceIndicatorCode: \"XYZ\"\n";
    Mcf.McfGraph.Builder graph = TestUtil.graphFromMcf(mcf).toBuilder();
    Mcf.McfGraph.Values declaredWithDups =
        Mcf.McfGraph.Values.newBuilder()
            .addTypedValues(
                Mcf.McfGraph.TypedValue.newBuilder()
                    .setValue("dcid:age")
                    .setType(Mcf.ValueType.RESOLVED_REF))
            .addTypedValues(
                Mcf.McfGraph.TypedValue.newBuilder()
                    .setValue("dcid:gender")
                    .setType(Mcf.ValueType.RESOLVED_REF))
            .addTypedValues(
                Mcf.McfGraph.TypedValue.newBuilder()
                    .setValue("age")
                    .setType(Mcf.ValueType.RESOLVED_REF))
            .build();
    graph.putNodes(
        "dcid:Count_Person_Female_18Years",
        graph.getNodesOrThrow("dcid:Count_Person_Female_18Years").toBuilder()
            .putPvs(Vocabulary.CONSTRAINT_PROPS, declaredWithDups)
            .build());

    Mcf.McfGraph gotFromProto =
        McfMutator.mutate(
            graph, TestUtil.newLogCtx(), /* isBaseDc= */ true, /* generateFallbacks= */ true);

    Mcf.McfGraph.PropertyValues node =
        gotFromProto.getNodesOrThrow("dcid:Count_Person_Female_18Years");
    assertEquals(List.of("age", "gender"), McfUtil.getPropVals(node, Vocabulary.CONSTRAINT_PROPS));
    assertEquals(
        "mp=count,pt=Person,age=Years18,gender=Female",
        McfUtil.getPropVal(node, Vocabulary.DEFINITION));
    assertEquals("Population: Years 18, Female", McfUtil.getPropVal(node, Vocabulary.NAME));
  }

  // Non-constraint properties (e.g. populationType, measuredProperty) mistakenly listed in
  // constraintProperties are stripped from constraintProperties with a warning, while still
  // contributing normally as core properties to the generated name and definition.
  @Test
  public void testDeclaredNonConstraintPropertyFilteredAndCorePropsPreservedInName()
      throws IOException {
    String mcf =
        "Node: dcid:Median_Income_Person_Female\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "statType: dcs:medianValue\n"
            + "measuredProperty: dcs:income\n"
            + "populationType: schema:Person\n"
            + "measurementQualifier: dcs:Annual, dcs:Nominal\n"
            + "measurementDenominator: dcs:Count_Person\n"
            + "constraintProperties: dcs:populationType, dcs:measuredProperty, dcs:gender\n"
            + "gender: schema:Female\n"
            + "sourceIndicatorCode: \"INC_F\"\n";
    LogWrapper logCtx = TestUtil.newLogCtx();
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            logCtx,
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    Mcf.McfGraph.PropertyValues node = got.getNodesOrThrow("dcid:Median_Income_Person_Female");
    assertEquals(List.of("gender"), McfUtil.getPropVals(node, Vocabulary.CONSTRAINT_PROPS));
    assertEquals(
        "Median Annual Income Of Person (Nominal): Female (Per Capita)",
        McfUtil.getPropVal(node, Vocabulary.NAME));
    assertEquals(
        "md=Count_Person,mq=Annual,st=medianValue,mp=income,pt=Person,gender=Female",
        McfUtil.getPropVal(node, Vocabulary.DEFINITION));
    assertEquals(2, TestUtil.getCounter(logCtx.getLog(), "Mutator_InvalidConstraintProperty"));
  }

  // An explicit name and definition are still preserved alongside a declared constraint list.
  @Test
  public void testDeclaredConstraintPropertiesWithExplicitNameAndDefinition() throws IOException {
    String mcf =
        "Node: dcid:Example_StatVar\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "populationType: dcid:EconomicProduction\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "constraintProperties: dcid:productTaxBasis\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n"
            + "name: \"Custom Name\"\n"
            + "definition: \"custom_definition\"\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:Example_StatVar\n"
            + "constraintProperties: dcid:productTaxBasis\n"
            + "dcid: \"Example_StatVar\"\n"
            + "definition: \"custom_definition\"\n"
            + "measuredProperty: dcid:valueAdded\n"
            + "name: \"Custom Name\"\n"
            + "populationType: dcid:EconomicProduction\n"
            + "productTaxBasis: dcid:IncludingProductTaxes\n"
            + "sourceIndicatorCode: \"NGDP\"\n"
            + "typeOf: dcid:StatisticalVariable\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }

  // A constraintProperties entry with no values is treated as "not declared", so the list is
  // derived as before.
  @Test
  public void testEmptyDeclaredConstraintPropertiesFallsBackToDerived() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_Female\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: dcs:count\n"
            + "gender: schema:Female\n"
            + "sourceIndicatorCode: \"XYZ\"\n";
    Mcf.McfGraph.Builder graph = TestUtil.graphFromMcf(mcf).toBuilder();
    Mcf.McfGraph.PropertyValues node =
        graph.getNodesOrThrow("dcid:Count_Person_Female").toBuilder()
            .putPvs(Vocabulary.CONSTRAINT_PROPS, Mcf.McfGraph.Values.getDefaultInstance())
            .build();
    graph.putNodes("dcid:Count_Person_Female", node);

    Mcf.McfGraph got =
        McfMutator.mutate(
            graph, TestUtil.newLogCtx(), /* isBaseDc= */ false, /* generateFallbacks= */ false);

    assertEquals(
        List.of("gender", "sourceIndicatorCode"),
        McfUtil.getPropVals(
            got.getNodesOrThrow("dcid:Count_Person_Female"), Vocabulary.CONSTRAINT_PROPS));
  }

  // ComplexValue expansion works alongside an explicitly declared constraintProperties list.
  @Test
  public void testDeclaredConstraintPropertiesWithComplexValue() throws IOException {
    String mcf =
        "Node: dcid:Count_Person_18Years\n"
            + "typeOf: schema:StatisticalVariable\n"
            + "populationType: schema:Person\n"
            + "measuredProperty: schema:count\n"
            + "statType: dcs:measuredValue\n"
            + "constraintProperties: dcs:age\n"
            + "age: [dcs:Year 18]\n"
            + "sourceIndicatorCode: \"POP_18\"\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: Year18\n"
            + "dcid: \"Year18\"\n"
            + "name: \"Year 18\"\n"
            + "typeOf: dcid:Quantity\n"
            + "unitOfMeasure: dcid:Year\n"
            + "value: 18\n"
            + "\n"
            + "Node: dcid:Count_Person_18Years\n"
            + "age: dcid:Year18\n"
            + "constraintProperties: dcid:age\n"
            + "dcid: \"Count_Person_18Years\"\n"
            + "definition: \"mp=count,pt=Person,age=Year18\"\n"
            + "measuredProperty: dcid:count\n"
            + "name: \"Population: Year 18\"\n"
            + "populationType: dcid:Person\n"
            + "sourceIndicatorCode: \"POP_18\"\n"
            + "statType: dcid:measuredValue\n"
            + "typeOf: dcid:StatisticalVariable\n\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }

  // Non-StatVar nodes are not affected by constraintProperties handling.
  @Test
  public void testConstraintPropertiesOnNonStatVarUntouched() throws IOException {
    String mcf =
        "Node: dcid:SomeThing\n"
            + "typeOf: dcs:Thing\n"
            + "constraintProperties: dcs:gender\n"
            + "gender: schema:Female\n"
            + "otherProp: schema:Value\n";
    Mcf.McfGraph got =
        McfMutator.mutate(
            TestUtil.graphFromMcf(mcf).toBuilder(),
            TestUtil.newLogCtx(),
            /* isBaseDc= */ true,
            /* generateFallbacks= */ true);

    String want =
        "Node: dcid:SomeThing\n"
            + "constraintProperties: dcid:gender\n"
            + "dcid: \"SomeThing\"\n"
            + "gender: dcid:Female\n"
            + "otherProp: dcid:Value\n"
            + "typeOf: dcid:Thing\n"
            + "\n";
    assertEquals(want, McfUtil.serializeMcfGraph(got, true));
  }
}
