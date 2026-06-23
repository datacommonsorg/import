package org.datacommons.ingestion.util;

import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GraphTransformerTest {

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testQuantityTransformation() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "earthquake1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("EarthquakeEvent")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[31.61 Kilometer]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "provenance",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:prov1")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "earthquake1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("EarthquakeEvent")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Kilometer31.61")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "provenance",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:prov1")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .putNodes(
                "Kilometer31.61",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Quantity")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "value",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("31.61").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "unitOfMeasure",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Kilometer")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Kilometer31.61")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Kilometer 31.61")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "provenance",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:prov1")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testLatLongTransformation() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "location",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[LatLong 33.1575 -116.0413333]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "location",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("latLong/3315750_-11604133")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .putNodes(
                "latLong/3315750_-11604133",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("GeoCoordinates")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "latitude",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("33.1575").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "longitude",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("-116.0413333")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("33.1575,-116.0413333")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("latLong/3315750_-11604133")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testMixedTransformation() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[10 Meter]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "location",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[LatLong 10.0 20.0]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Meter10")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "location",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("latLong/1000000_2000000")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .putNodes(
                "Meter10",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Quantity")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "value",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("10").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "unitOfMeasure",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Meter")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("Meter10").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Meter 10")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .putNodes(
                "latLong/1000000_2000000",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("GeoCoordinates")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "latitude",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("10.0").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "longitude",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("20.0").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("10.0,20.0")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("latLong/1000000_2000000")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testStatVarTransformation() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "NYTCovid19CumulativeCases",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:MedicalConditionIncident")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:cumulativeCount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "incidentType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:COVID_19")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "medicalStatus",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:ConfirmedCase")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "NYTCovid19CumulativeCases",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:MedicalConditionIncident")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:cumulativeCount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "incidentType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:COVID_19")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "medicalStatus",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:ConfirmedCase")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "constraintProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("incidentType")
                                    .setType(ValueType.RESOLVED_REF))
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("medicalStatus")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testStatVarTransformationWithExistingConstraintProperties() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "NYTCovid19CumulativeCases",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:MedicalConditionIncident")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:cumulativeCount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "incidentType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:COVID_19")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "medicalStatus",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:ConfirmedCase")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "constraintProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:incidentType")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "NYTCovid19CumulativeCases",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:MedicalConditionIncident")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:cumulativeCount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "incidentType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:COVID_19")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "medicalStatus",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:ConfirmedCase")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "constraintProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("incidentType")
                                    .setType(ValueType.RESOLVED_REF))
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("medicalStatus")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testStatVarTransformationWithObservationPropertiesAndEntityMapping() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "FinancialAid",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:FinancialTransaction")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:amount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "observationProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:destinationCountry")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "entityMapping",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:someMapping")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "someActualConstraint",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:someValue")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "FinancialAid",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:StatisticalVariable")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "populationType",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:FinancialTransaction")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "measuredProperty",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:amount")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "observationProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:destinationCountry")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "entityMapping",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:someMapping")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "someActualConstraint",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:someValue")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "constraintProperties",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("someActualConstraint")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testQuantityTransformationNoOverwrite() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[10 Meter]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .putNodes(
                "Meter10",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Quantity")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "value",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("10").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "unitOfMeasure",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Meter")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Meter10")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Curated Name for 10 Meter")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "depth",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Meter10")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .putNodes(
                "Meter10",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Quantity")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "value",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("10").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "unitOfMeasure",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Meter")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("dcid:Meter10")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Curated Name for 10 Meter")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }

  @Test
  public void testNegativeQuantityTransformation() {
    McfGraph inputGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "temperature",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("[-5.5 DegreeCelsius]")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    McfGraph expectedGraph =
        McfGraph.newBuilder()
            .putNodes(
                "node1",
                PropertyValues.newBuilder()
                    .putPvs(
                        "temperature",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("DegreeCelsius-5.5")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .build())
            .putNodes(
                "DegreeCelsius-5.5",
                PropertyValues.newBuilder()
                    .putPvs(
                        "typeOf",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("Quantity")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "value",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder().setValue("-5.5").setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "unitOfMeasure",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("DegreeCelsius")
                                    .setType(ValueType.RESOLVED_REF))
                            .build())
                    .putPvs(
                        "dcid",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("DegreeCelsius-5.5")
                                    .setType(ValueType.TEXT))
                            .build())
                    .putPvs(
                        "name",
                        Values.newBuilder()
                            .addTypedValues(
                                TypedValue.newBuilder()
                                    .setValue("DegreeCelsius -5.5")
                                    .setType(ValueType.TEXT))
                            .build())
                    .build())
            .build();

    PCollection<McfGraph> output =
        p.apply(Create.of(inputGraph)).apply(ParDo.of(new GraphTransformer()));

    PCollection<McfGraph> mergedOutput =
        output.apply(
            org.apache.beam.sdk.transforms.Combine.globally(
                    new PipelineUtilsTest.MergeMcfGraphsCombineFn())
                .withoutDefaults());

    PAssert.that(mergedOutput).containsInAnyOrder(expectedGraph);
    p.run().waitUntilFinish();
  }
}
