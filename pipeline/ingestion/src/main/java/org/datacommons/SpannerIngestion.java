package org.datacommons;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.spanner.Dialect;
import com.google.cloud.spanner.Mutation;

import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead.Method;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

public class SpannerIngestion {

  @DefaultCoder(AvroCoder.class)
  static class Observation{
    String id;
    @Nullable String provId;
    @Nullable String variableMeasured;
    @Nullable String measurementMethod;
    @Nullable String scalingFactor;
    @Nullable String unit;
    @Nullable String observationPeriod;
    @Nullable String observationDate;
    @Nullable String observationAbout;
    @Nullable String value;


 public Observation(java.lang.String id, java.lang.String measurementMethod, java.lang.String scalingFactor,
    java.lang.String unit, java.lang.String observationPeriod, java.lang.String observationDate,
    java.lang.String observationAbout, java.lang.String value) {
  this.id = id;
  this.measurementMethod = measurementMethod;
  this.scalingFactor = scalingFactor;
  this.unit = unit;
  this.observationPeriod = observationPeriod;
  this.observationDate = observationDate;
  this.observationAbout = observationAbout;
  this.value = value;
}


static Observation fromTableRow(TableRow row) {
  Observation observation = new Observation();
  if (row.get("id") != null) {
    observation.id = (String) row.get("id");
  }
  if (row.get("prov_id") != null) {
    observation.provId = (String) row.get("prov_id");
  }
  if (row.get("variable_measured") != null) {
    observation.variableMeasured = (String) row.get("variable_measured");
  }
  if (row.get("measurement_method") != null) {
    observation.measurementMethod = (String) row.get("measurement_method");
  }
  if (row.get("scaling_factor") != null) {
    observation.scalingFactor = (String) row.get("scaling_factor");
  }
  if (row.get("unit") != null) {
    observation.unit = (String) row.get("unit");
  }
  if (row.get("observation_period") != null) {
    observation.observationPeriod = (String) row.get("observation_period");
  }
  if (row.get("observation_date") != null) {
    observation.observationDate = (String) row.get("observation_date");
  }
  if (row.get("observation_about") != null) {
    observation.observationAbout = (String) row.get("observation_about");
  }
  if (row.get("value") != null) {
    observation.value = (String) row.get("value");
  }
  return observation;
}
Observation(){}

  }

  @DefaultCoder(AvroCoder.class)
  static class Triple {
    String id;
    @Nullable
    String provId;
    @Nullable
    String subjectId;
    @Nullable
    String predicate;
    @Nullable
    String objectId;

    public Triple(String id, String provId, String subjectId, String predicate, String objectId, String objectValue) {
      this.id = id;
      this.provId = provId;
      this.subjectId = subjectId;
      this.predicate = predicate;
      this.objectId = objectId;
    }

    Triple() {}

    static Triple fromTableRow(TableRow row) {
      Triple triple = new Triple();
      if (row.get("id") != null) { triple.id = (String) row.get("id");} 
      if (row.get("prov_id") != null) {triple.provId = (String) row.get("prov_id");}
      if (row.get("subject_id") != null) {triple.subjectId = (String) row.get("subject_id");}
      if (row.get("predicate") != null) {triple.predicate = (String) row.get("predicate");}
      if (row.get("object_id") != null) {triple.objectId = (String) row.get("object_id");}
      return triple;
    }
  }

  static PCollection<Observation> ObservationRead(Pipeline p) {

    String query = "SELECT * FROM `datcom-store.dc_kg_latest.StatVarObservation` LIMIT 100";
    PCollection<Observation> rows =
        p.apply(
                "Read from BigQuery table",
                BigQueryIO.readTableRows()
                    .from(String.format("%s:%s.%s", "datcom-store", "dc_kg_latest", "StatVarObservation"))
                    // .fromQuery(query)
                    // .usingStandardSql()
                    .withMethod(Method.DIRECT_READ))
            .apply(
                "TableRows to MyData",
                MapElements.into(TypeDescriptor.of(Observation.class)).via(Observation::fromTableRow));
    return rows;
  }

  static PCollection<Triple> TripeRead(Pipeline p) {

    String query = "SELECT * FROM `datcom-store.spanner.triple` LIMIT 100";
    PCollection<Triple> rows =
        p.apply(
                "Read from BigQuery table",
                BigQueryIO.readTableRows()
                    .from(String.format("%s:%s.%s", "datcom-store", "spanner", "triple"))
                    // .fromQuery(query)
                    // .usingStandardSql()
                    .withMethod(Method.DIRECT_READ))
            .apply(
                "TableRows to MyData",
                MapElements.into(TypeDescriptor.of(Triple.class)).via(Triple::fromTableRow));
    return rows;
  }


  public interface CustomOptions extends PipelineOptions {
  }

static void ObservationWrite(Pipeline p, PCollection<Observation> observations) {
    observations
        .apply(
            "CreateObservationMutation", // More descriptive name
            ParDo.of(
                new DoFn<Observation, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Observation o = c.element();
                    c.output(Mutation.newInsertOrUpdateBuilder("Observations")
                            .set("Id")
                            .to(o.id)
                            .set("ProvId")
                            .to(o.provId)
                            .set("VariableMeasured")
                            .to(o.variableMeasured)
                            .set("MeasurementMethod")
                            .to(o.measurementMethod)
                            .set("ScalingFactor")
                            .to(o.scalingFactor)
                            .set("Unit")
                            .to(o.unit)
                            .set("ObservationPeriod")
                            .to(o.observationPeriod)
                            .set("ObservationDate")
                            .to(o.observationDate)
                            .set("ObservationAbout")
                            .to(o.observationAbout)
                            .set("Value")
                            .to(o.value)
                            .build());

                  }
                }))
        .apply(
            "WriteObservationsToSpanner", // More descriptive name
            SpannerIO.write()
                .withProjectId("datcom-store") // Replace with your Project ID
                .withInstanceId("dc-kg-test") // Replace with your Instance ID
                .withDatabaseId("dc_graph") // Replace with your Database ID
                .withDialectView(
                    p.apply(Create.of(Dialect.GOOGLE_STANDARD_SQL)).apply(View.asSingleton())));
  }



  static void TripleWrite(Pipeline p, PCollection<Triple> triples) {

   triples 
        .apply(
            "CreateMutation",
            ParDo.of(
                new DoFn<Triple, Mutation>() {
                  @ProcessElement
                  public void processElement(ProcessContext c) {
                    Triple t = c.element();
                    c.output(
                        Mutation.newInsertOrUpdateBuilder("Triples")
                            .set("Id")
                            .to(t.id)
                            .set("ProvId")
                            .to(t.provId)
                            .set("SubjecTId")
                            .to(t.subjectId)
                            .set("Predicate")
                            .to(t.predicate)
                            .set("ObjectId")
                            .to(t.objectId)
                            .build());
                  }
                }))
        .apply(
            "WriteTriples",
            SpannerIO.write()
                // .withProjectId("vishg-dev")
                // .withInstanceId("spanner-test")
                // .withDatabaseId("test-db")
                .withProjectId("datcom-store")
                .withInstanceId("dc-kg-test")
                .withDatabaseId("dc_graph")
                .withDialectView(
                    p.apply(Create.of(Dialect.GOOGLE_STANDARD_SQL)).apply(View.asSingleton())));

    //  PCollection<Struct> records = p.apply(
    // SpannerIO.read()
    //     .withInstanceId("test-instance")
    //     .withDatabaseId("test-db")
    //     .withQuery("SELECT * FROM Singers"));

  }

  public static void main(String[] args) {
    CustomOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(CustomOptions.class);
    Pipeline p = Pipeline.create(options);
    // PCollection<Triple> rows = TripeRead(p);
    // TripleWrite(p, rows);
    PCollection<Observation> rows = ObservationRead(p);
    ObservationWrite(p, rows);
    p.run().waitUntilFinish();
  }
}
