package org.datacommons;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

// Models a statvar observation.
@DefaultCoder(AvroCoder.class)
public class Observation {
  String variableMeasured;
  String observationAbout;
  List<String> observations;
  @Nullable String observationPeriod;
  @Nullable String measurementMethod;
  @Nullable String scalingFactor;
  @Nullable String unit;
  String provenance;
  String provenance_url;
  String import_name;

  public Observation(
      String variableMeasured,
      String observationAbout,
      List<String> observations,
      String observationPeriod,
      String measurementMethod,
      String scalingFactor,
      String unit,
      String provenance,
      String provenance_url,
      String import_name) {
    this.variableMeasured = variableMeasured;
    this.observationAbout = observationAbout;
    this.observations = observations;
    this.observationPeriod = observationPeriod;
    this.measurementMethod = measurementMethod;
    this.scalingFactor = scalingFactor;
    this.unit = unit;
    this.provenance = provenance;
    this.provenance_url = provenance_url;
    this.import_name = import_name;
  }

  public Mutation toMutation() {
    return Mutation.newInsertOrUpdateBuilder("Observation")
        .set("variable_measured")
        .to(variableMeasured)
        .set("observation_about")
        .to(observationAbout)
        .set("provenance")
        .to(provenance)
        .set("observation_period")
        .to(observationPeriod)
        .set("measurement_method")
        .to(measurementMethod)
        .set("unit")
        .to(unit)
        .set("scaling_factor")
        .to(scalingFactor)
        .set("observations")
        .to(Value.jsonArray(observations))
        .set("import_name")
        .to(import_name)
        .set("provenance_url")
        .to(provenance_url)
        .build();
  }

  Observation() {}
  }