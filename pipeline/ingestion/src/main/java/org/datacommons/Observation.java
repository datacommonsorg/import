package org.datacommons;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Models a statvar observation. */
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((variableMeasured == null) ? 0 : variableMeasured.hashCode());
    result = prime * result + ((observationAbout == null) ? 0 : observationAbout.hashCode());
    result = prime * result + ((observations == null) ? 0 : observations.hashCode());
    result = prime * result + ((observationPeriod == null) ? 0 : observationPeriod.hashCode());
    result = prime * result + ((measurementMethod == null) ? 0 : measurementMethod.hashCode());
    result = prime * result + ((scalingFactor == null) ? 0 : scalingFactor.hashCode());
    result = prime * result + ((unit == null) ? 0 : unit.hashCode());
    result = prime * result + ((provenance == null) ? 0 : provenance.hashCode());
    result = prime * result + ((provenance_url == null) ? 0 : provenance_url.hashCode());
    result = prime * result + ((import_name == null) ? 0 : import_name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null) return false;
    if (getClass() != obj.getClass()) return false;
    Observation other = (Observation) obj;
    if (variableMeasured == null) {
      if (other.variableMeasured != null) return false;
    } else if (!variableMeasured.equals(other.variableMeasured)) return false;
    if (observationAbout == null) {
      if (other.observationAbout != null) return false;
    } else if (!observationAbout.equals(other.observationAbout)) return false;
    if (observations == null) {
      if (other.observations != null) return false;
    } else if (!observations.equals(other.observations)) return false;
    if (observationPeriod == null) {
      if (other.observationPeriod != null) return false;
    } else if (!observationPeriod.equals(other.observationPeriod)) return false;
    if (measurementMethod == null) {
      if (other.measurementMethod != null) return false;
    } else if (!measurementMethod.equals(other.measurementMethod)) return false;
    if (scalingFactor == null) {
      if (other.scalingFactor != null) return false;
    } else if (!scalingFactor.equals(other.scalingFactor)) return false;
    if (unit == null) {
      if (other.unit != null) return false;
    } else if (!unit.equals(other.unit)) return false;
    if (provenance == null) {
      if (other.provenance != null) return false;
    } else if (!provenance.equals(other.provenance)) return false;
    if (provenance_url == null) {
      if (other.provenance_url != null) return false;
    } else if (!provenance_url.equals(other.provenance_url)) return false;
    if (import_name == null) {
      if (other.import_name != null) return false;
    } else if (!import_name.equals(other.import_name)) return false;
    return true;
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