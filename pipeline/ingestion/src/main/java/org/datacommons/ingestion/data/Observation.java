package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.datacommons.proto.Storage.Observations;

/** Models a statvar observation time series. */
@DefaultCoder(AvroCoder.class)
public class Observation implements Serializable {

  private String variableMeasured;
  private String observationAbout;
  private Observations observations;
  private String observationPeriod;
  private String measurementMethod;
  private String unit;
  private String scalingFactor;
  private String importName;
  private String provenanceUrl;

  private Observation(Builder builder) {
    this.variableMeasured = builder.variableMeasured;
    this.observationAbout = builder.observationAbout;
    this.observations = builder.observations.build();
    this.observationPeriod = builder.observationPeriod;
    this.measurementMethod = builder.measurementMethod;
    this.unit = builder.unit;
    this.scalingFactor = builder.scalingFactor;
    this.importName = builder.importName;
    this.provenanceUrl = builder.provenanceUrl;
  }

  public static Builder builder() {
    return new Builder();
  }

  public String getVariableMeasured() {
    return variableMeasured;
  }

  public String getObservationAbout() {
    return observationAbout;
  }

  public Observations getObservations() {
    return observations;
  }

  public String getObservationPeriod() {
    return observationPeriod;
  }

  public String getMeasurementMethod() {
    return measurementMethod;
  }

  public String getUnit() {
    return unit;
  }

  public String getScalingFactor() {
    return scalingFactor;
  }

  public String getImportName() {
    return importName;
  }

  public String getProvenanceUrl() {
    return provenanceUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Observation that = (Observation) o;
    return Objects.equals(variableMeasured, that.variableMeasured)
        && Objects.equals(observationAbout, that.observationAbout)
        && Objects.equals(observations, that.observations)
        && Objects.equals(observationPeriod, that.observationPeriod)
        && Objects.equals(measurementMethod, that.measurementMethod)
        && Objects.equals(unit, that.unit)
        && Objects.equals(scalingFactor, that.scalingFactor)
        && Objects.equals(importName, that.importName)
        && Objects.equals(provenanceUrl, that.provenanceUrl);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        variableMeasured,
        observationAbout,
        observations,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        importName,
        provenanceUrl);
  }

  @Override
  public String toString() {
    return String.format(
        "Observation{"
            + "variableMeasured='%s', "
            + "observationAbout='%s', "
            + "observations=%s, "
            + "observationPeriod='%s', "
            + "measurementMethod='%s', "
            + "unit='%s', "
            + "scalingFactor='%s', "
            + "importName='%s', "
            + "provenanceUrl='%s'"
            + "}",
        variableMeasured,
        observationAbout,
        observations,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        importName,
        provenanceUrl);
  }

  // Builder for Observation
  public static class Builder {
    private String variableMeasured = "";
    private String observationAbout = "";
    private Observations.Builder observations = Observations.newBuilder();
    private String observationPeriod = "";
    private String measurementMethod = "";
    private String unit = "";
    private String scalingFactor = "";
    private String importName = "";
    private String provenanceUrl = "";

    public Builder variableMeasured(String variableMeasured) {
      this.variableMeasured = variableMeasured;
      return this;
    }

    public Builder observationAbout(String observationAbout) {
      this.observationAbout = observationAbout;
      return this;
    }

    public Builder observation(String date, String value) {
      this.observations.putValues(date, value);
      return this;
    }

    public Builder observations(Observations observations) {
      this.observations = observations.toBuilder();
      return this;
    }

    public Builder observationPeriod(String observationPeriod) {
      this.observationPeriod = observationPeriod;
      return this;
    }

    public Builder measurementMethod(String measurementMethod) {
      this.measurementMethod = measurementMethod;
      return this;
    }

    public Builder unit(String unit) {
      this.unit = unit;
      return this;
    }

    public Builder scalingFactor(String scalingFactor) {
      this.scalingFactor = scalingFactor;
      return this;
    }

    public Builder importName(String importName) {
      this.importName = importName;
      return this;
    }

    public Builder provenanceUrl(String provenanceUrl) {
      this.provenanceUrl = provenanceUrl;
      return this;
    }

    public Observation build() {
      return new Observation(this);
    }
  }
}
