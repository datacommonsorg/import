package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/** Models a statvar observation time series. */
@DefaultCoder(AvroCoder.class)
public class Observation implements Serializable {

  private String variableMeasured;
  private String observationAbout;
  private List<DateValue> observations;
  private String provenance;
  private String observationPeriod;
  private String measurementMethod;
  private String unit;
  private String scalingFactor;
  private String importName;
  private String provenanceUrl;

  private Observation(Builder builder) {
    this.variableMeasured = builder.variableMeasured;
    this.observationAbout = builder.observationAbout;
    this.observations = builder.observations;
    this.provenance = builder.provenance;
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

  public List<DateValue> getObservations() {
    return observations;
  }

  public String getProvenance() {
    return provenance;
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
        && Objects.equals(provenance, that.provenance)
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
        provenance,
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
            + "provenance='%s', "
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
        provenance,
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
    private List<DateValue> observations = new ArrayList<>();
    private String provenance = "";
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

    public Builder observation(DateValue dateValue) {
      this.observations.add(dateValue);
      return this;
    }

    public Builder observations(List<DateValue> observations) {
      this.observations = observations;
      return this;
    }

    public Builder provenance(String provenance) {
      this.provenance = provenance;
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
