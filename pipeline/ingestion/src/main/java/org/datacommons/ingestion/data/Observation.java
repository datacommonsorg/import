package org.datacommons.ingestion.data;

import java.util.Objects;

import org.datacommons.proto.Storage.Observations;

/**
 * Models a statvar observation time series.
 * 
 * This class is used to store the result of
 * parsing a time series row in memory. It is not inserted into the pipeline.
 * 
 */
public class Observation {

  private String variableMeasured;
  private String observationAbout;
  private Observations observations;
  private String observationPeriod;
  private String measurementMethod;
  private String unit;
  private String scalingFactor;
  private String importName;
  private String provenanceUrl;
  private String facetId;
  private boolean isDcAggregate;

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
    this.facetId = builder.facetId;
    this.isDcAggregate = builder.isDcAggregate;
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

  public String getFacetId() {
    return facetId;
  }

  public boolean isDcAggregate() {
    return isDcAggregate;
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
    private String facetId = "";
    private boolean isDcAggregate = false;

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

    public Builder isDcAggregate(boolean isDcAggregate) {
      this.isDcAggregate = isDcAggregate;
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
      this.facetId = String
          .valueOf(
              Objects.hash(
                  importName,
                  measurementMethod,
                  observationPeriod,
                  scalingFactor,
                  unit,
                  isDcAggregate));
      return new Observation(this);
    }
  }
}
