package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.Objects;

/** Uniquely identifies a time series. */
public class TimeSeriesKey implements Serializable {
  private final String variableMeasured;
  private final String observationAbout;
  private final String observationPeriod;
  private final String measurementMethod;
  private final String unit;
  private final String scalingFactor;
  private final String facetId;

  public TimeSeriesKey(
      String variableMeasured,
      String observationAbout,
      String observationPeriod,
      String measurementMethod,
      String unit,
      String scalingFactor,
      String facetId) {
    this.variableMeasured = Objects.requireNonNull(variableMeasured);
    this.observationAbout = Objects.requireNonNull(observationAbout);
    this.observationPeriod = Objects.requireNonNull(observationPeriod);
    this.measurementMethod = Objects.requireNonNull(measurementMethod);
    this.unit = Objects.requireNonNull(unit);
    this.scalingFactor = Objects.requireNonNull(scalingFactor);
    this.facetId = Objects.requireNonNull(facetId);
  }

  public String getVariableMeasured() {
    return variableMeasured;
  }

  public String getObservationAbout() {
    return observationAbout;
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

  public String getFacetId() {
    return facetId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TimeSeriesKey that = (TimeSeriesKey) o;
    return variableMeasured.equals(that.variableMeasured)
        && observationAbout.equals(that.observationAbout)
        && observationPeriod.equals(that.observationPeriod)
        && measurementMethod.equals(that.measurementMethod)
        && unit.equals(that.unit)
        && scalingFactor.equals(that.scalingFactor)
        && facetId.equals(that.facetId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        variableMeasured,
        observationAbout,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        facetId);
  }

  @Override
  public String toString() {
    return String.format(
        "TimeSeriesKey{variableMeasured='%s', observationAbout='%s', observationPeriod='%s', "
            + "measurementMethod='%s', unit='%s', scalingFactor='%s', facetId='%s'}",
        variableMeasured,
        observationAbout,
        observationPeriod,
        measurementMethod,
        unit,
        scalingFactor,
        facetId);
  }
}
