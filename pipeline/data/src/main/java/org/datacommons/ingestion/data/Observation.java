package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.Objects;

/** Models a single statvar observation data point. */
public class Observation implements Serializable {
  private final TimeSeriesKey seriesKey;
  private final String date;
  private final String value;

  private Observation(Builder builder) {
    this.seriesKey = builder.seriesKey;
    this.date = builder.date;
    this.value = builder.value;
  }

  public static Builder builder() {
    return new Builder();
  }

  public TimeSeriesKey getSeriesKey() {
    return seriesKey;
  }

  public String getDate() {
    return date;
  }

  public String getValue() {
    return value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Observation that = (Observation) o;
    return Objects.equals(seriesKey, that.seriesKey)
        && Objects.equals(date, that.date)
        && Objects.equals(value, that.value);
  }

  @Override
  public int hashCode() {
    return Objects.hash(seriesKey, date, value);
  }

  @Override
  public String toString() {
    return String.format(
        "Observation{seriesKey=%s, date='%s', value='%s'}", seriesKey, date, value);
  }

  public static class Builder {
    private TimeSeriesKey seriesKey;
    private String date = "";
    private String value = "";

    public Builder seriesKey(TimeSeriesKey seriesKey) {
      this.seriesKey = seriesKey;
      return this;
    }

    public Builder date(String date) {
      this.date = date;
      return this;
    }

    public Builder value(String value) {
      this.value = value;
      return this;
    }

    public Observation build() {
      Objects.requireNonNull(seriesKey, "seriesKey cannot be null");
      return new Observation(this);
    }
  }
}
