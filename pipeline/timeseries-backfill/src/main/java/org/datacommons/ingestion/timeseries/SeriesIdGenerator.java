package org.datacommons.ingestion.timeseries;

/** Builds the existing dc/os series identifier used as the normalized series id. */
final class SeriesIdGenerator {
  private static final String PREFIX = "dc/os/";

  private SeriesIdGenerator() {}

  static String build(String variableMeasured, String observationAbout, String facetId) {
    return PREFIX
        + sanitize(variableMeasured)
        + "_"
        + sanitize(observationAbout)
        + "_"
        + sanitize(facetId);
  }

  private static String sanitize(String value) {
    return value == null ? "" : value.replace('/', '_');
  }
}
