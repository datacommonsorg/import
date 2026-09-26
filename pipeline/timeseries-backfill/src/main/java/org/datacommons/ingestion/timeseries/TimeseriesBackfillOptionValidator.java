package org.datacommons.ingestion.timeseries;

/** Shared validation for backfill options. */
final class TimeseriesBackfillOptionValidator {
  private TimeseriesBackfillOptionValidator() {}

  static void validateCommonOptions(TimeseriesBackfillOptions options) {
    if ((!options.getStartObservationAbout().isEmpty()
            || !options.getEndObservationAboutExclusive().isEmpty())
        && VariableMeasuredFilters.parse(options.getVariableMeasured()).isEmpty()) {
      throw new IllegalArgumentException(
          "variableMeasured is required when startObservationAbout or "
              + "endObservationAboutExclusive is provided.");
    }
    validateOptionalPositiveLong(options.getBatchSizeBytes(), "batchSizeBytes");
    validateOptionalPositiveInteger(options.getMaxNumRows(), "maxNumRows");
    validateOptionalPositiveInteger(options.getMaxNumMutations(), "maxNumMutations");
    validateOptionalPositiveInteger(options.getGroupingFactor(), "groupingFactor");
    validateOptionalPositiveInteger(options.getCommitDeadlineSeconds(), "commitDeadlineSeconds");
  }

  private static void validateOptionalPositiveLong(Long value, String optionName) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(optionName + " must be positive when provided.");
    }
  }

  private static void validateOptionalPositiveInteger(Integer value, String optionName) {
    if (value != null && value <= 0) {
      throw new IllegalArgumentException(optionName + " must be positive when provided.");
    }
  }
}
