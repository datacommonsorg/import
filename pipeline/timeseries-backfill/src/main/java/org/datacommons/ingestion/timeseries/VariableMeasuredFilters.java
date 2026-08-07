package org.datacommons.ingestion.timeseries;

import java.util.ArrayList;
import java.util.List;

/** Parses the variable_measured option into one or more filter values. */
final class VariableMeasuredFilters {
  private VariableMeasuredFilters() {}

  static List<String> parse(String value) {
    List<String> filters = new ArrayList<>();
    if (value == null || value.isEmpty()) {
      return filters;
    }
    for (String part : value.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        filters.add(trimmed);
      }
    }
    return filters;
  }
}
