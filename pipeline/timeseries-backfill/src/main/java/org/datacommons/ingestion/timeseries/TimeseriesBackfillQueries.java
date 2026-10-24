package org.datacommons.ingestion.timeseries;

import com.google.cloud.spanner.Statement;
import java.util.ArrayList;
import java.util.List;

/** Builds source Spanner SQL for the timeseries backfill. */
final class TimeseriesBackfillQueries {
  private final TimeseriesBackfillOptions options;

  TimeseriesBackfillQueries(TimeseriesBackfillOptions options) {
    this.options = options;
  }

  Statement buildSourceQuery() {
    return buildSourceQuery(0);
  }

  Statement buildSourceQuery(int limit) {
    return buildQuery(
        """
            SELECT
              observation_about,
              variable_measured,
              facet_id,
              observation_period,
              measurement_method,
              unit,
              scaling_factor,
              import_name,
              provenance_url,
              is_dc_aggregate,
              provenance,
              observations
            FROM %s"""
            .formatted(options.getSourceObservationTableName()),
        "",
        limit);
  }

  private Statement buildQuery(String selectAndFrom, String columnPrefix, int limit) {
    StringBuilder sql = new StringBuilder(selectAndFrom);
    Statement.Builder builder = Statement.newBuilder(selectAndFrom);
    List<String> filters = new ArrayList<>();
    List<String> variableMeasuredFilters =
        VariableMeasuredFilters.parse(options.getVariableMeasured());

    if (!options.getStartObservationAbout().isEmpty()) {
      filters.add(columnPrefix + "observation_about >= @startObservationAbout");
      builder.bind("startObservationAbout").to(options.getStartObservationAbout());
    }
    if (!options.getEndObservationAboutExclusive().isEmpty()) {
      filters.add(columnPrefix + "observation_about < @endObservationAboutExclusive");
      builder.bind("endObservationAboutExclusive").to(options.getEndObservationAboutExclusive());
    }
    if (!variableMeasuredFilters.isEmpty()) {
      if (variableMeasuredFilters.size() == 1) {
        filters.add(columnPrefix + "variable_measured = @variableMeasured");
        builder.bind("variableMeasured").to(variableMeasuredFilters.get(0));
      } else {
        filters.add(columnPrefix + "variable_measured IN UNNEST(@variableMeasuredList)");
        builder.bind("variableMeasuredList").toStringArray(variableMeasuredFilters);
      }
    }

    if (!filters.isEmpty()) {
      sql.append(" WHERE ").append(String.join(" AND ", filters));
    }

    if (limit > 0) {
      sql.append(" ORDER BY ")
          .append(columnPrefix)
          .append("observation_about, ")
          .append(columnPrefix)
          .append("variable_measured, ")
          .append(columnPrefix)
          .append("facet_id");
      sql.append(" LIMIT ").append(limit);
    }

    return builder.build().withReplacedSql(sql.toString());
  }
}
