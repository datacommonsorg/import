package org.datacommons.ingestion.timeseries;

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;

/** Converts source rows into normalized timeseries table mutations. */
final class TimeseriesMutationFactory {
  private static final int MAX_GROUP_MUTATIONS = 1000;

  private TimeseriesMutationFactory() {}

  static Mutation toTimeSeriesMutation(SourceSeriesRow row, String tableName) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("id")
        .to(seriesId(row))
        .set("variable_measured")
        .to(row.variableMeasured())
        .set("provenance")
        .to(row.provenance())
        .build();
  }

  static List<Mutation> toTimeSeriesAttributeMutations(SourceSeriesRow row, String tableName) {
    List<Mutation> mutations = new ArrayList<>();
    addAttribute(mutations, tableName, row, "observationAbout", row.observationAbout());
    addAttribute(mutations, tableName, row, "facetId", row.facetId());
    addAttributeIfPresent(mutations, tableName, row, "importName", row.importName());
    addAttributeIfPresent(mutations, tableName, row, "provenanceUrl", row.provenanceUrl());
    addAttributeIfPresent(mutations, tableName, row, "observationPeriod", row.observationPeriod());
    addAttributeIfPresent(mutations, tableName, row, "measurementMethod", row.measurementMethod());
    addAttributeIfPresent(mutations, tableName, row, "unit", row.unit());
    addAttributeIfPresent(mutations, tableName, row, "scalingFactor", row.scalingFactor());
    addAttribute(mutations, tableName, row, "isDcAggregate", Boolean.toString(row.isDcAggregate()));
    return mutations;
  }

  static Mutation toStatVarObservationMutation(SourcePointRow row, String tableName) {
    return Mutation.newInsertOrUpdateBuilder(tableName)
        .set("id")
        .to(seriesId(row))
        .set("date")
        .to(row.date())
        .set("value")
        .to(row.value())
        .build();
  }

  private static void addAttributeIfPresent(
      List<Mutation> mutations,
      String tableName,
      SourceSeriesRow row,
      String property,
      String value) {
    if (value == null || value.isEmpty()) {
      return;
    }
    addAttribute(mutations, tableName, row, property, value);
  }

  private static void addAttribute(
      List<Mutation> mutations,
      String tableName,
      SourceSeriesRow row,
      String property,
      String value) {
    mutations.add(
        Mutation.newInsertOrUpdateBuilder(tableName)
            .set("id")
            .to(seriesId(row))
            .set("property")
            .to(property)
            .set("value")
            .to(value)
            .build());
  }

  static BackfillMutationGroups toMutationGroups(
      SourceObservationRow row,
      String timeSeriesTableName,
      String timeSeriesAttributeTableName,
      String statVarObservationTableName) {
    Mutation timeSeriesMutation = toTimeSeriesMutation(row.seriesRow(), timeSeriesTableName);
    List<Mutation> attributeMutations =
        toTimeSeriesAttributeMutations(row.seriesRow(), timeSeriesAttributeTableName);
    List<Mutation> pointMutations = new ArrayList<>();
    for (SourcePointRow pointRow : row.pointRows()) {
      pointMutations.add(toStatVarObservationMutation(pointRow, statVarObservationTableName));
    }

    List<MutationGroup> mutationGroups = new ArrayList<>();
    List<Mutation> attached = new ArrayList<>(attributeMutations);
    int maxAttachedMutations = MAX_GROUP_MUTATIONS - 1;
    int pointIndex = 0;
    while (pointIndex < pointMutations.size() && attached.size() < maxAttachedMutations) {
      attached.add(pointMutations.get(pointIndex));
      pointIndex++;
    }
    mutationGroups.add(MutationGroup.create(timeSeriesMutation, attached));

    while (pointIndex < pointMutations.size()) {
      attached = new ArrayList<>();
      while (pointIndex < pointMutations.size() && attached.size() < maxAttachedMutations) {
        attached.add(pointMutations.get(pointIndex));
        pointIndex++;
      }
      mutationGroups.add(MutationGroup.create(timeSeriesMutation, attached));
    }

    return new BackfillMutationGroups(
        mutationGroups, attributeMutations.size(), pointMutations.size());
  }

  static String seriesId(SourceSeriesRow row) {
    return SeriesIdGenerator.build(row.variableMeasured(), row.observationAbout(), row.facetId());
  }

  static String seriesId(SourcePointRow row) {
    return SeriesIdGenerator.build(row.variableMeasured(), row.observationAbout(), row.facetId());
  }
}

record BackfillMutationGroups(
    List<MutationGroup> groups, int timeSeriesAttributeRows, int statVarObservationRows) {}
