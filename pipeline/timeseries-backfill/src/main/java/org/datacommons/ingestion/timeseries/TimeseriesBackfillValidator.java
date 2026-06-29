package org.datacommons.ingestion.timeseries;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Small direct runner for validating the normalized backfill on a bounded shard. */
public class TimeseriesBackfillValidator {
  private static final Logger LOGGER = LoggerFactory.getLogger(TimeseriesBackfillValidator.class);
  private static final int MAX_VALIDATOR_WRITE_BATCH_SIZE = 1000;

  public static void main(String[] args) {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(TimeseriesBackfillOptions.class);
    TimeseriesBackfillOptionValidator.validateCommonOptions(options);
    LOGGER.info("Starting timeseries validator");

    TimeseriesBackfillQueries queries = new TimeseriesBackfillQueries(options);
    DatabaseId databaseId =
        DatabaseId.of(
            TimeseriesBackfillIO.resolveProjectId(options),
            options.getSpannerInstanceId(),
            options.getSpannerDatabaseId());

    try (LocalProgressTracker progressTracker =
            new LocalProgressTracker(
                "validator",
                options.getProgressEverySourceRows(),
                options.getHeartbeatSeconds(),
                LOGGER);
        Spanner spanner = TimeseriesBackfillIO.createSpannerService(options);
        ReadOnlyTransaction readOnlyTransaction =
            spanner
                .getDatabaseClient(databaseId)
                .readOnlyTransaction(TimeseriesBackfillIO.getTimestampBound(options))) {
      DatabaseClient databaseClient = spanner.getDatabaseClient(databaseId);
      Timestamp writeTimestamp =
          validate(databaseClient, readOnlyTransaction, queries, options, progressTracker);
      LOGGER.info("Validator finished. Final write timestamp: {}", writeTimestamp);
    }
  }

  private static Timestamp validate(
      DatabaseClient databaseClient,
      ReadOnlyTransaction readOnlyTransaction,
      TimeseriesBackfillQueries queries,
      TimeseriesBackfillOptions options,
      LocalProgressTracker progressTracker) {
    int sourceRows = 0;
    int timeSeriesRows = 0;
    int timeSeriesAttributeRows = 0;
    int statVarObservationRows = 0;
    List<String> sampleSeriesIds = new ArrayList<>();
    List<Mutation> pendingMutations = new ArrayList<>();
    Timestamp lastWriteTimestamp = null;

    try (ResultSet resultSet =
        readOnlyTransaction.executeQuery(
            queries.buildSourceQuery(options.getValidatorMaxSeriesRows()))) {
      while (resultSet.next()) {
        SourceObservationRow row =
            SourceObservationRows.toObservationRow(resultSet.getCurrentRowAsStruct());
        BackfillMutationGroups mutationGroups =
            TimeseriesMutationFactory.toMutationGroups(
                row,
                options.getDestinationTimeSeriesTableName(),
                options.getDestinationTimeSeriesAttributeTableName(),
                options.getDestinationStatVarObservationTableName());
        sourceRows++;
        if (sampleSeriesIds.size() < 5) {
          sampleSeriesIds.add(TimeseriesMutationFactory.seriesId(row.seriesRow()));
        }

        timeSeriesRows++;
        timeSeriesAttributeRows += mutationGroups.timeSeriesAttributeRows();
        statVarObservationRows += mutationGroups.statVarObservationRows();
        progressTracker.recordRow(
            mutationGroups.timeSeriesAttributeRows(), mutationGroups.statVarObservationRows());
        for (MutationGroup mutationGroup : mutationGroups.groups()) {
          for (Mutation mutation : mutationGroup) {
            pendingMutations.add(mutation);
          }
        }

        lastWriteTimestamp =
            flushMutationsIfNeeded(
                databaseClient, pendingMutations, lastWriteTimestamp, progressTracker);
      }
    }

    lastWriteTimestamp =
        flushMutationsIfNeeded(
            databaseClient, pendingMutations, lastWriteTimestamp, progressTracker, true);
    LOGGER.info(
        "Validated {} source rows into {} TimeSeries rows, {} TimeSeriesAttribute rows, and {} StatVarObservation rows. Sample series ids: {}",
        sourceRows,
        timeSeriesRows,
        timeSeriesAttributeRows,
        statVarObservationRows,
        sampleSeriesIds);
    return lastWriteTimestamp;
  }

  private static Timestamp flushMutationsIfNeeded(
      DatabaseClient databaseClient,
      List<Mutation> pendingMutations,
      Timestamp lastWriteTimestamp,
      LocalProgressTracker progressTracker) {
    return flushMutationsIfNeeded(
        databaseClient, pendingMutations, lastWriteTimestamp, progressTracker, false);
  }

  private static Timestamp flushMutationsIfNeeded(
      DatabaseClient databaseClient,
      List<Mutation> pendingMutations,
      Timestamp lastWriteTimestamp,
      LocalProgressTracker progressTracker,
      boolean flushAll) {
    if (pendingMutations.isEmpty()) {
      return lastWriteTimestamp;
    }
    if (!flushAll && pendingMutations.size() < MAX_VALIDATOR_WRITE_BATCH_SIZE) {
      return lastWriteTimestamp;
    }

    int mutationCount = pendingMutations.size();
    Timestamp writeTimestamp = databaseClient.writeAtLeastOnce(pendingMutations);
    if (progressTracker.isEnabled()) {
      progressTracker.recordValidatorFlush(mutationCount, writeTimestamp);
    }
    pendingMutations.clear();
    return writeTimestamp;
  }
}
