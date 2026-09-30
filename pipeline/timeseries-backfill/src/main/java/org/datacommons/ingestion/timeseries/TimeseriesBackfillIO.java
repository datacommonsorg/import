package org.datacommons.ingestion.timeseries;

import com.google.cloud.NoCredentials;
import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.TimestampBound;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.joda.time.Duration;

/** Shared Spanner IO configuration for the backfill job and validator. */
final class TimeseriesBackfillIO {
  // Previous sink tuning values kept here for reference only:
  // private static final int SPANNER_BATCH_SIZE_BYTES = 500 * 1024;
  // private static final int SPANNER_MAX_NUM_ROWS = 2000;
  // private static final int SPANNER_MAX_NUM_MUTATIONS = 10000;
  // private static final int SPANNER_GROUPING_FACTOR = 3000;
  // private static final int SPANNER_COMMIT_DEADLINE_SECONDS = 120;

  private TimeseriesBackfillIO() {}

  static String resolveProjectId(TimeseriesBackfillOptions options) {
    if (options.getProject() != null && !options.getProject().isEmpty()) {
      return options.getProject();
    }
    return options.getProjectId();
  }

  static SpannerIO.CreateTransaction buildReadTransaction(TimeseriesBackfillOptions options) {
    SpannerIO.CreateTransaction read =
        SpannerIO.createTransaction()
            .withProjectId(resolveProjectId(options))
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId())
            .withTimestampBound(getTimestampBound(options));
    if (!options.getSpannerEmulatorHost().isEmpty()) {
      read = read.withEmulatorHost(options.getSpannerEmulatorHost());
    }
    return read;
  }

  static SpannerIO.Read buildRead(TimeseriesBackfillOptions options) {
    SpannerIO.Read read =
        SpannerIO.read()
            .withProjectId(resolveProjectId(options))
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId());
    if (!options.getSpannerEmulatorHost().isEmpty()) {
      read = read.withEmulatorHost(options.getSpannerEmulatorHost());
    }
    return read;
  }

  static SpannerIO.Write buildWrite(TimeseriesBackfillOptions options) {
    SpannerIO.Write write =
        SpannerIO.write()
            .withProjectId(resolveProjectId(options))
            .withInstanceId(options.getSpannerInstanceId())
            .withDatabaseId(options.getSpannerDatabaseId());
    if (!options.getSpannerEmulatorHost().isEmpty()) {
      write = write.withEmulatorHost(options.getSpannerEmulatorHost());
    }
    if (options.getBatchSizeBytes() != null) {
      write = write.withBatchSizeBytes(options.getBatchSizeBytes());
    }
    if (options.getMaxNumRows() != null) {
      write = write.withMaxNumRows(options.getMaxNumRows());
    }
    if (options.getGroupingFactor() != null) {
      write = write.withGroupingFactor(options.getGroupingFactor());
    }
    if (options.getMaxNumMutations() != null) {
      write = write.withMaxNumMutations(options.getMaxNumMutations());
    }
    if (options.getCommitDeadlineSeconds() != null) {
      write =
          write.withCommitDeadline(Duration.standardSeconds(options.getCommitDeadlineSeconds()));
    }
    return write;
  }

  static SpannerIO.WriteGrouped buildGroupedWrite(TimeseriesBackfillOptions options) {
    return new SpannerIO.WriteGrouped(buildWrite(options));
  }

  static TimestampBound getTimestampBound(TimeseriesBackfillOptions options) {
    if (options.getReadTimestamp().isEmpty()) {
      return TimestampBound.strong();
    }
    return TimestampBound.ofReadTimestamp(Timestamp.parseTimestamp(options.getReadTimestamp()));
  }

  static Spanner createSpannerService(TimeseriesBackfillOptions options) {
    SpannerOptions.Builder builder =
        SpannerOptions.newBuilder().setProjectId(resolveProjectId(options));
    if (!options.getSpannerEmulatorHost().isEmpty()) {
      builder
          .setEmulatorHost(options.getSpannerEmulatorHost())
          .setCredentials(NoCredentials.getInstance());
    }
    return builder.build().getService();
  }
}
