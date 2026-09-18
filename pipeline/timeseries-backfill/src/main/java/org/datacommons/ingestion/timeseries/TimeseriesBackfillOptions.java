package org.datacommons.ingestion.timeseries;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;

/** Options shared by the Beam backfill and the standalone validator. */
public interface TimeseriesBackfillOptions extends GcpOptions {
  @Description("Legacy fallback for GCP project id. Prefer --project.")
  @Default.String("datcom-store")
  String getProjectId();

  void setProjectId(String projectId);

  @Description("Spanner instance id")
  @Default.String("dc-kg-test")
  String getSpannerInstanceId();

  void setSpannerInstanceId(String instanceId);

  @Description("Spanner database id")
  @Default.String("dc_graph_5")
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String databaseId);

  @Description("Spanner emulator host")
  @Default.String("")
  String getSpannerEmulatorHost();

  void setSpannerEmulatorHost(String emulatorHost);

  @Description("Source Observation table name")
  @Default.String("Observation")
  String getSourceObservationTableName();

  void setSourceObservationTableName(String tableName);

  @Description(
      "Input Spanner export directory containing Observation-manifest.json for the Avro entrypoint")
  @Default.String("")
  String getInputExportDir();

  void setInputExportDir(String inputExportDir);

  @Description("Comma-separated Avro file paths for the Avro entrypoint")
  @Default.String("")
  String getInputFiles();

  void setInputFiles(String inputFiles);

  @Description("Destination TimeSeries table name")
  @Default.String("TimeSeries_rk")
  String getDestinationTimeSeriesTableName();

  void setDestinationTimeSeriesTableName(String tableName);

  @Description("Destination TimeSeriesAttribute table name")
  @Default.String("TimeSeriesAttribute_rk")
  String getDestinationTimeSeriesAttributeTableName();

  void setDestinationTimeSeriesAttributeTableName(String tableName);

  @Description("Destination StatVarObservation table name")
  @Default.String("StatVarObservation_rk")
  String getDestinationStatVarObservationTableName();

  void setDestinationStatVarObservationTableName(String tableName);

  @Description(
      "Read timestamp in RFC3339 format for a consistent source snapshot. Empty uses a strong read.")
  @Default.String("")
  String getReadTimestamp();

  void setReadTimestamp(String readTimestamp);

  @Description("Inclusive lower bound for source observation_about")
  @Default.String("")
  String getStartObservationAbout();

  void setStartObservationAbout(String startObservationAbout);

  @Description("Exclusive upper bound for source observation_about")
  @Default.String("")
  String getEndObservationAboutExclusive();

  void setEndObservationAboutExclusive(String endObservationAboutExclusive);

  @Description(
      "Fixed source variable_measured filter. Accepts one value or a comma-separated list.")
  @Default.String("")
  String getVariableMeasured();

  void setVariableMeasured(String variableMeasured);

  @Description("Log local progress every N source rows. Non-positive disables row-progress logs.")
  @Default.Integer(1000)
  int getProgressEverySourceRows();

  void setProgressEverySourceRows(int progressEverySourceRows);

  @Description("Emit a local heartbeat log every N seconds. Non-positive disables heartbeats.")
  @Default.Integer(30)
  int getHeartbeatSeconds();

  void setHeartbeatSeconds(int heartbeatSeconds);

  @Description("Optional Spanner sink batch size in bytes. When unset, Beam uses its default.")
  Long getBatchSizeBytes();

  void setBatchSizeBytes(Long batchSizeBytes);

  @Description("Optional Spanner sink max rows per batch. When unset, Beam uses its default.")
  Integer getMaxNumRows();

  void setMaxNumRows(Integer maxNumRows);

  @Description("Optional Spanner sink max mutations per batch. When unset, Beam uses its default.")
  Integer getMaxNumMutations();

  void setMaxNumMutations(Integer maxNumMutations);

  @Description("Optional Spanner sink grouping factor. When unset, Beam uses its default.")
  Integer getGroupingFactor();

  void setGroupingFactor(Integer groupingFactor);

  @Description(
      "Optional Spanner sink commit deadline in seconds. When unset, Beam uses its default.")
  Integer getCommitDeadlineSeconds();

  void setCommitDeadlineSeconds(Integer commitDeadlineSeconds);

  @Description("Maximum source series rows to process in Beam mode. Non-positive means no limit.")
  @Default.Integer(0)
  int getMaxSeriesRows();

  void setMaxSeriesRows(int maxSeriesRows);

  @Description("Maximum source point rows to process in Beam mode. Non-positive means no limit.")
  @Default.Integer(0)
  int getMaxPointRows();

  void setMaxPointRows(int maxPointRows);

  @Description(
      "Maximum source series rows to process in validator mode. Non-positive means no limit.")
  @Default.Integer(1000)
  int getValidatorMaxSeriesRows();

  void setValidatorMaxSeriesRows(int validatorMaxSeriesRows);
}
