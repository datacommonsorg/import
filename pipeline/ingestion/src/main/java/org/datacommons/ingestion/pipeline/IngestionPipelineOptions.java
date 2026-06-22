package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** IngestionPipelineOptions interface for defining spanner ingestion pipeline options. */
public interface IngestionPipelineOptions extends PipelineOptions {
  @Description("GCP project id")
  @Default.String("datcom-store")
  String getProjectId();

  void setProjectId(String projectId);

  @Description("Spanner Instance Id for output")
  @Default.String("dc-kg-test")
  String getSpannerInstanceId();

  void setSpannerInstanceId(String instanceId);

  @Description("Spanner Database Id for output")
  @Default.String("dc_graph_5")
  String getSpannerDatabaseId();

  void setSpannerDatabaseId(String databaseId);

  @Description("GCS bucket Id for input data")
  @Default.String("datcom-store")
  String getStorageBucketId();

  void setStorageBucketId(String bucketId);

  @Description("List of imports for ingestion (CSV)")
  String getImportList();

  void setImportList(String importList);

  @Description("The number of shards to generate for writing mutations.")
  @Default.Integer(1)
  int getNumShards();

  void setNumShards(int numShards);

  @Description("Whether to skip delete operations.")
  @Default.Boolean(false)
  boolean getSkipDelete();

  void setSkipDelete(boolean skipDelete);

  @Description("Spanner Observation table name")
  @Default.String("Observation")
  String getSpannerObservationTableName();

  void setSpannerObservationTableName(String tableName);

  @Description("Spanner Node table name")
  @Default.String("Node")
  String getSpannerNodeTableName();

  void setSpannerNodeTableName(String tableName);

  @Description("Spanner Edge table name")
  @Default.String("Edge")
  String getSpannerEdgeTableName();

  void setSpannerEdgeTableName(String tableName);

  @Description("Spanner TimeSeries table name")
  @Default.String("TimeSeries")
  String getSpannerTimeSeriesTableName();

  void setSpannerTimeSeriesTableName(String tableName);

  @Description("Whether to force combination of schema nodes across shards.")
  @Default.Boolean(false)
  boolean getForceCombineNodes();

  void setForceCombineNodes(boolean forceCombineNodes);

  @Description("Whether this is a base Data Commons ingestion run")
  @Default.Boolean(true)
  boolean getIsBaseDc();

  void setIsBaseDc(boolean isBaseDc);
}
