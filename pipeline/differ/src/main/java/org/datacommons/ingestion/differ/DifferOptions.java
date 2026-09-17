package org.datacommons.ingestion.differ;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation.Required;

/** Parameters for the differ pipeline. */
public interface DifferOptions extends PipelineOptions {
  @Description("Path of the current data")
  @Required
  String getCurrentData();

  void setCurrentData(String value);

  @Description("Path of the previous data")
  @Required
  String getPreviousData();

  void setPreviousData(String value);

  @Description("Path of the diff output")
  @Required
  String getOutputLocation();

  void setOutputLocation(String value);

  @Description("Whether to use optimized tfrecord file format")
  Boolean getUseOptimizedGraphFormat();

  void setUseOptimizedGraphFormat(Boolean value);

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

  @Description("The number of shards to generate for writing mutations.")
  @Default.Integer(1)
  int getNumShards();

  void setNumShards(int numShards);
}
