package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * BasePipelineOptions interface for defining common ingestion pipeline options.
 */
public interface BasePipelineOptions extends PipelineOptions {
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

}
