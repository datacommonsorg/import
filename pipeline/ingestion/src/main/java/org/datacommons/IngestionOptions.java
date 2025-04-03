package org.datacommons;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

public interface IngestionOptions extends PipelineOptions {

    @Description("GCP project id")
    String getProjectId();

    void setProjectId(String projectId);

    @Description("Spanner Instance Id for output")
    String getSpannerInstanceId();

    void setSpannerInstanceId(String instanceId);

    @Description("Spanner Database Id for output")
    String getSpannerDatabaseId();

    void setSpannerDatabaseId(String databaseId);

    @Description("GCS bucket Id for input data")
    String getStorageBucketId();

    void setStorageBucketId(String bucketId);

    @Description("Import group list in CSV format")
    String getImportGroupList();

    void setImportGroupList(String importGroupList);

    @Description("Cache type for import (observation/graph)")
    @Default.String("graph")
    String getCacheType();

    void setCacheType(String cacheType);

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
