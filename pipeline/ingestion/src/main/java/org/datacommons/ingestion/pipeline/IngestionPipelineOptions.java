package org.datacommons.ingestion.pipeline;

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
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

  @Description("The DC version endpoint to fetch import group versions to ingest.")
  @Default.String("https://autopush.api.datacommons.org/version")
  String getVersionEndpoint();

  void setVersionEndpoint(String versionEndpoint);

  @Description(
      "The import group version to be ingested into Spanner. e.g. auto1d_2025_03_26_02_16_23")
  String getImportGroupVersion();

  void setImportGroupVersion(String importGroupVersion);

  @Description("The number of shards to generate for writing edge mutations.")
  @Default.Integer(100)
  int getNumEdgeShards();

  void setNumEdgeShards(int numEdgeShards);

  @Description("If true, observations will not be ingested.")
  @Default.Boolean(false)
  boolean getSkipObservations();

  void setSkipObservations(boolean skipObservations);

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

  @Description("List of prefixes of predicates to skip during ingestion.")
  @Default.InstanceFactory(SkipPredicatePrefixesFactory.class)
  List<String> getSkipPredicatePrefixes();

  void setSkipPredicatePrefixes(List<String> skipPredicatePrefixes);

  static class SkipPredicatePrefixesFactory implements DefaultValueFactory<List<String>> {
    @Override
    public List<String> create(PipelineOptions options) {
      return List.of("geoJsonCoordinates", "kmlCoordinates");
    }
  }
}
