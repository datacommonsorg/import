package org.datacommons.ingestion.pipeline;

import static org.datacommons.ingestion.data.ImportGroupVersions.getImportGroupVersions;
import static org.datacommons.ingestion.pipeline.Transforms.buildIngestionPipeline;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestionPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipeline.class);

  public static void main(String[] args) {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info("Fetching versions from: {}", options.getVersionEndpoint());

    CacheReader cacheReader =
        new CacheReader(options.getStorageBucketId(), options.getSkipPredicatePrefixes());
    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId(options.getProjectId())
            .spannerInstanceId(options.getSpannerInstanceId())
            .spannerDatabaseId(options.getSpannerDatabaseId())
            .nodeTableName(options.getSpannerNodeTableName())
            .edgeTableName(options.getSpannerEdgeTableName())
            .observationTableName(options.getSpannerObservationTableName())
            .build();

    List<String> importGroupVersions = getImportGroupVersions(options.getVersionEndpoint());

    buildIngestionPipeline(pipeline, importGroupVersions, cacheReader, spannerClient);

    pipeline.run();
  }
}
