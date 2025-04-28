package org.datacommons.ingestion.pipeline;

import static org.datacommons.ingestion.pipeline.Transforms.buildImportGroupPipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportGroupPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportGroupPipeline.class);

  public static void main(String[] args) {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info(
        "Running import group pipeline for import group: {}", options.getImportGroupVersion());

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

    buildImportGroupPipeline(pipeline, options.getImportGroupVersion(), cacheReader, spannerClient);

    pipeline.run();
  }
}
