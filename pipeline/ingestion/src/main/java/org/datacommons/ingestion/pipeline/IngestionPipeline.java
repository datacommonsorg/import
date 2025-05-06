package org.datacommons.ingestion.pipeline;

import static org.datacommons.ingestion.data.ImportGroupVersions.getImportGroupVersions;
import static org.datacommons.ingestion.pipeline.Transforms.buildIngestionPipeline;

import com.google.common.base.Strings;
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

    List<String> importGroupVersions = List.of();

    String specificImportGroupVersion = options.getImportGroupVersion();
    if (!Strings.isNullOrEmpty(specificImportGroupVersion)) {
      LOGGER.info("Ingesting import group version: {}", specificImportGroupVersion);
      importGroupVersions = List.of(specificImportGroupVersion);
    } else {
      LOGGER.info("Fetching versions from: {}", options.getVersionEndpoint());
      importGroupVersions = getImportGroupVersions(options.getVersionEndpoint());
    }

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
            .numShards(options.getNumShards())
            .build();

    buildIngestionPipeline(pipeline, importGroupVersions, cacheReader, spannerClient);

    pipeline.run();
  }
}
