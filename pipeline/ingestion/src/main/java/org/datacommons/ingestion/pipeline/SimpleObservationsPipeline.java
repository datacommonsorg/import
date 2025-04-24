package org.datacommons.ingestion.pipeline;

import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.pipeline.Transforms.ObsTimeSeriesRowToMutationGroupDoFn;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SimpleObservationsPipeline is a pipeline that reads a single import group from cache and
 * populates the spanner observations table.
 */
public class SimpleObservationsPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleObservationsPipeline.class);

  public static void main(String[] args) {
    SimpleObservationsPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args)
            .withValidation()
            .as(SimpleObservationsPipelineOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info(
        "Running simple observations pipeline for import group: {}", options.getImportGroup());

    CacheReader cacheReader = new CacheReader(List.of());
    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId(options.getProjectId())
            .spannerInstanceId(options.getSpannerInstanceId())
            .spannerDatabaseId(options.getSpannerDatabaseId())
            .nodeTableName(options.getSpannerNodeTableName())
            .edgeTableName(options.getSpannerEdgeTableName())
            .observationTableName(options.getSpannerObservationTableName())
            .build();

    String cachePath =
        CacheReader.getCachePath(
            options.getProjectId(), options.getStorageBucketId(), options.getImportGroup());

    PCollection<String> entries = pipeline.apply("ReadFromCache", TextIO.read().from(cachePath));
    PCollection<MutationGroup> mutationGroups =
        entries.apply(
            "CreateMutationGroups",
            ParDo.of(new ObsTimeSeriesRowToMutationGroupDoFn(cacheReader, spannerClient)));
    mutationGroups.apply("WriteToSpanner", spannerClient.getWriteGroupedTransform());

    pipeline.run();
  }
}
