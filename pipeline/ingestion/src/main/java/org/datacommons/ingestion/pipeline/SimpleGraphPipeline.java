package org.datacommons.ingestion.pipeline;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.CacheReader;
import org.datacommons.ingestion.pipeline.Transforms.ArcRowToMutationDoFn;
import org.datacommons.ingestion.pipeline.Transforms.GraphMutationGroupTransform;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.spanner.Mutation;

/**
 * SimpleGraphPipeline is a pipeline that reads a single import group from cache
 * and populates the spanner graph tables (nodes and edges) simultaneously.
 * DO NOT use this pipeline if you want to write nodes and edges separately or
 * in a certain order.
 * It is a simple pipeline that reads from the cache and writes to Spanner.
 */
public class SimpleGraphPipeline {
    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleGraphPipeline.class);

    public static void main(String[] args) {
        SimpleGraphPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(SimpleGraphPipelineOptions.class);
        Pipeline pipeline = Pipeline.create(options);
        LOGGER.info("Running simple graph pipeline for import group: {}", options.getImportGroup());

        CacheReader cacheReader = new CacheReader(options.getSkipPredicatePrefixes());
        SpannerClient spannerClient = SpannerClient.builder().gcpProjectId(options.getProjectId())
                .spannerInstanceId(options.getSpannerInstanceId())
                .spannerDatabaseId(options.getSpannerDatabaseId())
                .nodeTableName(options.getSpannerNodeTableName())
                .edgeTableName(options.getSpannerEdgeTableName())
                .observationTableName(options.getSpannerObservationTableName())
                .build();

        String cachePath = CacheReader.getCachePath(options.getProjectId(), options.getStorageBucketId(),
                options.getImportGroup());

        PCollection<String> entries = pipeline.apply("ReadCache", TextIO.read().from(cachePath));
        PCollection<Mutation> mutations = entries.apply("CreateMutations",
                ParDo.of(new ArcRowToMutationDoFn(cacheReader, spannerClient)));
        PCollection<Mutation> groupedMutations = mutations
                .apply("GroupAndSortMutations", new GraphMutationGroupTransform());
        groupedMutations.apply("WriteToSpanner", spannerClient.getWriteTransform());

        pipeline.run();
    }
}
