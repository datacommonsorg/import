package org.datacommons;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * Spanner graph data ingestion pipeline.
 */
public class IngestionPipeline {

  private static final Logger LOGGER = LoggerFactory.getLogger(IngestionPipeline.class);

  public static void main(String[] args) {
    IngestionOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionOptions.class);
    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info(
        "Running ingestion pipeline for {} cache type and {} import groups",
        options.getCacheType(),
        options.getImportGroupList());
    String[] groups = options.getImportGroupList().split(",");

    if (options.getCacheType().equals("observation")) {
      List<PCollection<Observation>> collectionList = new ArrayList<>();
      for (String group : groups) {
        String path =
            CacheReader.getCachePath(options.getProjectId(), options.getStorageBucketId(), group);
        PCollection<String> entries = pipeline.apply(group, TextIO.read().from(path));
        PCollection<Observation> result = CacheReader.getObservations(entries);
        collectionList.add(result);
      }
      PCollectionList<Observation> collections = PCollectionList.of(collectionList);
      PCollection<Observation> merged = collections.apply(Flatten.pCollections());
      SpannerClient.WriteObservations(options, merged);
    } else { // graph
      List<PCollection<Entity>> collectionList = new ArrayList<>();
      for (String group : groups) {
        String path =
            CacheReader.getCachePath(options.getProjectId(), options.getStorageBucketId(), group);
        PCollection<String> entries = pipeline.apply(group, TextIO.read().from(path));
        PCollection<Entity> result = CacheReader.getEntities(entries);
        collectionList.add(result);
      }
      PCollectionList<Entity> collections = PCollectionList.of(collectionList);
      PCollection<Entity> merged = collections.apply(Flatten.pCollections());
      SpannerClient.WriteGraph(options, merged);
    }
    pipeline.run();
  }
}
