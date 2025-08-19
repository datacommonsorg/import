package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.datacommons.ingestion.data.GraphReader;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportGroupPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportGroupPipeline.class);
  private static final String TABLE_MCF_NODES = "table_mcf_nodes";
  private static final String INSTANCE_MCF_NODES = "instance_mcf_nodes";
  private static final String GCS_PREFIX = "gs://";
  private static final String MCF_SUFFIX = "*.mcf";
  private static final Counter counter =
      Metrics.counter(ImportGroupPipeline.class, "mcf_nodes_without_type");

  private static String getGraphPath(String projectId, String bucketId, String importName) {
    Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    String importPath = importName.replace(":", "/");
    String versionPath = Paths.get(importPath, "latest_version.txt").toString();
    Blob blob = storage.get(bucketId, versionPath);
    String version = new String(blob.getContent());
    String graphPath = Paths.get(bucketId, importPath, version, "*", "validation").toString();
    return GCS_PREFIX + graphPath;
  }

  public static void main(String[] args) {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

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

    LOGGER.info("Starting Spanner DDL creation...");
    spannerClient.createDatabase();
    LOGGER.info("Spanner DDL creation complete.");

    Pipeline pipeline = Pipeline.create(options);
    LOGGER.info("Running import pipeline for imports: {}", options.getImportList());

    String[] imports = options.getImportList().split(",");
    List<PCollection<Mutation>> obsMutationList = new ArrayList<>();
    List<PCollection<Mutation>> edgeMutationList = new ArrayList<>();
    List<PCollection<Mutation>> nodeMutationList = new ArrayList<>();
    for (String importName : imports) {
      String path = getGraphPath(options.getProjectId(), options.getStorageBucketId(), importName);
      LOGGER.info("Import {} graph path {}", importName, path);
      // Read schema mcf files and combine MCF nodes, and convert to spanner mutations (Node/Edge).
      PCollection<McfGraph> schemaNodes =
          PipelineUtils.readMcfFiles(path + "/" + INSTANCE_MCF_NODES + MCF_SUFFIX, pipeline);
      PCollection<McfGraph> combinedGraph = PipelineUtils.combineGraphNodes(schemaNodes);
      PCollection<Mutation> nodeMutations =
          GraphReader.graphToNodes(combinedGraph, spannerClient, counter)
              .apply("ExtractNodeMutations", Values.create());
      PCollection<Mutation> edgeMutations =
          GraphReader.graphToEdges(combinedGraph, spannerClient, counter)
              .apply("ExtractEdgeMutations", Values.create());
      nodeMutationList.add(nodeMutations);
      edgeMutationList.add(edgeMutations);

      // Read observation mcf files, build optimized graph, and convert to spanner mutations
      // (Observation).
      PCollection<McfGraph> observationNodes =
          PipelineUtils.readMcfFiles(path + "/" + TABLE_MCF_NODES + MCF_SUFFIX, pipeline);
      PCollection<McfOptimizedGraph> optimizedGraph =
          PipelineUtils.buildOptimizedMcfGraph(observationNodes);
      // PCollection<McfOptimizedGraph> optimizedGraph =
      //     PipelineUtils.readOptimizedMcfGraph(path, pipeline);
      PCollection<Mutation> observationMutations =
          GraphReader.graphToObservations(optimizedGraph, spannerClient)
              .apply("ExtractObsMutations", Values.create());
      obsMutationList.add(observationMutations);
    }
    // Write the mutations to spanner.
    PCollection<Mutation> obsMutations =
        PCollectionList.of(obsMutationList).apply(Flatten.pCollections());
    obsMutations.apply("WriteObsToSpanner", spannerClient.getWriteTransform());
    PCollection<Mutation> nodeMutations =
        PCollectionList.of(nodeMutationList).apply(Flatten.pCollections());
    SpannerWriteResult result =
        nodeMutations.apply("WriteNodesToSpanner", spannerClient.getWriteTransform());
    PCollection<Mutation> edgeMutations =
        PCollectionList.of(edgeMutationList).apply(Flatten.pCollections());
    // Wait for node mutations to complete before writing edge mutations.
    edgeMutations
        .apply(Wait.on(result.getOutput()))
        .apply("WriteEdgesToSpanner", spannerClient.getWriteTransform());

    pipeline.run();
  }
}
