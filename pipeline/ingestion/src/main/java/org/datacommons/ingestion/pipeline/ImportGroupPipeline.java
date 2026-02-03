package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
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
import org.apache.beam.sdk.values.PCollectionTuple;
import org.datacommons.ingestion.data.GraphReader;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ImportGroupPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(ImportGroupPipeline.class);
  private static final Counter nodeInvalidTypeCounter =
      Metrics.counter(ImportGroupPipeline.class, "mcf_nodes_without_type");
  private static final Counter nodeCounter =
      Metrics.counter(ImportGroupPipeline.class, "graph_node_count");
  private static final Counter edgeCounter =
      Metrics.counter(ImportGroupPipeline.class, "graph_edge_count");
  private static final Counter obsCounter =
      Metrics.counter(ImportGroupPipeline.class, "graph_observation_count");

  private static boolean isJsonNullOrEmpty(JsonElement element) {
    return element == null || element.getAsString().isEmpty();
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

    List<PCollection<Mutation>> deleteMutationList = new ArrayList<>();
    List<PCollection<Mutation>> obsMutationList = new ArrayList<>();
    List<PCollection<Mutation>> edgeMutationList = new ArrayList<>();
    List<PCollection<Mutation>> nodeMutationList = new ArrayList<>();

    JsonElement jsonElement = JsonParser.parseString(options.getImportList());
    JsonArray jsonArray = jsonElement.getAsJsonArray();
    if (jsonArray.isEmpty()) {
      LOGGER.error("Empty import input json: {}", jsonArray.toString());
      return;
    }

    for (JsonElement element : jsonArray) {
      JsonElement importElement = element.getAsJsonObject().get("importName");
      JsonElement versionElement = element.getAsJsonObject().get("latestVersion");
      JsonElement pathElement = element.getAsJsonObject().get("graphPath");

      if (isJsonNullOrEmpty(importElement)
          || isJsonNullOrEmpty(pathElement)
          || isJsonNullOrEmpty(versionElement)) {
        LOGGER.error("Invalid import input json: {}", element.toString());
        continue;
      }
      String importName = importElement.getAsString();
      String latestVersion = versionElement.getAsString();
      LOGGER.info("Import: {} Latest version: {}", importName, latestVersion);

      // Populate provenance node/edges.
      String provenance = "dc/base/" + importName;
      PCollection<McfGraph> provenanceMcf =
          GraphReader.getProvenanceMcf(
              options.getStorageBucketId(), importName, latestVersion, pipeline);

      PCollection<Mutation> deleteMutations =
          GraphReader.getDeleteMutations(importName, provenance, pipeline, spannerClient);
      deleteMutationList.add(deleteMutations);
      // Read schema mcf files and combine MCF nodes, and convert to spanner mutations (Node/Edge).
      String graphPath =
          latestVersion.replaceAll("/+$", "")
              + "/"
              + pathElement.getAsString().replaceAll("^/+", "");
      PCollection<McfGraph> nodes =
          pathElement.getAsString().contains("tfrecord")
              ? PipelineUtils.readMcfGraph(graphPath, pipeline)
              : PipelineUtils.readMcfFiles(graphPath, pipeline);
      PCollectionTuple graphNodes = PipelineUtils.splitGraph(nodes);
      PCollection<McfGraph> observationNodes = graphNodes.get(PipelineUtils.OBSERVATION_NODES_TAG);
      PCollection<McfGraph> schemaNodes = graphNodes.get(PipelineUtils.SCHEMA_NODES_TAG);
      PCollection<McfGraph> schemaMcf =
          PCollectionList.of(schemaNodes)
              .and(provenanceMcf)
              .apply("FlattenSchema", Flatten.pCollections());

      PCollection<Mutation> edgeMutations =
          GraphReader.graphToEdges(schemaMcf, provenance, spannerClient, edgeCounter)
              .apply("ExtractEdgeMutations", Values.create());

      PCollection<Mutation> nodeMutations =
          GraphReader.graphToNodes(schemaMcf, spannerClient, nodeCounter, nodeInvalidTypeCounter)
              .apply("ExtractEdgeMutations", Values.create());

      nodeMutationList.add(nodeMutations);
      edgeMutationList.add(edgeMutations);
      // Read observation mcf files, build optimized graph, and convert to spanner mutations
      // (Observation).
      PCollection<McfOptimizedGraph> optimizedGraph =
          PipelineUtils.buildOptimizedMcfGraph(observationNodes);
      PCollection<Mutation> observationMutations =
          GraphReader.graphToObservations(optimizedGraph, importName, spannerClient, obsCounter)
              .apply("ExtractObsMutations", Values.create());
      obsMutationList.add(observationMutations);
    }
    PCollection<Mutation> deleteMutations =
        PCollectionList.of(deleteMutationList)
            .apply("FlattenDeleteMutations", Flatten.pCollections());
    SpannerWriteResult deleted =
        deleteMutations.apply("DeleteImportsFromSpanner", spannerClient.getWriteTransform());
    // Write the mutations to spanner.
    PCollection<Mutation> obsMutations =
        PCollectionList.of(obsMutationList).apply("FlattenObsMutations", Flatten.pCollections());
    PCollection<Mutation> obs = obsMutations.apply("WaitOnDelete", Wait.on(deleted.getOutput()));
    obs.apply("WriteObsToSpanner", spannerClient.getWriteTransform());
    PCollection<Mutation> nodeMutations =
        PCollectionList.of(nodeMutationList).apply("FlattenNodeMutations", Flatten.pCollections());
    PCollection<Mutation> nodes = nodeMutations.apply("WaitOnDelete", Wait.on(deleted.getOutput()));
    SpannerWriteResult result =
        nodes.apply("WriteNodesToSpanner", spannerClient.getWriteTransform());
    PCollection<Mutation> edgeMutations =
        PCollectionList.of(edgeMutationList).apply("FlattenEdgeMutations", Flatten.pCollections());
    // Wait for node mutations to complete before writing edge mutations.
    PCollection<Mutation> edges = edgeMutations.apply("WaitOnNodes", Wait.on(result.getOutput()));
    edges.apply("WriteEdgesToSpanner", spannerClient.getWriteTransform());

    pipeline.run();
  }
}
