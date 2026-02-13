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
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.util.GraphReader;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphIngestionPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphIngestionPipeline.class);
  private static final Counter nodeInvalidTypeCounter =
      Metrics.counter(GraphIngestionPipeline.class, "mcf_nodes_without_type");
  private static final Counter nodeCounter =
      Metrics.counter(GraphIngestionPipeline.class, "graph_node_count");
  private static final Counter edgeCounter =
      Metrics.counter(GraphIngestionPipeline.class, "graph_edge_count");
  private static final Counter obsCounter =
      Metrics.counter(GraphIngestionPipeline.class, "graph_observation_count");

  // List of imports that require node combination.
  private static final List<String> IMPORTS_TO_COMBINE = List.of("Schema", "Place");

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
    buildPipeline(pipeline, options, spannerClient);
    pipeline.run();
  }

  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    LOGGER.info("Running import pipeline for imports: {}", options.getImportList());

    // Initialize lists to hold mutations from all imports.
    List<PCollection<Mutation>> deleteMutationList = new ArrayList<>();
    List<PCollection<Mutation>> obsMutationList = new ArrayList<>();
    List<PCollection<Mutation>> edgeMutationList = new ArrayList<>();
    List<PCollection<Mutation>> nodeMutationList = new ArrayList<>();

    // Parse the input import list JSON.
    JsonElement jsonElement = JsonParser.parseString(options.getImportList());
    JsonArray jsonArray = jsonElement.getAsJsonArray();
    if (jsonArray.isEmpty()) {
      LOGGER.error("Empty import input json: {}", jsonArray.toString());
      return;
    }

    // Iterate through each import configuration and process it.
    for (JsonElement element : jsonArray) {
      JsonElement importElement = element.getAsJsonObject().get("importName");
      JsonElement pathElement = element.getAsJsonObject().get("graphPath");

      if (isJsonNullOrEmpty(importElement) || isJsonNullOrEmpty(pathElement)) {
        LOGGER.error("Invalid import input json: {}", element.toString());
        continue;
      }
      String importName = importElement.getAsString();
      String graphPath = pathElement.getAsString();

      // Process the individual import.
      processImport(
          pipeline,
          spannerClient,
          importName,
          graphPath,
          deleteMutationList,
          nodeMutationList,
          edgeMutationList,
          obsMutationList);
    }
    // Finally, aggregate all collected mutations and write them to Spanner.
    // 1. Process Deletes:
    // First, execute all delete mutations to clear old data for the imports.
    SpannerWriteResult deleted =
        spannerClient.writeMutations(pipeline, "Delete", deleteMutationList, null);

    // 2. Process Observations:
    // Write observation mutations after deletes are complete.
    spannerClient.writeMutations(pipeline, "Obs", obsMutationList, deleted.getOutput());

    // 3. Process Nodes:
    // Write node mutations after deletes are complete.
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(pipeline, "Nodes", nodeMutationList, deleted.getOutput());

    // 4. Process Edges:
    // Write edge mutations only after node mutations are complete to ensure referential integrity.
    spannerClient.writeMutations(pipeline, "Edges", edgeMutationList, writtenNodes.getOutput());
  }

  /**
   * Processes a single import configuration.
   *
   * @param pipeline The Beam pipeline.
   * @param spannerClient The Spanner client.
   * @param importName The name of the import.
   * @param graphPath The full path to the graph data.
   * @param deleteMutationList List to collect delete mutations.
   * @param nodeMutationList List to collect node mutations.
   * @param edgeMutationList List to collect edge mutations.
   * @param obsMutationList List to collect observation mutations.
   */
  private static void processImport(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      String graphPath,
      List<PCollection<Mutation>> deleteMutationList,
      List<PCollection<Mutation>> nodeMutationList,
      List<PCollection<Mutation>> edgeMutationList,
      List<PCollection<Mutation>> obsMutationList) {
    LOGGER.info("Import: {} Graph path: {}", importName, graphPath);

    String provenance = "dc/base/" + importName;

    // 1. Prepare Deletes:
    // Generate mutations to delete existing data for this import/provenance.
    PCollection<Mutation> deleteMutations =
        GraphReader.getDeleteMutations(importName, provenance, pipeline, spannerClient);
    deleteMutationList.add(deleteMutations);

    // 2. Read and Split Graph:
    // Read the graph data (TFRecord or MCF files) and split into schema and observation nodes.
    PCollection<McfGraph> graph =
        graphPath.contains("tfrecord")
            ? PipelineUtils.readMcfGraph(graphPath, pipeline)
            : PipelineUtils.readMcfFiles(graphPath, pipeline);
    PCollectionTuple graphNodes = PipelineUtils.splitGraph(graph);
    PCollection<McfGraph> observationNodes = graphNodes.get(PipelineUtils.OBSERVATION_NODES_TAG);
    PCollection<McfGraph> schemaNodes = graphNodes.get(PipelineUtils.SCHEMA_NODES_TAG);

    // 3. Process Schema Nodes:
    // Combine schema nodes if required, then convert to Node and Edge mutations.
    PCollection<McfGraph> combinedGraph = schemaNodes;
    if (IMPORTS_TO_COMBINE.contains(importName)) {
      combinedGraph = PipelineUtils.combineGraphNodes(schemaNodes);
    }
    PCollection<Mutation> nodeMutations =
        GraphReader.graphToNodes(combinedGraph, spannerClient, nodeCounter, nodeInvalidTypeCounter)
            .apply("ExtractNodeMutations", Values.create());
    PCollection<Mutation> edgeMutations =
        GraphReader.graphToEdges(combinedGraph, provenance, spannerClient, edgeCounter)
            .apply("ExtractEdgeMutations", Values.create());

    nodeMutationList.add(nodeMutations);
    edgeMutationList.add(edgeMutations);

    // 4. Process Observation Nodes:
    // Build an optimized graph from observation nodes and convert to Observation mutations.
    PCollection<McfOptimizedGraph> optimizedGraph =
        PipelineUtils.buildOptimizedMcfGraph(observationNodes);
    PCollection<Mutation> observationMutations =
        GraphReader.graphToObservations(optimizedGraph, importName, spannerClient, obsCounter)
            .apply("ExtractObsMutations", Values.create());
    obsMutationList.add(observationMutations);
  }
}
