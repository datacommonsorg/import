package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.ingestion.util.GraphReader;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
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

    String isBaseDcEnv = System.getenv("IS_BASE_DC");
    if (isBaseDcEnv != null) {
      options.setIsBaseDc(Boolean.parseBoolean(isBaseDcEnv));
    }

    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId(options.getProjectId())
            .spannerInstanceId(options.getSpannerInstanceId())
            .spannerDatabaseId(options.getSpannerDatabaseId())
            .nodeTableName(options.getSpannerNodeTableName())
            .edgeTableName(options.getSpannerEdgeTableName())
            .timeSeriesTableName("TimeSeries")
            .observationTableName(options.getSpannerObservationTableName())
            .numShards(options.getNumShards())
            .build();

    if (options.getInitializeDatabase()) {
      LOGGER.info("Starting Spanner DDL creation...");
      spannerClient.validateOrInitializeDatabase();
      LOGGER.info("Spanner DDL creation complete.");
    }

    Pipeline pipeline = Pipeline.create(options);
    buildPipeline(pipeline, options, spannerClient);
    pipeline.run().waitUntilFinish();
  }

  public static void buildPipeline(
      Pipeline pipeline, IngestionPipelineOptions options, SpannerClient spannerClient) {
    LOGGER.info("Running import pipeline for imports: {}", options.getImportList());

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
      if (isJsonNullOrEmpty(importElement)) {
        LOGGER.error("Invalid import input json, missing importName: {}", element.toString());
        continue;
      }
      String importName = importElement.getAsString();
      String graphPath = isJsonNullOrEmpty(pathElement) ? null : pathElement.getAsString();

      if (graphPath == null) {
        LOGGER.error("Invalid import input json, missing graphPath: {}", element.toString());
        continue;
      }

      // Process the individual import.
      processImport(pipeline, spannerClient, importName, graphPath, options);
    }
  }

  /**
   * Processes a single import configuration.
   *
   * @param pipeline The Beam pipeline.
   * @param spannerClient The Spanner client.
   * @param importName The name of the import.
   * @param graphPath The full path to the graph data.
   * @param options The ingestion pipeline options.
   */
  private static void processImport(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      String graphPath,
      IngestionPipelineOptions options) {
    LOGGER.info("Import: {} Graph path: {}", importName, graphPath);

    boolean isBaseDc = options.getIsBaseDc();
    String provenance = ProvenanceUtils.getProvenanceDcid(importName, isBaseDc);

    // 1. Prepare Deletes:
    // Generate mutations to delete existing data for this import/provenance.
    // Create a dummy signal if deletes are skipped, so downstream dependencies are satisfied
    // immediately.
    PCollection<Void> deleteObsWait;
    PCollection<Void> deleteEdgesWait;
    if (!options.getSkipDelete()) {
      deleteObsWait =
          spannerClient.deleteDataForImport(
              pipeline, provenance, spannerClient.getTimeSeriesTableName(), "provenance");
      deleteEdgesWait =
          spannerClient.deleteDataForImport(
              pipeline, provenance, spannerClient.getEdgeTableName(), "provenance");
    } else {
      deleteObsWait =
          pipeline.apply(
              "CreateEmptyObsWait-" + importName, Create.empty(TypeDescriptor.of(Void.class)));
      deleteEdgesWait =
          pipeline.apply(
              "CreateEmptyEdgesWait-" + importName, Create.empty(TypeDescriptor.of(Void.class)));
    }

    PCollection<McfGraph> graph;
    switch (PipelineUtils.resolveFormat(graphPath)) {
      case TFRECORD:
        graph = PipelineUtils.readMcfGraph(importName, graphPath, pipeline);
        break;
      case JSONLD:
        graph = PipelineUtils.readJsonLdFiles(importName, graphPath, pipeline);
        break;
      case MCF:
        graph = PipelineUtils.readMcfFiles(importName, graphPath, pipeline);
        break;
      default:
        throw new IllegalArgumentException(
            "Invalid import config: missing graphPath or template/csv paths");
    }

    PCollectionTuple graphNodes = PipelineUtils.splitGraph(importName, graph);
    PCollection<McfGraph> observationNodes = graphNodes.get(PipelineUtils.OBSERVATION_NODES_TAG);
    PCollection<McfGraph> schemaNodes = graphNodes.get(PipelineUtils.SCHEMA_NODES_TAG);

    // 3. Process Schema Nodes:
    // Combine nodes if required.
    PCollection<McfGraph> combinedGraph = schemaNodes;
    if (options.getForceCombineNodes() || IMPORTS_TO_COMBINE.contains(importName)) {
      LOGGER.info(
          ">>> Combining nodes for import: {} (ForceCombine: {})",
          importName,
          options.getForceCombineNodes());
      combinedGraph = PipelineUtils.combineGraphNodes(importName, schemaNodes);
    }

    // Convert all nodes to mutations
    PCollection<Mutation> nodeMutations =
        GraphReader.graphToNodes(
                "NodeMutations-" + importName,
                combinedGraph,
                spannerClient,
                nodeCounter,
                nodeInvalidTypeCounter)
            .apply("ExtractNodeMutations-" + importName, Values.create());
    PCollection<Mutation> edgeMutations =
        GraphReader.graphToEdges(
                "EdgeMutations-" + importName,
                combinedGraph,
                provenance,
                spannerClient,
                edgeCounter)
            .apply("ExtractEdgeMutations-" + importName, Values.create());

    // Write Nodes
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(pipeline, "WriteNodesToSpanner-" + importName, nodeMutations);

    // Write Edges (wait for Nodes write and Edges delete)
    PCollection<Mutation> waitingEdges =
        edgeMutations.apply(
            "EdgesWaitOn-" + importName,
            Wait.on(List.of(writtenNodes.getOutput(), deleteEdgesWait)));
    spannerClient.writeMutations(pipeline, "WriteEdgesToSpanner-" + importName, waitingEdges);

    // Path 1: TimeSeries (Metadata)
    // Extract unique series keys and convert to metadata-only TimeSeries
    PCollection<TimeSeries> uniqueSeries =
        GraphReader.extractUniqueSeries(observationNodes, importName, isBaseDc);
    // Convert unique TimeSeries to TimeSeries mutations
    PCollection<Mutation> tsMutations =
        uniqueSeries.apply(
            "ToTimeSeriesMutations-" + importName,
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(spannerClient::toTimeSeriesMutation));

    // Path 2: Observations (Data Points)
    // Extract all individual data points as Observations (one per date/value)
    PCollection<Observation> obsDataPoints =
        GraphReader.extractObservations(observationNodes, importName, isBaseDc);
    // Convert Observations to Observation mutations
    PCollection<Mutation> childMutations =
        obsDataPoints.apply(
            "ToObservationMutations-" + importName,
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(spannerClient::toObservationMutation));

    // Write TimeSeries (wait for Obs delete)
    PCollection<Mutation> waitingTS =
        tsMutations.apply("TSWaitOn-" + importName, Wait.on(deleteObsWait));
    SpannerWriteResult writtenTS =
        spannerClient.writeMutations(pipeline, "WriteTimeSeriesToSpanner-" + importName, waitingTS);

    // Write Observations (wait for TimeSeries write)
    PCollection<Mutation> waitingChildren =
        childMutations.apply("ChildWaitOn-" + importName, Wait.on(writtenTS.getOutput()));
    spannerClient.writeMutations(
        pipeline, "WriteChildObservationsToSpanner-" + importName, waitingChildren);
  }
}
