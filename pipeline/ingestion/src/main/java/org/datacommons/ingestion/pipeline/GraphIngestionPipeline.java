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
import org.datacommons.ingestion.util.GraphTransformer;
import org.datacommons.ingestion.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphIngestionPipeline {
  private static final Logger LOGGER = LoggerFactory.getLogger(GraphIngestionPipeline.class);
  private static final Counter nodeInvalidTypeCounter =
      Metrics.counter(GraphIngestionPipeline.class, "mcf_nodes_without_type");

  // List of imports that require node combination.
  private static final List<String> IMPORTS_TO_COMBINE = List.of("Schema", "Place", "Provenance");

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

    String emulatorHost = options.getEmulatorHost();
    if (emulatorHost != null && !emulatorHost.isEmpty()) {
      if ("production".equalsIgnoreCase(System.getenv("ENVIRONMENT"))) {
        throw new SecurityException(
            "CRITICAL: Emulator settings detected in production environment!");
      }
    }

    SpannerClient spannerClient =
        SpannerClient.builder()
            .gcpProjectId(options.getProjectId())
            .spannerInstanceId(options.getSpannerInstanceId())
            .spannerDatabaseId(options.getSpannerDatabaseId())
            .nodeTableName(options.getSpannerNodeTableName())
            .edgeTableName(options.getSpannerEdgeTableName())
            .timeSeriesTableName(options.getSpannerTimeSeriesTableName())
            .observationTableName(options.getSpannerObservationTableName())
            .numShards(options.getNumShards())
            .emulatorHost(emulatorHost)
            .build();

    Pipeline pipeline = Pipeline.create(options);
    buildPipeline(pipeline, options, spannerClient);
    pipeline.run();
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

    String shortName =
        importName.contains(":")
            ? importName.substring(importName.lastIndexOf(':') + 1)
            : importName;

    Counter nodeCounter = Metrics.counter(GraphIngestionPipeline.class, "node_count:" + shortName);
    Counter edgeCounter = Metrics.counter(GraphIngestionPipeline.class, "edge_count:" + shortName);
    Counter obsCounter =
        Metrics.counter(GraphIngestionPipeline.class, "observation_count:" + shortName);
    Counter timeSeriesCounter =
        Metrics.counter(GraphIngestionPipeline.class, "timeseries_count:" + shortName);

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

    PipelineUtils.InputFormat format = PipelineUtils.resolveFormat(graphPath);
    if (format == PipelineUtils.InputFormat.TFRECORD) {
      TfRecordProcessingResult result =
          processTfRecordImport(
              pipeline, importName, graphPath, isBaseDc, obsCounter, timeSeriesCounter);
      writeToSpanner(
          pipeline,
          spannerClient,
          importName,
          result.nodeMutations,
          result.edgeMutations,
          result.uniqueSeries,
          result.obsDataPoints,
          deleteObsWait,
          deleteEdgesWait,
          options);
      return;
    }

    PCollection<McfGraph> graph;
    if (format == PipelineUtils.InputFormat.JSONLD) {
      graph = PipelineUtils.readJsonLdFiles(importName, graphPath, pipeline);
    } else {
      graph = PipelineUtils.readMcfFiles(importName, graphPath, pipeline);
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

    // Transform schema nodes (handle quantities, etc.)
    PCollection<McfGraph> transformedGraph;
    if (options.getSkipTransformation()) {
      LOGGER.info("Skipping transformation step for import: {}", importName);
      transformedGraph = combinedGraph;
    } else {
      transformedGraph =
          combinedGraph.apply(
              "TransformNodes-" + importName,
              org.apache.beam.sdk.transforms.ParDo.of(new GraphTransformer()));
    }

    // Convert all nodes to mutations
    PCollection<Mutation> nodeMutations =
        GraphReader.graphToNodes(
                "NodeMutations-" + importName,
                transformedGraph,
                spannerClient,
                nodeCounter,
                nodeInvalidTypeCounter)
            .apply("ExtractNodeMutations-" + importName, Values.create());
    PCollection<Mutation> edgeMutations =
        GraphReader.graphToEdges(
                "EdgeMutations-" + importName,
                transformedGraph,
                provenance,
                spannerClient,
                edgeCounter)
            .apply("ExtractEdgeMutations-" + importName, Values.create());

    PCollection<TimeSeries> uniqueSeries =
        GraphReader.extractUniqueSeries(observationNodes, importName, isBaseDc, timeSeriesCounter);
    PCollection<Observation> obsDataPoints =
        GraphReader.extractObservations(observationNodes, importName, isBaseDc, obsCounter);

    writeToSpanner(
        pipeline,
        spannerClient,
        importName,
        nodeMutations,
        edgeMutations,
        uniqueSeries,
        obsDataPoints,
        deleteObsWait,
        deleteEdgesWait,
        options);
  }

  private static void writeToSpanner(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      PCollection<Mutation> nodeMutations,
      PCollection<Mutation> edgeMutations,
      PCollection<TimeSeries> uniqueSeries,
      PCollection<Observation> obsDataPoints,
      PCollection<Void> deleteObsWait,
      PCollection<Void> deleteEdgesWait,
      IngestionPipelineOptions options) {
    // Write Nodes
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(pipeline, "WriteNodesToSpanner-" + importName, nodeMutations);

    // Write Edges (wait for Nodes write and Edges delete)
    PCollection<Mutation> waitingEdges = edgeMutations;
    if (!options.getSkipWait()) {
      waitingEdges =
          edgeMutations.apply(
              "EdgesWaitOn-" + importName,
              Wait.on(List.of(writtenNodes.getOutput(), deleteEdgesWait)));
    }
    spannerClient.writeMutations(pipeline, "WriteEdgesToSpanner-" + importName, waitingEdges);

    // Convert unique TimeSeries to TimeSeries mutations
    PCollection<Mutation> timeSeriesMutations =
        uniqueSeries.apply(
            "ToTimeSeriesMutations-" + importName,
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(spannerClient::toTimeSeriesMutation));

    // Convert Observations to Observation mutations
    PCollection<Mutation> observationMutations =
        obsDataPoints.apply(
            "ToObservationMutations-" + importName,
            MapElements.into(TypeDescriptor.of(Mutation.class))
                .via(spannerClient::toObservationMutation));

    // Write TimeSeries (wait for Obs delete)
    PCollection<Mutation> waitingTimeSeries = timeSeriesMutations;
    if (!options.getSkipWait()) {
      waitingTimeSeries =
          timeSeriesMutations.apply("TimeSeriesWaitOn-" + importName, Wait.on(deleteObsWait));
    }
    SpannerWriteResult writtenTimeSeries =
        spannerClient.writeMutations(
            pipeline, "WriteTimeSeriesToSpanner-" + importName, waitingTimeSeries);

    // Write Observations (wait for TimeSeries write)
    PCollection<Mutation> waitingObservations = observationMutations;
    if (!options.getSkipWait()) {
      waitingObservations =
          observationMutations.apply(
              "ObservationWaitOn-" + importName, Wait.on(writtenTimeSeries.getOutput()));
    }
    spannerClient.writeMutations(
        pipeline, "WriteObservationsToSpanner-" + importName, waitingObservations);
  }

  private static class TfRecordProcessingResult {
    final PCollection<TimeSeries> uniqueSeries;
    final PCollection<Observation> obsDataPoints;
    final PCollection<Mutation> nodeMutations;
    final PCollection<Mutation> edgeMutations;

    TfRecordProcessingResult(
        PCollection<TimeSeries> uniqueSeries,
        PCollection<Observation> obsDataPoints,
        PCollection<Mutation> nodeMutations,
        PCollection<Mutation> edgeMutations) {
      this.uniqueSeries = uniqueSeries;
      this.obsDataPoints = obsDataPoints;
      this.nodeMutations = nodeMutations;
      this.edgeMutations = edgeMutations;
    }
  }

  private static TfRecordProcessingResult processTfRecordImport(
      Pipeline pipeline,
      String importName,
      String graphPath,
      boolean isBaseDc,
      Counter obsCounter,
      Counter timeSeriesCounter) {
    PCollection<McfOptimizedGraph> optGraph =
        PipelineUtils.readOptimizedMcfGraph(importName, graphPath, pipeline);
    PCollection<TimeSeries> uniqueSeries =
        GraphReader.extractSeriesFromOptimized(optGraph, importName, isBaseDc, timeSeriesCounter);
    PCollection<Observation> obsDataPoints =
        GraphReader.extractObservationsFromOptimized(optGraph, importName, isBaseDc, obsCounter);
    PCollection<Mutation> nodeMutations =
        pipeline.apply(
            "EmptyNodeMutations-" + importName, Create.empty(TypeDescriptor.of(Mutation.class)));
    PCollection<Mutation> edgeMutations =
        pipeline.apply(
            "EmptyEdgeMutations-" + importName, Create.empty(TypeDescriptor.of(Mutation.class)));
    return new TfRecordProcessingResult(uniqueSeries, obsDataPoints, nodeMutations, edgeMutations);
  }
}
