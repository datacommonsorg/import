package org.datacommons.ingestion.pipeline;

import com.google.cloud.spanner.Mutation;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.util.List;
import java.util.Objects;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reshuffle;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.data.Observation;
import org.datacommons.ingestion.data.ProvenanceUtils;
import org.datacommons.ingestion.data.TimeSeries;
import org.datacommons.ingestion.pipeline.embedding.DeleteExistingEmbeddingsFn;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingConfig;
import org.datacommons.ingestion.pipeline.embedding.GenerateEmbeddingMutations;
import org.datacommons.ingestion.pipeline.embedding.VertexAiEmbeddingClient;
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

  public static SpannerClient.Builder createBaseSpannerClientBuilder(
      SpannerPipelineOptions options) {
    String isBaseDcEnv = System.getenv("IS_BASE_DC");
    if (isBaseDcEnv != null && !isBaseDcEnv.isEmpty()) {
      options.setIsBaseDc(Boolean.parseBoolean(isBaseDcEnv));
    }

    String emulatorHost = options.getEmulatorHost();
    if (emulatorHost != null && !emulatorHost.isEmpty()) {
      if ("production".equalsIgnoreCase(System.getenv("ENVIRONMENT"))) {
        throw new SecurityException(
            "CRITICAL: Emulator settings detected in production environment!");
      }
    }

    return SpannerClient.builder()
        .gcpProjectId(options.getProjectId())
        .spannerInstanceId(options.getSpannerInstanceId())
        .spannerDatabaseId(options.getSpannerDatabaseId())
        .numShards(options.getNumShards())
        .emulatorHost(emulatorHost);
  }

  public static SpannerClient createSpannerClient(IngestionPipelineOptions options) {
    return createBaseSpannerClientBuilder(options)
        .nodeTableName(options.getSpannerNodeTableName())
        .edgeTableName(options.getSpannerEdgeTableName())
        .timeSeriesTableName(options.getSpannerTimeSeriesTableName())
        .observationTableName(options.getSpannerObservationTableName())
        .build();
  }

  public static VertexAiEmbeddingClient createVertexAiEmbeddingClient(
      IngestionPipelineOptions options) {
    if (!isEmbeddingsEnabled(options)) {
      return null;
    }
    return new VertexAiEmbeddingClient(
        options.getProjectId(), options.as(DataflowPipelineOptions.class).getRegion());
  }

  public static void main(String[] args) {
    IngestionPipelineOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(IngestionPipelineOptions.class);

    // Install a fail-fast uncaught exception handler exclusively for local DirectRunner runs.
    if (isDirectRunner(options)) {
      configureDirectRunnerUncaughtExceptionHandler();
    }

    SpannerClient spannerClient = createSpannerClient(options);
    VertexAiEmbeddingClient embeddingClient = createVertexAiEmbeddingClient(options);
    Pipeline pipeline = Pipeline.create(options);
    buildPipeline(pipeline, options, spannerClient, embeddingClient);
    pipeline.run();
  }

  /**
   * Returns true if the configured Beam pipeline runner is {@code DirectRunner} (used for local
   * testing and emulated environments).
   */
  static boolean isDirectRunner(SpannerPipelineOptions options) {
    Class<?> runnerClass = options.getRunner();
    return runnerClass != null && "DirectRunner".equals(runnerClass.getSimpleName());
  }

  /**
   * Registers a default {@link Thread.UncaughtExceptionHandler} for DirectRunner executions.
   *
   * <p>In local/emulated DirectRunner pipelines, fatal runtime exceptions (such as Spanner
   * schema/mutation constraint violations) terminate the executing pipeline worker thread. However,
   * lingering non-daemon threads in background connection pools (e.g., Spanner session pools and
   * gRPC workers) prevent the JVM from halting. This causes local test containers to hang
   * indefinitely.
   *
   * <p>This handler forces the JVM process to exit immediately with code 1 upon encountering an
   * unhandled exception.
   */
  static void configureDirectRunnerUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(
        (thread, throwable) -> {
          LOGGER.error(
              "Fatal uncaught exception in DirectRunner [thread: {}]: ",
              thread.getName(),
              throwable);
          System.exit(1);
        });
  }

  static boolean isEmbeddingsEnabled(IngestionPipelineOptions options) {
    return options != null && options.getEnableEmbeddings();
  }

  public static void buildPipeline(
      Pipeline pipeline,
      IngestionPipelineOptions options,
      SpannerClient spannerClient,
      VertexAiEmbeddingClient embeddingClient) {
    LOGGER.info("Running import pipeline for imports: {}", options.getImportList());

    // Parse the input import list JSON.
    JsonElement jsonElement = JsonParser.parseString(options.getImportList());
    JsonArray jsonArray = jsonElement.getAsJsonArray();
    if (jsonArray.isEmpty()) {
      LOGGER.error("Empty import input json: {}", jsonArray.toString());
      return;
    }

    EmbeddingConfig embeddingConfig = null;
    if (isEmbeddingsEnabled(options)) {
      Objects.requireNonNull(
          embeddingClient, "embeddingClient must not be null when enableEmbeddings is true");
      embeddingConfig = EmbeddingConfig.load(options.getEmbeddingConfigPath());
      LOGGER.info(
          "Embedding ingestion enabled: config='{}', table='{}', specs={}, batchSize={}",
          options.getEmbeddingConfigPath(),
          options.getEmbeddingTableName(),
          embeddingConfig.specs().size(),
          options.getEmbeddingBatchSize());
    } else {
      LOGGER.info("Embedding ingestion disabled (enableEmbeddings=false).");
    }

    // Iterate through each import configuration and process it.
    for (JsonElement element : jsonArray) {
      JsonElement importElement = element.getAsJsonObject().get("importName");
      JsonElement pathElement =
          element.getAsJsonObject().has("graphPath")
              ? element.getAsJsonObject().get("graphPath")
              : element.getAsJsonObject().get("latestVersion");
      if (isJsonNullOrEmpty(importElement)) {
        LOGGER.error("Invalid import input json, missing importName: {}", element.toString());
        continue;
      }
      String importName = importElement.getAsString();
      String graphPath = isJsonNullOrEmpty(pathElement) ? null : pathElement.getAsString();

      if (graphPath == null) {
        LOGGER.error("Invalid import input json, missing latestVersion: {}", element.toString());
        continue;
      }

      // Process the individual import.
      processImport(
          pipeline,
          spannerClient,
          importName,
          graphPath,
          options,
          embeddingConfig,
          embeddingClient);
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
   * @param embeddingConfig Loaded embedding configuration, or {@code null} when disabled.
   * @param embeddingClient Vertex AI embedding client, or {@code null} when disabled.
   */
  private static void processImport(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      String graphPath,
      IngestionPipelineOptions options,
      EmbeddingConfig embeddingConfig,
      VertexAiEmbeddingClient embeddingClient) {
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
    PCollection<Void> deleteEmbeddingsWait = null;
    if (!options.getSkipDelete()) {
      deleteObsWait =
          spannerClient.deleteDataForImport(
              pipeline,
              provenance,
              spannerClient.getTimeSeriesTableName(),
              "provenance",
              /* waitSignal= */ null);
      if (embeddingConfig != null) {
        // Read Edge subject_ids once per import and delete existing NodeEmbedding rows before
        // running DELETE FROM Edge so Edge rows are not deleted out from under the scan.
        // TODO(b/584694504): Once NodeEmbedding schema includes provenance, delete existing
        // embeddings directly via spannerClient.deleteDataForImport(pipeline, provenance,
        // embeddingTable, "provenance", /* waitSignal= */ null).
        deleteEmbeddingsWait =
            DeleteExistingEmbeddingsFn.deleteExistingEmbeddings(
                pipeline, spannerClient, importName, options.getEmbeddingTableName(), provenance);
      }
      deleteEdgesWait =
          spannerClient.deleteDataForImport(
              pipeline,
              provenance,
              spannerClient.getEdgeTableName(),
              "provenance",
              deleteEmbeddingsWait);
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
          /* embeddingMutations= */ List.of(),
          result.uniqueSeries,
          result.obsDataPoints,
          deleteObsWait,
          deleteEdgesWait,
          deleteEmbeddingsWait);
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
              org.apache.beam.sdk.transforms.ParDo.of(new GraphTransformer(isBaseDc)));
    }

    // Convert transformed graph to a PCollection of NodeSubGraph
    PCollection<NodeSubGraph> nodeSubGraphs =
        GraphReader.extractNodeSubGraphsFromGraph(
            importName, transformedGraph, provenance, nodeInvalidTypeCounter);

    // Extract node, edge, and embedding mutations from nodeSubGraphs
    PCollection<Mutation> nodeMutations =
        GraphReader.nodesToMutations(importName, nodeSubGraphs, spannerClient, nodeCounter);
    PCollection<Mutation> edgeMutations =
        GraphReader.edgesToMutations(importName, nodeSubGraphs, spannerClient, edgeCounter);
    List<PCollection<Mutation>> embeddingMutations =
        embeddingConfig != null
            ? GenerateEmbeddingMutations.buildEmbeddingMutations(
                importName,
                embeddingConfig,
                embeddingClient,
                options.getEmbeddingTableName(),
                options.getEmbeddingBatchSize(),
                nodeSubGraphs)
            : List.of();

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
        embeddingMutations,
        uniqueSeries,
        obsDataPoints,
        deleteObsWait,
        deleteEdgesWait,
        deleteEmbeddingsWait);
  }

  private static void writeToSpanner(
      Pipeline pipeline,
      SpannerClient spannerClient,
      String importName,
      PCollection<Mutation> nodeMutations,
      PCollection<Mutation> edgeMutations,
      List<PCollection<Mutation>> embeddingMutations,
      PCollection<TimeSeries> uniqueSeries,
      PCollection<Observation> obsDataPoints,
      PCollection<Void> deleteObsWait,
      PCollection<Void> deleteEdgesWait,
      PCollection<Void> deleteEmbeddingsWait) {
    // Write Nodes first so downstream Edge and NodeEmbedding writes can wait on parent Node rows.
    SpannerWriteResult writtenNodes =
        spannerClient.writeMutations(pipeline, "WriteNodesToSpanner-" + importName, nodeMutations);

    // Write Edges (wait for Nodes write and Edges delete)
    PCollection<Mutation> waitingEdges =
        edgeMutations.apply(
            "EdgesWaitOn-" + importName,
            Wait.on(List.of(writtenNodes.getOutput(), deleteEdgesWait)));
    spannerClient.writeMutations(pipeline, "WriteEdgesToSpanner-" + importName, waitingEdges);

    // Write NodeEmbeddings (wait for Nodes write and, if enabled, NodeEmbedding pre-delete)
    if (embeddingMutations != null && !embeddingMutations.isEmpty()) {
      PCollection<Mutation> allEmbeddingMutations =
          embeddingMutations.size() == 1
              ? embeddingMutations.get(0)
              : PCollectionList.of(embeddingMutations)
                  .apply("FlattenNodeEmbeddings-" + importName, Flatten.pCollections());
      List<PCollection<?>> embeddingWaits =
          deleteEmbeddingsWait != null
              ? List.of(writtenNodes.getOutput(), deleteEmbeddingsWait)
              : List.of(writtenNodes.getOutput());
      PCollection<Mutation> waitingEmbeddings =
          allEmbeddingMutations.apply(
              "NodeEmbeddingsWaitOn-" + importName, Wait.on(embeddingWaits));
      spannerClient.writeMutations(
          pipeline, "WriteNodeEmbeddingsToSpanner-" + importName, waitingEmbeddings);
    }

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
    PCollection<Mutation> waitingTimeSeries =
        timeSeriesMutations.apply("TimeSeriesWaitOn-" + importName, Wait.on(deleteObsWait));
    SpannerWriteResult writtenTimeSeries =
        spannerClient.writeMutations(
            pipeline, "WriteTimeSeriesToSpanner-" + importName, waitingTimeSeries);

    // Write Observations (wait for TimeSeries write)
    PCollection<Mutation> waitingObservations =
        observationMutations.apply(
            "ObservationWaitOn-" + importName, Wait.on(writtenTimeSeries.getOutput()));
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
    // TFRecord files are not splittable, so each file is read by a single worker. Reshuffle to
    // break fusion and distribute downstream processing and Spanner writes across workers.
    // Random distribution is safe: each record is one complete, unique time series (key and all
    // its observations) and nothing downstream groups records, so each Spanner row is written by
    // exactly one worker.
    PCollection<McfOptimizedGraph> optGraph =
        PipelineUtils.readOptimizedMcfGraph(importName, graphPath, pipeline)
            .apply("ReshuffleTfRecords-" + importName, Reshuffle.viaRandomKey());
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
