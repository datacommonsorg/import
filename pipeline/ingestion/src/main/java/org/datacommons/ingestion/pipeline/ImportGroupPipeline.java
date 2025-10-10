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
  private static final String GCS_PREFIX = "gs://";
  private static final String MCF_SUFFIX = "/*.mcf";
  private static final Counter counter =
      Metrics.counter(ImportGroupPipeline.class, "mcf_nodes_without_type");

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

    List<PCollection<Mutation>> obsMutationList = new ArrayList<>();
    List<PCollection<Mutation>> edgeMutationList = new ArrayList<>();
    List<PCollection<Mutation>> nodeMutationList = new ArrayList<>();

    JsonElement jsonElement = JsonParser.parseString(options.getImportList());
    JsonArray jsonArray = jsonElement.getAsJsonArray();
    if (jsonArray.isEmpty()) {
      LOGGER.error("Empty import input json: {}", jsonArray.getAsString());
      return;
    }

    for (JsonElement element : jsonArray) {
      JsonElement importName = element.getAsJsonObject().get("importName");
      JsonElement path = element.getAsJsonObject().get("latestVersion");
      if (importName == null
          || path == null
          || importName.getAsString().isEmpty()
          || path.getAsString().isEmpty()) {
        LOGGER.error("Invalid import input json: {}", element.getAsString());
        continue;
      }
      LOGGER.info("Import {} graph path {}", importName.getAsString(), path.getAsString());
      // Read schema mcf files and combine MCF nodes, and convert to spanner mutations (Node/Edge).
      PCollection<McfGraph> nodes =
          PipelineUtils.readMcfFiles(GCS_PREFIX + path.getAsString() + MCF_SUFFIX, pipeline);
      PCollectionTuple graphNodes = PipelineUtils.splitGraph(nodes);
      PCollection<McfGraph> observationNodes = graphNodes.get(PipelineUtils.OBSERVATION_NODES_TAG);
      PCollection<McfGraph> schemaNodes = graphNodes.get(PipelineUtils.SCHEMA_NODES_TAG);
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
      PCollection<McfOptimizedGraph> optimizedGraph =
          PipelineUtils.buildOptimizedMcfGraph(observationNodes);
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
