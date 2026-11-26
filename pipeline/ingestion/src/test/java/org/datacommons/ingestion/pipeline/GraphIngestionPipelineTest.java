package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Lists;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.datacommons.ingestion.pipeline.embedding.VertexAiEmbeddingClient;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.datacommons.proto.Mcf.McfOptimizedGraph;
import org.datacommons.proto.Mcf.McfStatVarObsSeries;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class GraphIngestionPipelineTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();
  @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

  @Before
  public void setUp() {
    MockSpannerClient.clearCapturedMutations();
  }

  @Test
  public void testBuildPipeline() throws Exception {
    // 1. Setup Test Data (MCF File)
    File mcfFile = tmpFolder.newFile("test.mcf");
    Files.writeString(
        Path.of(mcfFile.getPath()),
        "Node: dcid:geoId/06\n" + "typeOf: dcs:Place\n" + "name: \"California\"\n\n");

    // 2. Setup Options
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    String importListJson =
        String.format(
            "[{\"importName\": \"testImport\", \"graphPath\": \"%s\"}]", mcfFile.getPath());
    options.setImportList(importListJson);
    options.setProjectId("test-project");
    options.setSpannerInstanceId("test-instance");
    options.setSpannerDatabaseId("test-database");

    // 3. Setup Mock SpannerClient
    PCollection<Void> emptySignal =
        pipeline.apply("CreateEmptySignal", Create.empty(TypeDescriptor.of(Void.class)));

    SpannerClient mockSpannerClient =
        new MockSpannerClient(
            emptySignal, MockSpannerClient.createMockWriteResult(pipeline, "Default"));

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, /* vertexAiClient= */ null);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    verifyMutations(MockSpannerClient.getCapturedMutations());
  }

  private void verifyMutations(List<Mutation> capturedMutations) {
    boolean foundNode = false;
    boolean foundNameEdge = false;

    for (Mutation m : capturedMutations) {
      if (m.getTable().equals("Node")
          && m.asMap().get("subject_id").getString().equals("geoId/06")) {
        foundNode = true;
      }

      if (m.getTable().equals("Edge")
          && m.asMap().get("subject_id").getString().equals("geoId/06")
          && m.asMap().get("predicate").getString().equals("name")) {
        foundNameEdge = true;
      }
    }
    assertTrue("Node mutation for geoId/06 not found", foundNode);
    assertTrue("Edge mutation for name not found", foundNameEdge);
  }

  @Test
  public void testBuildPipelineWithForceCombineNodes() throws Exception {
    // 1. Setup Test Data (Two MCF files with same node ID)
    File mcfFile1 = tmpFolder.newFile("test1.mcf");
    Files.writeString(
        Path.of(mcfFile1.getPath()),
        "Node: dcid:geoId/06\n" + "typeOf: dcs:Place\n" + "name: \"California\"\n\n");

    File mcfFile2 = tmpFolder.newFile("test2.mcf");
    Files.writeString(
        Path.of(mcfFile2.getPath()),
        "Node: dcid:geoId/06\n" + "typeOf: dcs:Place\n" + "containedInPlace: dcid:country/USA\n\n");

    // 2. Setup Options
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    // Point to the folder containing both files
    String importListJson =
        String.format(
            "[{\"importName\": \"customImport\", \"graphPath\": \"%s/*\"}]",
            tmpFolder.getRoot().getPath());
    options.setImportList(importListJson);
    options.setProjectId("test-project");
    options.setSpannerInstanceId("test-instance");
    options.setSpannerDatabaseId("test-database");
    options.setForceCombineNodes(true); // Enable the new flag!

    // 3. Setup Mock SpannerClient
    PCollection<Void> emptySignal =
        pipeline.apply("CreateEmptySignal_Combine", Create.empty(TypeDescriptor.of(Void.class)));

    SpannerClient mockSpannerClient =
        new MockSpannerClient(
            emptySignal, MockSpannerClient.createMockWriteResult(pipeline, "Combine"));

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, /* vertexAiClient= */ null);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    // If combined, we should see only 1 Node mutation for geoId/06!
    int nodeMutationCount = 0;
    for (com.google.cloud.spanner.Mutation m : MockSpannerClient.getCapturedMutations()) {
      if (m.getTable().equals("Node")
          && m.asMap().get("subject_id").getString().equals("geoId/06")) {
        nodeMutationCount++;
      }
    }
    assertTrue("Node mutations should be combined into 1", nodeMutationCount == 1);
  }

  @Test
  public void testBuildPipelineWithSkipTransformation() throws Exception {
    // 1. Setup Test Data (MCF File with a StatisticalVariable)
    File mcfFile = tmpFolder.newFile("test_skip_transform.mcf");
    Files.writeString(
        Path.of(mcfFile.getPath()),
        "Node: dcid:testStatVar\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "populationType: dcs:Person\n"
            + "gender: dcs:Female\n\n"); // gender is a constraint property

    // 2. Setup Options
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    String importListJson =
        String.format(
            "[{\"importName\": \"testImport\", \"graphPath\": \"%s\"}]", mcfFile.getPath());
    options.setImportList(importListJson);
    options.setProjectId("test-project");
    options.setSpannerInstanceId("test-instance");
    options.setSpannerDatabaseId("test-database");
    options.setSkipTransformation(true); // Enable skip transformation!

    // 3. Setup Mock SpannerClient
    PCollection<Void> emptySignal =
        pipeline.apply(
            "CreateEmptySignal_SkipTransform", Create.empty(TypeDescriptor.of(Void.class)));

    SpannerClient mockSpannerClient =
        new MockSpannerClient(
            emptySignal, MockSpannerClient.createMockWriteResult(pipeline, "SkipTransform"));

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, /* vertexAiClient= */ null);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    // If transformation is skipped, we should NOT see "constraintProperties" in the mutations.
    // "gender" is NOT in NON_CONSTRAINT_PROPS, so it would normally trigger constraintProperties
    // generation.
    boolean foundConstraintProperties = false;
    for (Mutation m : MockSpannerClient.getCapturedMutations()) {
      if (m.getTable().equals("Edge")
          && m.asMap().get("subject_id").getString().equals("testStatVar")
          && m.asMap().get("predicate").getString().equals("constraintProperties")) {
        foundConstraintProperties = true;
      }
    }
    assertTrue(
        "Should NOT find constraintProperties when transformation is skipped",
        !foundConstraintProperties);
  }

  @Test
  public void testBuildPipelineWithTfRecordInput() throws Exception {
    // 1. Setup Test Data (gzipped TFRecord file with two series and three observations)
    McfOptimizedGraph usa =
        createOptimizedGraph(
            "count_person",
            "country/USA",
            createStatVarObs("2020", 1.0),
            createStatVarObs("2021", 2.0));
    McfOptimizedGraph india =
        createOptimizedGraph("count_person", "country/India", createStatVarObs("2020", 3.0));
    // TFRecordIO appends the ".gz" compression suffix to the output file.
    String tfRecordPrefix = tmpFolder.getRoot().getPath() + "/graph.tfrecord";
    Pipeline writePipeline = Pipeline.create();
    writePipeline
        .apply(Create.of(usa.toByteArray(), india.toByteArray()))
        .apply(
            TFRecordIO.write()
                .to(tfRecordPrefix)
                .withCompression(Compression.GZIP)
                .withoutSharding());
    writePipeline.run().waitUntilFinish();

    // 2. Setup Options
    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    String importListJson =
        String.format("[{\"importName\": \"testImport\", \"graphPath\": \"%s*\"}]", tfRecordPrefix);
    options.setImportList(importListJson);
    options.setProjectId("test-project");
    options.setSpannerInstanceId("test-instance");
    options.setSpannerDatabaseId("test-database");

    // 3. Setup Mock SpannerClient
    PCollection<Void> emptySignal =
        pipeline.apply("CreateEmptySignal_TfRecord", Create.empty(TypeDescriptor.of(Void.class)));
    SpannerClient mockSpannerClient =
        new MockSpannerClient(
            emptySignal, MockSpannerClient.createMockWriteResult(pipeline, "TfRecord"));

    // 4. Build and Run Pipeline
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, /* vertexAiClient= */ null);
    pipeline.run().waitUntilFinish();

    // 5. Assertions: one TimeSeries mutation per record, one Observation mutation per obs.
    List<Mutation> captured = MockSpannerClient.getCapturedMutations();
    long timeSeriesCount = captured.stream().filter(m -> m.getTable().equals("TimeSeries")).count();
    long observationCount =
        captured.stream().filter(m -> m.getTable().equals("Observation")).count();
    assertEquals(2, timeSeriesCount);
    assertEquals(3, observationCount);
  }

  private static McfOptimizedGraph createOptimizedGraph(
      String statVar, String location, McfStatVarObsSeries.StatVarObs... observations) {
    return McfOptimizedGraph.newBuilder()
        .setSvObsSeries(
            McfStatVarObsSeries.newBuilder()
                .setKey(
                    McfStatVarObsSeries.Key.newBuilder()
                        .setVariableMeasured(statVar)
                        .setObservationAbout(location))
                .addAllSvObsList(List.of(observations)))
        .build();
  }

  private static McfStatVarObsSeries.StatVarObs createStatVarObs(String date, double value) {
    return McfStatVarObsSeries.StatVarObs.newBuilder()
        .setDate(date)
        .setNumber(value)
        .setDcid("dc/o/" + date + "_" + value)
        .build();
  }

  @Test
  public void testIsDirectRunner() {
    IngestionPipelineOptions directOptions =
        PipelineOptionsFactory.fromArgs("--runner=DirectRunner").as(IngestionPipelineOptions.class);
    assertTrue(
        "Should return true for DirectRunner",
        GraphIngestionPipeline.isDirectRunner(directOptions));

    IngestionPipelineOptions dataflowOptions =
        PipelineOptionsFactory.fromArgs(
                "--runner=DataflowRunner",
                "--project=test-project",
                "--stagingLocation=gs://test-bucket/staging")
            .as(IngestionPipelineOptions.class);
    assertTrue(
        "Should return false for DataflowRunner",
        !GraphIngestionPipeline.isDirectRunner(dataflowOptions));
  }

  @Test
  public void testConfigureDirectRunnerUncaughtExceptionHandler() {
    Thread.UncaughtExceptionHandler originalHandler = Thread.getDefaultUncaughtExceptionHandler();
    try {
      Thread.setDefaultUncaughtExceptionHandler(null);
      GraphIngestionPipeline.configureDirectRunnerUncaughtExceptionHandler();
      assertTrue(
          "Default uncaught exception handler should be registered",
          Thread.getDefaultUncaughtExceptionHandler() != null);
    } finally {
      Thread.setDefaultUncaughtExceptionHandler(originalHandler);
    }
  }

  @Test
  public void testDirectRunnerSubprocessExitsFastOnFailure() throws Exception {
    String javaHome = System.getProperty("java.home");
    String javaBin = javaHome + File.separator + "bin" + File.separator + "java";
    String classpath = System.getProperty("java.class.path");
    String className = GraphIngestionPipeline.class.getCanonicalName();

    ProcessBuilder builder =
        new ProcessBuilder(
            javaBin,
            "-cp",
            classpath,
            className,
            "--runner=DirectRunner",
            "--importList=[{\"importName\": \"invalidImport\", \"graphPath\": \"/invalid/nonexistent/*\"}]",
            "--projectId=test-project",
            "--spannerInstanceId=test-instance",
            "--spannerDatabaseId=test-db");

    builder.inheritIO();
    Process process = builder.start();
    boolean finished = false;
    try {
      finished = process.waitFor(30, java.util.concurrent.TimeUnit.SECONDS);
    } finally {
      if (!finished) {
        process.destroyForcibly();
      }
    }

    assertTrue("Process should terminate within 30 seconds without hanging", finished);
    assertTrue("Process should exit with non-zero exit code", process.exitValue() != 0);
  }

  @Test
  public void testIsEmbeddingsEnabled() {
    assertFalse(GraphIngestionPipeline.isEmbeddingsEnabled(null));

    IngestionPipelineOptions defaultOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    assertFalse(GraphIngestionPipeline.isEmbeddingsEnabled(defaultOpts));

    defaultOpts.setEnableEmbeddings(true);
    assertTrue(GraphIngestionPipeline.isEmbeddingsEnabled(defaultOpts));
  }

  @Test
  public void testCreateVertexAiEmbeddingClient_conditionalOnEnableEmbeddings() {
    IngestionPipelineOptions disabledOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    disabledOpts.setProjectId("spanner-project");
    assertNull(GraphIngestionPipeline.createVertexAiEmbeddingClient(disabledOpts));

    IngestionPipelineOptions blankRegionOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    blankRegionOpts.setProjectId("spanner-project");
    blankRegionOpts.as(DataflowPipelineOptions.class).setRegion("  ");
    blankRegionOpts.setEnableEmbeddings(true);
    assertThrows(
        IllegalArgumentException.class,
        () -> GraphIngestionPipeline.createVertexAiEmbeddingClient(blankRegionOpts));

    IngestionPipelineOptions customRegionOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    customRegionOpts.setProjectId("spanner-project");
    customRegionOpts.as(DataflowPipelineOptions.class).setRegion("us-east1");
    customRegionOpts.setEnableEmbeddings(true);
    VertexAiEmbeddingClient customClient =
        GraphIngestionPipeline.createVertexAiEmbeddingClient(customRegionOpts);
    assertNotNull(customClient);
    assertEquals("spanner-project", customClient.getProjectId());
    assertEquals("us-east1", customClient.getLocation());

    IngestionPipelineOptions parsedArgsOpts =
        PipelineOptionsFactory.fromArgs(
                "--projectId=spanner-project", "--region=europe-west1", "--enableEmbeddings=true")
            .withValidation()
            .as(IngestionPipelineOptions.class);
    VertexAiEmbeddingClient parsedClient =
        GraphIngestionPipeline.createVertexAiEmbeddingClient(parsedArgsOpts);
    assertNotNull(parsedClient);
    assertEquals("spanner-project", parsedClient.getProjectId());
    assertEquals("europe-west1", parsedClient.getLocation());
  }

  @Test
  public void testBuildPipeline_embeddingsEnabled_generatesAndWritesEmbeddings() throws Exception {
    MockVertexAIClient.clearCapturedCalls();
    File mcfFile = tmpFolder.newFile("embed.mcf");
    Files.writeString(
        Path.of(mcfFile.getPath()),
        "Node: dcid:Count_Person\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "name: \"Total Population\"\n\n"
            + "Node: dcid:Farm\n"
            + "typeOf: dcs:Class\n"
            + "name: \"Farm\"\n\n"
            + "Node: dcid:geoId/06\n"
            + "typeOf: dcs:Place\n"
            + "name: \"California\"\n\n");

    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList(
        String.format(
            "[{\"importName\": \"testImport\", \"graphPath\": \"%s\"}]", mcfFile.getPath()));
    options.setProjectId("test-project");
    options.setEnableEmbeddings(true);
    options.setSkipDelete(true);
    options.setSkipTransformation(true);

    SpannerClient mockSpannerClient =
        new MockSpannerClient(MockSpannerClient.createMockWriteResult(pipeline, "Embed"));
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, new MockVertexAIClient(List.of(0.5, -0.5)));
    pipeline.run().waitUntilFinish();

    Map<String, Mutation> embUpsertsByLabel =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(
                m ->
                    m.getTable().equals("NodeEmbedding")
                        && m.getOperation() == Mutation.Op.INSERT_OR_UPDATE)
            .collect(Collectors.toMap(m -> m.asMap().get("embedding_label").getString(), m -> m));
    assertEquals(Set.of("base_text_embedding", "type_embedding"), embUpsertsByLabel.keySet());

    Mutation statVarEmb = embUpsertsByLabel.get("base_text_embedding");
    assertEquals("Count_Person", statVarEmb.asMap().get("subject_id").getString());
    assertEquals(
        "{\"name\":\"Total Population\",\"title\":\"Count_Person\"}",
        statVarEmb.asMap().get("embedding_content").getString());
    assertEquals(List.of(0.5, -0.5), statVarEmb.asMap().get("embeddings").getFloat64Array());

    Mutation classEmb = embUpsertsByLabel.get("type_embedding");
    assertEquals("Farm", classEmb.asMap().get("subject_id").getString());
    assertEquals("{\"name\":\"Farm\"}", classEmb.asMap().get("embedding_content").getString());
  }

  @Test
  public void testBuildPipeline_embeddingsEnabled_multiImportUsesUniqueTransformNames()
      throws Exception {
    MockVertexAIClient.clearCapturedCalls();
    File importA = tmpFolder.newFile("importA.mcf");
    Files.writeString(
        Path.of(importA.getPath()),
        "Node: dcid:dc/topic/Health\n" + "typeOf: dcs:Topic\n" + "name: \"Health\"\n\n");
    File importB = tmpFolder.newFile("importB.mcf");
    Files.writeString(
        Path.of(importB.getPath()),
        "Node: dcid:dc/topic/Economy\n" + "typeOf: dcs:Topic\n" + "name: \"Economy\"\n\n");

    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList(
        String.format(
            "[{\"importName\": \"importA\", \"graphPath\": \"%s\"},"
                + "{\"importName\": \"importB\", \"graphPath\": \"%s\"}]",
            importA.getPath(), importB.getPath()));
    options.setProjectId("test-project");
    options.setEnableEmbeddings(true);
    options.setSkipDelete(true);
    options.setSkipTransformation(true);

    SpannerClient mockSpannerClient =
        new MockSpannerClient(MockSpannerClient.createMockWriteResult(pipeline, "Multi"));
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, new MockVertexAIClient(List.of(0.5, -0.5)));
    pipeline.run().waitUntilFinish();

    Map<String, String> embContentBySubject =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(
                m ->
                    m.getTable().equals("NodeEmbedding")
                        && m.getOperation() == Mutation.Op.INSERT_OR_UPDATE)
            .collect(
                Collectors.toMap(
                    m -> m.asMap().get("subject_id").getString(),
                    m -> m.asMap().get("embedding_content").getString()));
    assertEquals(
        Map.of(
            "dc/topic/Health",
            "{\"name\":\"Health\",\"title\":\"dc/topic/Health\"}",
            "dc/topic/Economy",
            "{\"name\":\"Economy\",\"title\":\"dc/topic/Economy\"}"),
        embContentBySubject);
  }

  @Test
  public void testBuildPipeline_embeddingsEnabled_readsEdgeSnapshotAtCurrentTimestamp()
      throws Exception {
    MockVertexAIClient.clearCapturedCalls();
    File mcfFile = tmpFolder.newFile("embed_delete.mcf");
    Files.writeString(
        Path.of(mcfFile.getPath()),
        "Node: dcid:Count_Person\n"
            + "typeOf: dcs:StatisticalVariable\n"
            + "name: \"Total Population\"\n\n"
            + "Node: dcid:Farm\n"
            + "typeOf: dcs:Class\n"
            + "name: \"Farm\"\n\n");

    IngestionPipelineOptions options = PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    options.setImportList(
        String.format(
            "[{\"importName\": \"testImport\", \"graphPath\": \"%s\"}]", mcfFile.getPath()));
    options.setProjectId("test-project");
    options.setEnableEmbeddings(true);
    options.setSkipDelete(false);
    options.setSkipTransformation(true);

    Timestamp expectedReadTimestamp = Timestamp.ofTimeSecondsAndNanos(1700000123L, 456000000);
    SpannerClient mockSpannerClient =
        new MockSpannerClient(
            Map.of("dc/base/testImport", List.of("OldNode", "Count_Person")),
            expectedReadTimestamp);
    GraphIngestionPipeline.buildPipeline(
        pipeline, options, mockSpannerClient, new MockVertexAIClient(List.of(0.5, -0.5)));
    pipeline.run().waitUntilFinish();

    // Edge should be queried once per import pinned to the pre-delete snapshot timestamp.
    assertEquals(List.of("dc/base/testImport"), MockSpannerClient.getCapturedEdgeReads());
    assertEquals(List.of(expectedReadTimestamp), MockSpannerClient.getCapturedEdgeReadTimestamps());

    Set<KeyRange> deletedRanges =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(
                m -> m.getTable().equals("NodeEmbedding") && m.getOperation() == Mutation.Op.DELETE)
            .flatMap(m -> Lists.newArrayList(m.getKeySet().getRanges()).stream())
            .collect(Collectors.toSet());
    assertEquals(
        Set.of(KeyRange.prefix(Key.of("OldNode")), KeyRange.prefix(Key.of("Count_Person"))),
        deletedRanges);

    List<String> stageEvents = MockSpannerClient.getCapturedStageEvents();
    int nodeWriteIdx = stageEvents.lastIndexOf("WriteNodesToSpanner-testImport");
    int embDelIdx = stageEvents.lastIndexOf("WriteDeletedEmbeddings-testImport");
    int edgeDelIdx = stageEvents.indexOf("ExecuteDeleteEdge-testImport");
    int edgeWriteIdx = stageEvents.indexOf("WriteEdgesToSpanner-testImport");
    int embWriteIdx = stageEvents.indexOf("WriteNodeEmbeddingsToSpanner-testImport");

    assertTrue("Missing Node write event", nodeWriteIdx >= 0);
    assertTrue("Missing NodeEmbedding delete event", embDelIdx >= 0);
    assertTrue("Missing Edge delete event", edgeDelIdx >= 0);
    assertTrue("Missing NodeEmbedding write event", embWriteIdx >= 0);
    assertTrue("Edge write must wait for Edge delete", edgeDelIdx < edgeWriteIdx);
    assertTrue("NodeEmbedding write must wait for Node write", nodeWriteIdx < embWriteIdx);
    assertTrue("NodeEmbedding write must wait for NodeEmbedding delete", embDelIdx < embWriteIdx);

    Set<String> upsertedLabels =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(
                m ->
                    m.getTable().equals("NodeEmbedding")
                        && m.getOperation() == Mutation.Op.INSERT_OR_UPDATE)
            .map(m -> m.asMap().get("embedding_label").getString())
            .collect(Collectors.toSet());
    assertEquals(Set.of("base_text_embedding", "type_embedding"), upsertedLabels);
  }
}
