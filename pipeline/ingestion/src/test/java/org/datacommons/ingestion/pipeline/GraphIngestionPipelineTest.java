package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.spanner.Mutation;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.TFRecordIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
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

    // Mock Write Result
    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteSignal", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);

    SpannerClient mockSpannerClient = new MockSpannerClient(emptySignal, mockWriteResult);

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient, null);

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

    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteSignal_Combine", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);

    SpannerClient mockSpannerClient = new MockSpannerClient(emptySignal, mockWriteResult);

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient, null);

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

    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply(
            "CreateWriteSignal_SkipTransform", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);

    SpannerClient mockSpannerClient = new MockSpannerClient(emptySignal, mockWriteResult);

    // 4. Build Pipeline
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient, null);

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
    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteOutput_TfRecord", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);
    SpannerClient mockSpannerClient = new MockSpannerClient(emptySignal, mockWriteResult);

    // 4. Build and Run Pipeline
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient, null);
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

  private SpannerWriteResult createMockWriteResult(String suffix) {
    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteSignal_" + suffix, Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);
    return mockWriteResult;
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

    IngestionPipelineOptions defaultLocationOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    defaultLocationOpts.setProjectId("spanner-project");
    defaultLocationOpts.setEnableEmbeddings(true);
    VertexAiEmbeddingClient defaultClient =
        GraphIngestionPipeline.createVertexAiEmbeddingClient(defaultLocationOpts);
    assertNotNull(defaultClient);
    assertEquals("spanner-project", defaultClient.getProjectId());
    assertEquals("us-central1", defaultClient.getLocation());

    IngestionPipelineOptions customLocationOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    customLocationOpts.setProjectId("spanner-project");
    customLocationOpts.setLocation("us-east1");
    customLocationOpts.setEnableEmbeddings(true);
    VertexAiEmbeddingClient customClient =
        GraphIngestionPipeline.createVertexAiEmbeddingClient(customLocationOpts);
    assertNotNull(customClient);
    assertEquals("spanner-project", customClient.getProjectId());
    assertEquals("us-east1", customClient.getLocation());

    IngestionPipelineOptions blankLocationOpts =
        PipelineOptionsFactory.as(IngestionPipelineOptions.class);
    blankLocationOpts.setProjectId("spanner-project");
    blankLocationOpts.setLocation("  ");
    blankLocationOpts.setEnableEmbeddings(true);
    assertThrows(
        IllegalArgumentException.class,
        () -> GraphIngestionPipeline.createVertexAiEmbeddingClient(blankLocationOpts));
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

    SpannerClient mockSpannerClient = new MockSpannerClient(createMockWriteResult("Embed"));
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
        "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
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

    SpannerClient mockSpannerClient = new MockSpannerClient(createMockWriteResult("Multi"));
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
            "{\"subject_id\":\"dc/topic/Health\",\"name\":\"Health\"}",
            "dc/topic/Economy",
            "{\"subject_id\":\"dc/topic/Economy\",\"name\":\"Economy\"}"),
        embContentBySubject);
  }
}
