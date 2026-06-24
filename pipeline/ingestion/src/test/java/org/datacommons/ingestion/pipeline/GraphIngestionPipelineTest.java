package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.google.cloud.spanner.Mutation;
import java.io.File;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.spanner.SpannerWriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.datacommons.ingestion.spanner.SpannerClient;
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

  public static List<Mutation> capturedMutations = Collections.synchronizedList(new ArrayList<>());

  @Before
  public void setUp() {
    capturedMutations.clear();
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
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    verifyMutations(capturedMutations);
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
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    // If combined, we should see only 1 Node mutation for geoId/06!
    int nodeMutationCount = 0;
    for (com.google.cloud.spanner.Mutation m : capturedMutations) {
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
    GraphIngestionPipeline.buildPipeline(pipeline, options, mockSpannerClient);

    // 5. Run Pipeline
    pipeline.run().waitUntilFinish();

    // 6. Assertions
    // If transformation is skipped, we should NOT see "constraintProperties" in the mutations.
    // "gender" is NOT in NON_CONSTRAINT_PROPS, so it would normally trigger constraintProperties
    // generation.
    boolean foundConstraintProperties = false;
    for (Mutation m : capturedMutations) {
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

  static class MockSpannerClient extends SpannerClient {
    private final transient PCollection<Void> deleteSignal;
    private final transient SpannerWriteResult mockWriteResult;

    public MockSpannerClient(PCollection<Void> deleteSignal, SpannerWriteResult mockWriteResult) {
      super(
          SpannerClient.builder()
              .gcpProjectId("test")
              .spannerInstanceId("test")
              .spannerDatabaseId("test"));
      this.deleteSignal = deleteSignal;
      this.mockWriteResult = mockWriteResult;
    }

    @Override
    public PCollection<Void> deleteDataForImport(
        Pipeline pipeline, String importName, String tableName, String columnName) {
      return deleteSignal;
    }

    @Override
    public SpannerWriteResult writeMutations(
        Pipeline pipeline, String name, PCollection<Mutation> mutations) {

      mutations.apply(
          "Capture" + name + "Mutations",
          ParDo.of(
              new DoFn<Mutation, Void>() {
                @ProcessElement
                public void processElement(@Element Mutation m) {
                  capturedMutations.add(m);
                }
              }));
      return mockWriteResult;
    }
  }
}
