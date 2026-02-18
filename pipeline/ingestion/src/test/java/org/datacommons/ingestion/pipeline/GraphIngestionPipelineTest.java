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
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
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
    PCollection<Mutation> emptyMutations =
        pipeline.apply("CreateEmptyMutations", Create.empty(TypeDescriptor.of(Mutation.class)));

    // Mock Write Result
    SpannerWriteResult mockWriteResult =
        mock(SpannerWriteResult.class, withSettings().serializable());
    PCollection<Void> mockWriteOutput =
        pipeline.apply("CreateWriteSignal", Create.empty(TypeDescriptor.of(Void.class)));
    when(mockWriteResult.getOutput()).thenReturn(mockWriteOutput);

    SpannerClient mockSpannerClient = new MockSpannerClient(emptyMutations, mockWriteResult);

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

  static class MockSpannerClient extends SpannerClient {
    private final transient PCollection<Mutation> deleteMutations;
    private final transient SpannerWriteResult mockWriteResult;

    public MockSpannerClient(
        PCollection<Mutation> deleteMutations, SpannerWriteResult mockWriteResult) {
      super(
          SpannerClient.builder()
              .gcpProjectId("test")
              .spannerInstanceId("test")
              .spannerDatabaseId("test"));
      this.deleteMutations = deleteMutations;
      this.mockWriteResult = mockWriteResult;
    }

    @Override
    public PCollection<Mutation> getObservationDeleteMutations(
        String importName, Pipeline pipeline) {
      return deleteMutations;
    }

    @Override
    public PCollection<Mutation> getEdgeDeleteMutations(String provenance, Pipeline pipeline) {
      return deleteMutations;
    }

    @Override
    public SpannerWriteResult writeMutations(
        Pipeline pipeline,
        String name,
        List<PCollection<Mutation>> mutationList,
        PCollection<?> waitSignal) {

      PCollectionList.of(mutationList)
          .apply("Flatten" + name + "Mutations-Test", Flatten.pCollections())
          .apply(
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
