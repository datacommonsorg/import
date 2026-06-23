package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;

public class MissingEdgeNodesUtilsTest {
  private final PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(options);

  @Test
  public void edgeCandidatesEmitsAllNonEmptyValues() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<KV<String, String>> candidates =
        pipeline
            .apply(
                Create.of(
                    Struct.newBuilder()
                        .set(MissingEdgeNodesUtils.SUBJECT_ID)
                        .to("subject")
                        .set(MissingEdgeNodesUtils.PREDICATE)
                        .to("predicate")
                        .set(MissingEdgeNodesUtils.OBJECT_ID)
                        .to("object")
                        .set(MissingEdgeNodesUtils.PROVENANCE)
                        .to("provenance")
                        .build(),
                    Struct.newBuilder()
                        .set(MissingEdgeNodesUtils.SUBJECT_ID)
                        .to("")
                        .set(MissingEdgeNodesUtils.PREDICATE)
                        .to("predicate2")
                        .set(MissingEdgeNodesUtils.OBJECT_ID)
                        .to("")
                        .set(MissingEdgeNodesUtils.PROVENANCE)
                        .to("provenance2")
                        .build()))
            .apply(MissingEdgeNodesUtils.edgeCandidates());

    PAssert.that(candidates)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("subject", MissingEdgeNodesUtils.SUBJECT_ID),
                KV.of("predicate", MissingEdgeNodesUtils.PREDICATE),
                KV.of("object", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("provenance", MissingEdgeNodesUtils.PROVENANCE),
                KV.of("predicate2", MissingEdgeNodesUtils.PREDICATE),
                KV.of("provenance2", MissingEdgeNodesUtils.PROVENANCE)));

    pipeline.run();
  }

  @Test
  public void findMissingCandidatesFiltersExistingNodesAndKeepsDistinctTypes() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<KV<String, String>> edgeCandidates =
        pipeline.apply(
            "Create edge candidates",
            Create.<KV<String, String>>of(
                KV.of("exists", MissingEdgeNodesUtils.SUBJECT_ID),
                KV.of("missing", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("missing", MissingEdgeNodesUtils.PROVENANCE),
                KV.of("missing", MissingEdgeNodesUtils.PROVENANCE)));
    PCollection<KV<String, String>> nodeKeys =
        pipeline.apply("Create node keys", Create.<KV<String, String>>of(KV.of("exists", "node")));

    PCollection<KV<String, String>> missing =
        MissingEdgeNodesUtils.findMissingCandidates(
            edgeCandidates.apply(MissingEdgeNodesUtils.distinctCandidates()), nodeKeys);

    PAssert.that(missing)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("missing", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("missing", MissingEdgeNodesUtils.PROVENANCE)));

    pipeline.run();
  }

  @Test
  public void formatCsvRowsEscapesSpecialCharacters() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<String> rows =
        pipeline
            .apply(
                Create.<KV<String, String>>of(
                    KV.of("plain", MissingEdgeNodesUtils.SUBJECT_ID),
                    KV.of("has,comma", MissingEdgeNodesUtils.PREDICATE),
                    KV.of("has\"quote", MissingEdgeNodesUtils.OBJECT_ID),
                    KV.of("has\nnewline", MissingEdgeNodesUtils.PROVENANCE)))
            .apply(MissingEdgeNodesUtils.formatCsvRows());

    PAssert.that(rows)
        .containsInAnyOrder(
            Arrays.asList(
                "plain,subject_id",
                "\"has,comma\",predicate",
                "\"has\"\"quote\",object_id",
                "\"has\nnewline\",provenance"));

    pipeline.run();
  }
}
