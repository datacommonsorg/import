package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import java.util.Arrays;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.junit.Rule;
import org.junit.Test;

public class MissingEdgeNodesUtilsTest {
  private final PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline pipeline = TestPipeline.fromOptions(options);

  @Test
  public void extractColumnFiltersEmptyValues() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<Struct> edgeRows =
        pipeline.apply(
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
                    .build()));

    PAssert.that(
            edgeRows.apply(MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.SUBJECT_ID)))
        .containsInAnyOrder(Arrays.asList("subject"));
    PAssert.that(
            edgeRows.apply(MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.PREDICATE)))
        .containsInAnyOrder(Arrays.asList("predicate", "predicate2"));
    PAssert.that(
            edgeRows.apply(MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.OBJECT_ID)))
        .containsInAnyOrder(Arrays.asList("object"));
    PAssert.that(
            edgeRows.apply(MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.PROVENANCE)))
        .containsInAnyOrder(Arrays.asList("provenance", "provenance2"));

    pipeline.run();
  }

  @Test
  public void findMissingCandidatesUsesDistinctTypedStreams() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<String> objectIds =
        pipeline
            .apply("Create object ids", Create.of("missing", "missing"))
            .apply(MissingEdgeNodesUtils.distinctValues());
    PCollection<String> provenances =
        pipeline
            .apply("Create provenances", Create.of("missing", "missing2"))
            .apply(MissingEdgeNodesUtils.distinctValues());
    PCollection<KV<String, String>> edgeCandidates =
        PCollectionList.of(
                objectIds.apply(
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.OBJECT_ID)))
            .and(
                provenances.apply(
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.PROVENANCE)))
            .apply(Flatten.pCollections());
    PCollection<KV<String, String>> nodeKeys =
        pipeline.apply(
            "Create node keys", Create.<KV<String, String>>of(KV.of("missing2", "node")));

    PCollection<KV<String, String>> missing =
        MissingEdgeNodesUtils.findMissingCandidates(edgeCandidates, nodeKeys);

    PAssert.that(missing)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("missing", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("missing", MissingEdgeNodesUtils.PROVENANCE)));

    pipeline.run();
  }

  @Test
  public void formatCsvEscapesSpecialCharacters() {
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

    PCollection<String> values =
        pipeline
            .apply("Create one-column values", Create.of("plain", "has,comma", "has\"quote"))
            .apply(MissingEdgeNodesUtils.formatCsvValues());

    PAssert.that(values)
        .containsInAnyOrder(Arrays.asList("plain", "\"has,comma\"", "\"has\"\"quote\""));

    pipeline.run();
  }
}
