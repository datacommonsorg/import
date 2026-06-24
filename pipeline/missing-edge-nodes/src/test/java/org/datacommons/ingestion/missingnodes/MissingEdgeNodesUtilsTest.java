package org.datacommons.ingestion.missingnodes;

import static org.junit.Assert.assertFalse;

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
  public void findMissingCandidateOutputsUsesDistinctTypedStreams() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<Struct> edgeRows =
        pipeline.apply(
            Create.of(
                Struct.newBuilder()
                    .set(MissingEdgeNodesUtils.SUBJECT_ID)
                    .to("subj1")
                    .set(MissingEdgeNodesUtils.PREDICATE)
                    .to("pred1")
                    .set(MissingEdgeNodesUtils.OBJECT_ID)
                    .to("missing")
                    .set(MissingEdgeNodesUtils.PROVENANCE)
                    .to("missing")
                    .build(),
                Struct.newBuilder()
                    .set(MissingEdgeNodesUtils.SUBJECT_ID)
                    .to("subj2")
                    .set(MissingEdgeNodesUtils.PREDICATE)
                    .to("pred2")
                    .set(MissingEdgeNodesUtils.OBJECT_ID)
                    .to("missing")
                    .set(MissingEdgeNodesUtils.PROVENANCE)
                    .to("missing2")
                    .build()));
    PCollection<KV<String, String>> edgeCandidates =
        edgeRows
            .apply(MissingEdgeNodesUtils.extractCandidates())
            .apply(MissingEdgeNodesUtils.distinctValues());
    PCollection<KV<String, String>> nodeKeys =
        pipeline
            .apply("Create node ids", Create.of("missing2"))
            .apply(MissingEdgeNodesUtils.nodeKeys());

    MissingEdgeNodesUtils.MissingCandidateOutputs missing =
        MissingEdgeNodesUtils.findMissingCandidateOutputs(edgeCandidates, nodeKeys);

    PAssert.that(missing.typedRows())
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("subj1", MissingEdgeNodesUtils.SUBJECT_ID),
                KV.of("subj2", MissingEdgeNodesUtils.SUBJECT_ID),
                KV.of("pred1", MissingEdgeNodesUtils.PREDICATE),
                KV.of("pred2", MissingEdgeNodesUtils.PREDICATE),
                KV.of("missing", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("missing", MissingEdgeNodesUtils.PROVENANCE)));
    PAssert.that(missing.dcids())
        .containsInAnyOrder(Arrays.asList("subj1", "subj2", "pred1", "pred2", "missing"));

    pipeline.run();
  }

  @Test
  public void filterAndExtractKeysKeepsOnlyRequestedType() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<String> objectIds =
        pipeline
            .apply(
                Create.<KV<String, String>>of(
                    KV.of("subject", MissingEdgeNodesUtils.SUBJECT_ID),
                    KV.of("object", MissingEdgeNodesUtils.OBJECT_ID),
                    KV.of("predicate", MissingEdgeNodesUtils.PREDICATE)))
            .apply(MissingEdgeNodesUtils.filterAndExtractKeys(MissingEdgeNodesUtils.OBJECT_ID));

    PAssert.that(objectIds).containsInAnyOrder(Arrays.asList("object"));

    pipeline.run();
  }

  @Test
  public void countStringValuesPassesThroughElements() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<String> values =
        pipeline
            .apply(Create.of("a", "b"))
            .apply(MissingEdgeNodesUtils.countStringValues("test_distinct_values"));

    PAssert.that(values).containsInAnyOrder(Arrays.asList("a", "b"));

    pipeline.run();
  }

  @Test
  public void countTypedValuesPassesThroughElements() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<KV<String, String>> values =
        pipeline
            .apply(
                Create.<KV<String, String>>of(
                    KV.of("subject", MissingEdgeNodesUtils.SUBJECT_ID),
                    KV.of("predicate", MissingEdgeNodesUtils.PREDICATE),
                    KV.of("object", MissingEdgeNodesUtils.OBJECT_ID),
                    KV.of("provenance", MissingEdgeNodesUtils.PROVENANCE)))
            .apply(
                MissingEdgeNodesUtils.countTypedValues(
                    "test_subject_ids", "test_predicates", "test_object_ids", "test_provenances"));

    PAssert.that(values)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("subject", MissingEdgeNodesUtils.SUBJECT_ID),
                KV.of("predicate", MissingEdgeNodesUtils.PREDICATE),
                KV.of("object", MissingEdgeNodesUtils.OBJECT_ID),
                KV.of("provenance", MissingEdgeNodesUtils.PROVENANCE)));

    pipeline.run();
  }

  @Test
  public void classifyProvisionalMcfDcidsSplitsValidAndInvalidDcids() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);
    String longDcid = repeat('a', 257);

    MissingEdgeNodesUtils.ProvisionalMcfDcidOutputs outputs =
        MissingEdgeNodesUtils.classifyProvisionalMcfDcids(
            pipeline.apply(
                Create.of(
                    "myCustomProperty",
                    "geoId/06",
                    "dc/p/abc",
                    "has,comma",
                    "has\nnewline",
                    "has\"quote",
                    "has space",
                    "bio/foo bar",
                    "",
                    longDcid)));

    PAssert.that(outputs.validDcids())
        .containsInAnyOrder(Arrays.asList("myCustomProperty", "geoId/06", "dc/p/abc"));
    PAssert.that(outputs.invalidDcids())
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("has,comma", "invalid_dcid_chars"),
                KV.of("has\nnewline", "invalid_dcid_chars"),
                KV.of("has\"quote", "invalid_dcid_chars"),
                KV.of("has space", "invalid_dcid_chars"),
                KV.of("bio/foo bar", "invalid_dcid_chars"),
                KV.of("", "empty"),
                KV.of(longDcid, "too_long")));

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

  @Test
  public void formatProvisionalMcfNodes() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);

    PCollection<String> rows =
        pipeline
            .apply(Create.of("myCustomProperty"))
            .apply(MissingEdgeNodesUtils.formatProvisionalMcfNodes());

    PAssert.that(rows)
        .containsInAnyOrder(
            Arrays.asList("Node: dcid:myCustomProperty\ntypeOf: dcs:ProvisionalNode\n"));

    pipeline.run();
  }

  @Test
  public void writeDedupedInputsDefaultsToFalse() {
    MissingEdgeNodesOptions missingOptions =
        PipelineOptionsFactory.create().as(MissingEdgeNodesOptions.class);

    assertFalse(missingOptions.getWriteDedupedInputs());
  }

  private static String repeat(char value, int count) {
    char[] chars = new char[count];
    Arrays.fill(chars, value);
    return new String(chars);
  }
}
