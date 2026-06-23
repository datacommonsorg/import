package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

/** Dataflow pipeline for finding Edge identifiers missing from Node.subject_id. */
public class MissingEdgeNodesPipeline {
  private static final String OUTPUT_HEADER = "dcid,type";
  private static final String NODE_QUERY = "SELECT subject_id FROM Node";
  private static final String EDGE_QUERY =
      "SELECT subject_id, predicate, object_id, provenance FROM Edge";
  private static final String PART_PREFIX = "part";

  public static void main(String[] args) {
    MissingEdgeNodesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MissingEdgeNodesOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Struct> nodeRows =
        pipeline.apply("Read Node subject_ids", spannerRead(options, NODE_QUERY));
    PCollection<String> nodeSubjectIds =
        nodeRows
            .apply(
                "Extract Node subject_ids",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.SUBJECT_ID))
            .apply("Deduplicate Node subject_ids", MissingEdgeNodesUtils.distinctValues());
    PCollection<KV<String, String>> nodeKeys =
        nodeSubjectIds.apply("Key Node subject_ids", MissingEdgeNodesUtils.nodeKeys());

    PCollection<Struct> edgeRows =
        pipeline.apply("Read Edge identifiers", spannerRead(options, EDGE_QUERY));

    PCollection<String> subjectIds =
        edgeRows
            .apply(
                "Extract Edge subject_ids",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.SUBJECT_ID))
            .apply("Deduplicate Edge subject_ids", MissingEdgeNodesUtils.distinctValues());
    PCollection<String> predicates =
        edgeRows
            .apply(
                "Extract Edge predicates",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.PREDICATE))
            .apply("Deduplicate Edge predicates", MissingEdgeNodesUtils.distinctValues());
    PCollection<String> objectIds =
        edgeRows
            .apply(
                "Extract Edge object_ids",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.OBJECT_ID))
            .apply("Deduplicate Edge object_ids", MissingEdgeNodesUtils.distinctValues());
    PCollection<String> provenances =
        edgeRows
            .apply(
                "Extract Edge provenances",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.PROVENANCE))
            .apply("Deduplicate Edge provenances", MissingEdgeNodesUtils.distinctValues());

    if (options.getWriteDedupedInputs()) {
      writeDedupedValues(
          nodeSubjectIds,
          "node_subject_ids",
          outputPrefix(options.getOutputLocation(), "distinct-node-subject-ids"));
      writeDedupedValues(
          subjectIds,
          "edge_subject_ids",
          outputPrefix(options.getOutputLocation(), "distinct-edge-subject-ids"));
      writeDedupedValues(
          predicates,
          "predicates",
          outputPrefix(options.getOutputLocation(), "distinct-predicates"));
      writeDedupedValues(
          objectIds,
          "object_ids",
          outputPrefix(options.getOutputLocation(), "distinct-object-ids"));
      writeDedupedValues(
          provenances,
          "provenances",
          outputPrefix(options.getOutputLocation(), "distinct-provenances"));
    }

    PCollection<KV<String, String>> edgeCandidates =
        PCollectionList.of(
                subjectIds.apply(
                    "Convert subject_ids to candidates",
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.SUBJECT_ID)))
            .and(
                predicates.apply(
                    "Convert predicates to candidates",
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.PREDICATE)))
            .and(
                objectIds.apply(
                    "Convert object_ids to candidates",
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.OBJECT_ID)))
            .and(
                provenances.apply(
                    "Convert provenances to candidates",
                    MissingEdgeNodesUtils.toCandidates(MissingEdgeNodesUtils.PROVENANCE)))
            .apply("Flatten Edge candidates", Flatten.pCollections());

    MissingEdgeNodesUtils.findMissingCandidates(edgeCandidates, nodeKeys)
        .apply("Format CSV rows", MissingEdgeNodesUtils.formatCsvRows())
        .apply(
            "Write missing dcids",
            TextIO.write()
                .to(outputPrefix(options.getOutputLocation(), "missing-edge-node-dcids"))
                .withSuffix(".csv")
                .withHeader(OUTPUT_HEADER));

    pipeline.run();
  }

  private static SpannerIO.Read spannerRead(MissingEdgeNodesOptions options, String query) {
    return SpannerIO.read()
        .withProjectId(options.getSpannerProjectId())
        .withInstanceId(options.getSpannerInstanceId())
        .withDatabaseId(options.getSpannerDatabaseId())
        .withQuery(query);
  }

  private static void writeDedupedValues(
      PCollection<String> values, String label, String outputPath) {
    values
        .apply("Format distinct " + label, MissingEdgeNodesUtils.formatCsvValues())
        .apply("Write distinct " + label, TextIO.write().to(outputPath).withSuffix(".csv"));
  }

  private static String outputPrefix(String outputLocation, String resultFolder) {
    return outputLocation + "/" + resultFolder + "/" + PART_PREFIX;
  }
}
