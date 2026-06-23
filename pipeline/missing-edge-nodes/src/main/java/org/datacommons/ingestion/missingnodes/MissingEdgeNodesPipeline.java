package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.spanner.SpannerIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

/** Dataflow pipeline for finding Edge identifiers missing from Node.subject_id. */
public class MissingEdgeNodesPipeline {
  private static final String OUTPUT_HEADER = "dcid,type";
  private static final String NODE_QUERY = "SELECT subject_id FROM Node";
  private static final String EDGE_QUERY =
      "SELECT subject_id, predicate, object_id, provenance FROM Edge";
  private static final String PART_PREFIX = "part";
  private static final String SPANNER_NODE_ROWS_READ = "spanner_node_rows_read";
  private static final String SPANNER_EDGE_ROWS_READ = "spanner_edge_rows_read";
  private static final String DISTINCT_NODE_SUBJECT_IDS = "distinct_node_subject_ids";
  private static final String DISTINCT_EDGE_SUBJECT_IDS = "distinct_edge_subject_ids";
  private static final String DISTINCT_PREDICATES = "distinct_predicates";
  private static final String DISTINCT_OBJECT_IDS = "distinct_object_ids";
  private static final String DISTINCT_PROVENANCES = "distinct_provenances";
  private static final String MISSING_EDGE_SUBJECT_IDS = "missing_edge_subject_ids";
  private static final String MISSING_PREDICATES = "missing_predicates";
  private static final String MISSING_OBJECT_IDS = "missing_object_ids";
  private static final String MISSING_PROVENANCES = "missing_provenances";

  public static void main(String[] args) {
    MissingEdgeNodesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MissingEdgeNodesOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<Struct> nodeRows =
        pipeline
            .apply("Read Node subject_ids", spannerRead(options, NODE_QUERY))
            .apply(
                "Count Spanner Node rows",
                MissingEdgeNodesUtils.countStructRows(SPANNER_NODE_ROWS_READ));
    PCollection<String> nodeSubjectIds =
        nodeRows
            .apply(
                "Extract Node subject_ids",
                MissingEdgeNodesUtils.extractColumn(MissingEdgeNodesUtils.SUBJECT_ID))
            .apply(
                "Count distinct Node subject_ids",
                MissingEdgeNodesUtils.countStringValues(DISTINCT_NODE_SUBJECT_IDS));
    PCollection<KV<String, String>> nodeKeys =
        nodeSubjectIds.apply("Key Node subject_ids", MissingEdgeNodesUtils.nodeKeys());

    PCollection<Struct> edgeRows =
        pipeline
            .apply("Read Edge identifiers", spannerRead(options, EDGE_QUERY))
            .apply(
                "Count Spanner Edge rows",
                MissingEdgeNodesUtils.countStructRows(SPANNER_EDGE_ROWS_READ));

    PCollection<KV<String, String>> edgeCandidates =
        edgeRows
            .apply("Extract Edge candidates", MissingEdgeNodesUtils.extractCandidates())
            .apply("Deduplicate Edge candidates", MissingEdgeNodesUtils.distinctValues())
            .apply(
                "Count distinct Edge candidate types",
                MissingEdgeNodesUtils.countTypedValues(
                    DISTINCT_EDGE_SUBJECT_IDS,
                    DISTINCT_PREDICATES,
                    DISTINCT_OBJECT_IDS,
                    DISTINCT_PROVENANCES));

    if (options.getWriteDedupedInputs()) {
      writeDedupedValues(
          nodeSubjectIds,
          "node_subject_ids",
          outputPrefix(options.getOutputLocation(), "distinct-node-subject-ids"));
      writeDedupedValues(
          edgeCandidates.apply(
              "Filter distinct Edge subject_ids",
              MissingEdgeNodesUtils.filterAndExtractKeys(MissingEdgeNodesUtils.SUBJECT_ID)),
          "edge_subject_ids",
          outputPrefix(options.getOutputLocation(), "distinct-edge-subject-ids"));
      writeDedupedValues(
          edgeCandidates.apply(
              "Filter distinct predicates",
              MissingEdgeNodesUtils.filterAndExtractKeys(MissingEdgeNodesUtils.PREDICATE)),
          "predicates",
          outputPrefix(options.getOutputLocation(), "distinct-predicates"));
      writeDedupedValues(
          edgeCandidates.apply(
              "Filter distinct object_ids",
              MissingEdgeNodesUtils.filterAndExtractKeys(MissingEdgeNodesUtils.OBJECT_ID)),
          "object_ids",
          outputPrefix(options.getOutputLocation(), "distinct-object-ids"));
      writeDedupedValues(
          edgeCandidates.apply(
              "Filter distinct provenances",
              MissingEdgeNodesUtils.filterAndExtractKeys(MissingEdgeNodesUtils.PROVENANCE)),
          "provenances",
          outputPrefix(options.getOutputLocation(), "distinct-provenances"));
    }

    MissingEdgeNodesUtils.findMissingCandidates(edgeCandidates, nodeKeys)
        .apply(
            "Count missing candidate types",
            MissingEdgeNodesUtils.countTypedValues(
                MISSING_EDGE_SUBJECT_IDS,
                MISSING_PREDICATES,
                MISSING_OBJECT_IDS,
                MISSING_PROVENANCES))
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
