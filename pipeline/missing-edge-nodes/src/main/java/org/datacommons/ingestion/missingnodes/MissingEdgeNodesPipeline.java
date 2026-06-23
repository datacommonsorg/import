package org.datacommons.ingestion.missingnodes;

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

  public static void main(String[] args) {
    MissingEdgeNodesOptions options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(MissingEdgeNodesOptions.class);
    Pipeline pipeline = Pipeline.create(options);

    PCollection<KV<String, String>> nodeKeys =
        pipeline
            .apply("Read Node subject_ids", spannerRead(options, NODE_QUERY))
            .apply("Key Node subject_ids", MissingEdgeNodesUtils.nodeKeys());

    PCollection<KV<String, String>> edgeCandidates =
        pipeline
            .apply("Read Edge identifiers", spannerRead(options, EDGE_QUERY))
            .apply("Extract Edge candidates", MissingEdgeNodesUtils.edgeCandidates())
            .apply("Deduplicate Edge candidates", MissingEdgeNodesUtils.distinctCandidates());

    MissingEdgeNodesUtils.findMissingCandidates(edgeCandidates, nodeKeys)
        .apply("Format CSV rows", MissingEdgeNodesUtils.formatCsvRows())
        .apply(
            "Write missing dcids",
            TextIO.write()
                .to(options.getOutputLocation() + "/missing-edge-node-dcids")
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
}
