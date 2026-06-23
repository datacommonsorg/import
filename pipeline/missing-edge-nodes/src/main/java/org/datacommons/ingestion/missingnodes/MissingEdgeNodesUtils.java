package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

/** Utility transforms for the missing Edge dcids pipeline. */
class MissingEdgeNodesUtils {
  static final String SUBJECT_ID = "subject_id";
  static final String PREDICATE = "predicate";
  static final String OBJECT_ID = "object_id";
  static final String PROVENANCE = "provenance";

  private static final String NODE_MARKER = "node";

  private MissingEdgeNodesUtils() {}

  static ParDo.SingleOutput<Struct, KV<String, String>> nodeKeys() {
    return ParDo.of(new NodeKeyFn());
  }

  static ParDo.SingleOutput<Struct, KV<String, String>> edgeCandidates() {
    return ParDo.of(new EdgeCandidateFn());
  }

  static Distinct<KV<String, String>> distinctCandidates() {
    return Distinct.create();
  }

  static PCollection<KV<String, String>> findMissingCandidates(
      PCollection<KV<String, String>> edgeCandidates, PCollection<KV<String, String>> nodeKeys) {
    TupleTag<String> edgeTag = new TupleTag<>();
    TupleTag<String> nodeTag = new TupleTag<>();

    return KeyedPCollectionTuple.of(edgeTag, edgeCandidates)
        .and(nodeTag, nodeKeys)
        .apply("Join Edge candidates to Node keys", CoGroupByKey.create())
        .apply("Keep candidates without Node", ParDo.of(new MissingCandidateFn(edgeTag, nodeTag)));
  }

  static ParDo.SingleOutput<KV<String, String>, String> formatCsvRows() {
    return ParDo.of(new FormatCsvRowFn());
  }

  private static boolean hasValue(String value) {
    return value != null && !value.isEmpty();
  }

  private static String nullableString(Struct row, String columnName) {
    if (row.isNull(columnName)) {
      return null;
    }
    return row.getString(columnName);
  }

  private static String escapeCsv(String value) {
    if (value.indexOf(',') < 0
        && value.indexOf('"') < 0
        && value.indexOf('\n') < 0
        && value.indexOf('\r') < 0) {
      return value;
    }
    return "\"" + value.replace("\"", "\"\"") + "\"";
  }

  static class NodeKeyFn extends DoFn<Struct, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      String subjectId = nullableString(context.element(), SUBJECT_ID);
      if (hasValue(subjectId)) {
        context.output(KV.of(subjectId, NODE_MARKER));
      }
    }
  }

  static class EdgeCandidateFn extends DoFn<Struct, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Struct row = context.element();
      String subjectId = nullableString(row, SUBJECT_ID);
      String predicate = nullableString(row, PREDICATE);
      String objectId = nullableString(row, OBJECT_ID);
      String provenance = nullableString(row, PROVENANCE);
      if (hasValue(subjectId)) {
        context.output(KV.of(subjectId, SUBJECT_ID));
      }
      if (hasValue(predicate)) {
        context.output(KV.of(predicate, PREDICATE));
      }
      if (hasValue(objectId)) {
        context.output(KV.of(objectId, OBJECT_ID));
      }
      if (hasValue(provenance)) {
        context.output(KV.of(provenance, PROVENANCE));
      }
    }
  }

  static class MissingCandidateFn extends DoFn<KV<String, CoGbkResult>, KV<String, String>> {
    private final TupleTag<String> edgeTag;
    private final TupleTag<String> nodeTag;

    MissingCandidateFn(TupleTag<String> edgeTag, TupleTag<String> nodeTag) {
      this.edgeTag = edgeTag;
      this.nodeTag = nodeTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, CoGbkResult> input = context.element();
      if (input.getValue().getAll(nodeTag).iterator().hasNext()) {
        return;
      }
      for (String type : input.getValue().getAll(edgeTag)) {
        context.output(KV.of(input.getKey(), type));
      }
    }
  }

  static class FormatCsvRowFn extends DoFn<KV<String, String>, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> input = context.element();
      context.output(escapeCsv(input.getKey()) + "," + escapeCsv(input.getValue()));
    }
  }
}
