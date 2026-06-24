package org.datacommons.ingestion.missingnodes;

import com.google.cloud.spanner.Struct;
import java.util.Iterator;
import java.util.regex.Pattern;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;

/** Utility transforms for the missing Edge dcids pipeline. */
class MissingEdgeNodesUtils {
  static final String SUBJECT_ID = "subject_id";
  static final String PREDICATE = "predicate";
  static final String OBJECT_ID = "object_id";
  static final String PROVENANCE = "provenance";

  private static final int MAX_DCID_LENGTH = 256;
  private static final String NODE_MARKER = "node";
  private static final Pattern VALID_DCID_PATTERN =
      Pattern.compile("^[A-Za-z0-9_&/%\\)\\(+\\-\\.:]+$");

  private MissingEdgeNodesUtils() {}

  static ParDo.SingleOutput<String, KV<String, String>> nodeKeys() {
    return ParDo.of(new NodeKeyFn());
  }

  static ParDo.SingleOutput<Struct, Struct> countStructRows(String counterName) {
    return ParDo.of(new CountElementsFn<>(counterName));
  }

  static ParDo.SingleOutput<String, String> countStringValues(String counterName) {
    return ParDo.of(new CountElementsFn<>(counterName));
  }

  static ParDo.SingleOutput<KV<String, String>, KV<String, String>> countKvValues(
      String counterName) {
    return ParDo.of(new CountElementsFn<>(counterName));
  }

  static ParDo.SingleOutput<Struct, String> extractColumn(String columnName) {
    return ParDo.of(new ExtractColumnFn(columnName));
  }

  static <T> Distinct<T> distinctValues() {
    return Distinct.create();
  }

  static ParDo.SingleOutput<Struct, KV<String, String>> extractCandidates() {
    return ParDo.of(new ExtractCandidatesFn());
  }

  static ParDo.SingleOutput<KV<String, String>, KV<String, String>> countTypedValues(
      String subjectIdCounterName,
      String predicateCounterName,
      String objectIdCounterName,
      String provenanceCounterName) {
    return ParDo.of(
        new CountTypedValuesFn(
            subjectIdCounterName,
            predicateCounterName,
            objectIdCounterName,
            provenanceCounterName));
  }

  static ParDo.SingleOutput<KV<String, String>, String> filterAndExtractKeys(String type) {
    return ParDo.of(new FilterAndExtractKeysFn(type));
  }

  static PCollection<KV<String, String>> findMissingCandidates(
      PCollection<KV<String, String>> edgeCandidates, PCollection<KV<String, String>> nodeKeys) {
    return findMissingCandidateOutputs(edgeCandidates, nodeKeys).typedRows();
  }

  static MissingCandidateOutputs findMissingCandidateOutputs(
      PCollection<KV<String, String>> edgeCandidates, PCollection<KV<String, String>> nodeKeys) {
    TupleTag<String> edgeTag = new TupleTag<>();
    TupleTag<String> nodeTag = new TupleTag<>();
    TupleTag<KV<String, String>> missingTypedRowsTag = new TupleTag<KV<String, String>>() {};
    TupleTag<String> missingDcidsTag = new TupleTag<String>() {};

    PCollectionTuple outputs =
        KeyedPCollectionTuple.of(edgeTag, edgeCandidates)
            .and(nodeTag, nodeKeys)
            .apply("Join Edge candidates to Node keys", CoGroupByKey.create())
            .apply(
                "Keep candidates without Node",
                ParDo.of(new MissingCandidateFn(edgeTag, nodeTag, missingDcidsTag))
                    .withOutputTags(missingTypedRowsTag, TupleTagList.of(missingDcidsTag)));
    return new MissingCandidateOutputs(
        outputs.get(missingTypedRowsTag), outputs.get(missingDcidsTag));
  }

  static ProvisionalMcfDcidOutputs classifyProvisionalMcfDcids(PCollection<String> dcids) {
    TupleTag<String> validDcidsTag = new TupleTag<String>() {};
    TupleTag<KV<String, String>> invalidDcidsTag = new TupleTag<KV<String, String>>() {};

    PCollectionTuple outputs =
        dcids.apply(
            "Classify provisional MCF dcids",
            ParDo.of(new ClassifyProvisionalMcfDcidFn(invalidDcidsTag))
                .withOutputTags(validDcidsTag, TupleTagList.of(invalidDcidsTag)));
    return new ProvisionalMcfDcidOutputs(outputs.get(validDcidsTag), outputs.get(invalidDcidsTag));
  }

  static ParDo.SingleOutput<KV<String, String>, String> formatCsvRows() {
    return ParDo.of(new FormatCsvRowFn());
  }

  static ParDo.SingleOutput<String, String> formatCsvValues() {
    return ParDo.of(new FormatCsvValueFn());
  }

  static ParDo.SingleOutput<String, String> formatProvisionalMcfNodes() {
    return ParDo.of(new FormatProvisionalMcfNodeFn());
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

  private static String invalidProvisionalMcfDcidReason(String dcid) {
    if (!hasValue(dcid)) {
      return "empty";
    }
    if (dcid.length() > MAX_DCID_LENGTH) {
      return "too_long";
    }
    if (!VALID_DCID_PATTERN.matcher(dcid).matches()) {
      return "invalid_dcid_chars";
    }
    return "";
  }

  static class NodeKeyFn extends DoFn<String, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      String subjectId = context.element();
      if (hasValue(subjectId)) {
        context.output(KV.of(subjectId, NODE_MARKER));
      }
    }
  }

  static class CountElementsFn<T> extends DoFn<T, T> {
    private final String counterName;
    private transient Counter counter;

    CountElementsFn(String counterName) {
      this.counterName = counterName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      getCounter().inc();
      context.output(context.element());
    }

    private Counter getCounter() {
      if (counter == null) {
        counter = Metrics.counter(MissingEdgeNodesUtils.class, counterName);
      }
      return counter;
    }
  }

  static class ExtractColumnFn extends DoFn<Struct, String> {
    private final String columnName;

    ExtractColumnFn(String columnName) {
      this.columnName = columnName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      String value = nullableString(context.element(), columnName);
      if (hasValue(value)) {
        context.output(value);
      }
    }
  }

  static class ExtractCandidatesFn extends DoFn<Struct, KV<String, String>> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      Struct row = context.element();
      outputIfValid(context, row, SUBJECT_ID);
      outputIfValid(context, row, PREDICATE);
      outputIfValid(context, row, OBJECT_ID);
      outputIfValid(context, row, PROVENANCE);
    }

    private void outputIfValid(ProcessContext context, Struct row, String columnName) {
      String value = nullableString(row, columnName);
      if (hasValue(value)) {
        context.output(KV.of(value, columnName));
      }
    }
  }

  static class CountTypedValuesFn extends DoFn<KV<String, String>, KV<String, String>> {
    private final String subjectIdCounterName;
    private final String predicateCounterName;
    private final String objectIdCounterName;
    private final String provenanceCounterName;
    private transient Counter subjectIdCounter;
    private transient Counter predicateCounter;
    private transient Counter objectIdCounter;
    private transient Counter provenanceCounter;

    CountTypedValuesFn(
        String subjectIdCounterName,
        String predicateCounterName,
        String objectIdCounterName,
        String provenanceCounterName) {
      this.subjectIdCounterName = subjectIdCounterName;
      this.predicateCounterName = predicateCounterName;
      this.objectIdCounterName = objectIdCounterName;
      this.provenanceCounterName = provenanceCounterName;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> candidate = context.element();
      incrementCounter(candidate.getValue());
      context.output(candidate);
    }

    private void incrementCounter(String type) {
      if (SUBJECT_ID.equals(type)) {
        getSubjectIdCounter().inc();
      } else if (PREDICATE.equals(type)) {
        getPredicateCounter().inc();
      } else if (OBJECT_ID.equals(type)) {
        getObjectIdCounter().inc();
      } else if (PROVENANCE.equals(type)) {
        getProvenanceCounter().inc();
      }
    }

    private Counter getSubjectIdCounter() {
      if (subjectIdCounter == null) {
        subjectIdCounter = Metrics.counter(MissingEdgeNodesUtils.class, subjectIdCounterName);
      }
      return subjectIdCounter;
    }

    private Counter getPredicateCounter() {
      if (predicateCounter == null) {
        predicateCounter = Metrics.counter(MissingEdgeNodesUtils.class, predicateCounterName);
      }
      return predicateCounter;
    }

    private Counter getObjectIdCounter() {
      if (objectIdCounter == null) {
        objectIdCounter = Metrics.counter(MissingEdgeNodesUtils.class, objectIdCounterName);
      }
      return objectIdCounter;
    }

    private Counter getProvenanceCounter() {
      if (provenanceCounter == null) {
        provenanceCounter = Metrics.counter(MissingEdgeNodesUtils.class, provenanceCounterName);
      }
      return provenanceCounter;
    }
  }

  static class FilterAndExtractKeysFn extends DoFn<KV<String, String>, String> {
    private final String type;

    FilterAndExtractKeysFn(String type) {
      this.type = type;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, String> candidate = context.element();
      if (type.equals(candidate.getValue())) {
        context.output(candidate.getKey());
      }
    }
  }

  static class MissingCandidateOutputs {
    private final PCollection<KV<String, String>> typedRows;
    private final PCollection<String> dcids;

    MissingCandidateOutputs(PCollection<KV<String, String>> typedRows, PCollection<String> dcids) {
      this.typedRows = typedRows;
      this.dcids = dcids;
    }

    PCollection<KV<String, String>> typedRows() {
      return typedRows;
    }

    PCollection<String> dcids() {
      return dcids;
    }
  }

  static class ProvisionalMcfDcidOutputs {
    private final PCollection<String> validDcids;
    private final PCollection<KV<String, String>> invalidDcids;

    ProvisionalMcfDcidOutputs(
        PCollection<String> validDcids, PCollection<KV<String, String>> invalidDcids) {
      this.validDcids = validDcids;
      this.invalidDcids = invalidDcids;
    }

    PCollection<String> validDcids() {
      return validDcids;
    }

    PCollection<KV<String, String>> invalidDcids() {
      return invalidDcids;
    }
  }

  static class ClassifyProvisionalMcfDcidFn extends DoFn<String, String> {
    private final TupleTag<KV<String, String>> invalidDcidsTag;

    ClassifyProvisionalMcfDcidFn(TupleTag<KV<String, String>> invalidDcidsTag) {
      this.invalidDcidsTag = invalidDcidsTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      String dcid = context.element();
      String invalidReason = invalidProvisionalMcfDcidReason(dcid);
      if (invalidReason.isEmpty()) {
        context.output(dcid);
        return;
      }
      context.output(invalidDcidsTag, KV.of(dcid == null ? "" : dcid, invalidReason));
    }
  }

  static class MissingCandidateFn extends DoFn<KV<String, CoGbkResult>, KV<String, String>> {
    private final TupleTag<String> edgeTag;
    private final TupleTag<String> nodeTag;
    private final TupleTag<String> missingDcidsTag;

    MissingCandidateFn(
        TupleTag<String> edgeTag, TupleTag<String> nodeTag, TupleTag<String> missingDcidsTag) {
      this.edgeTag = edgeTag;
      this.nodeTag = nodeTag;
      this.missingDcidsTag = missingDcidsTag;
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
      KV<String, CoGbkResult> input = context.element();
      if (input.getValue().getAll(nodeTag).iterator().hasNext()) {
        return;
      }
      Iterator<String> edgeTypes = input.getValue().getAll(edgeTag).iterator();
      if (!edgeTypes.hasNext()) {
        return;
      }
      context.output(missingDcidsTag, input.getKey());
      while (edgeTypes.hasNext()) {
        context.output(KV.of(input.getKey(), edgeTypes.next()));
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

  static class FormatCsvValueFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(escapeCsv(context.element()));
    }
  }

  static class FormatProvisionalMcfNodeFn extends DoFn<String, String> {
    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output("Node: dcid:" + context.element() + "\n" + "typeOf: dcs:ProvisionalNode\n");
    }
  }
}
