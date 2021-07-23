package org.datacommons.util;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

public class TmcfCsvParser {
  private Mcf.McfGraph tmcf;
  private char delimiter;
  private CSVParser csvParser;
  private HashMap<String, Long> counters;
  private Debug.Log.Builder logCtx;

  public static TmcfCsvParser init(
      String tmcf_file, String csv_file, char delimiter, Debug.Log.Builder logCtx)
      throws IOException {
    TmcfCsvParser tmcfCsvParser = new TmcfCsvParser();
    tmcfCsvParser.tmcf = McfParser.parseTemplateMcfFile(tmcf_file);
    tmcfCsvParser.csvParser =
        CSVParser.parse(
            csv_file,
            CSVFormat.DEFAULT.withDelimiter(delimiter).withEscape('\\').withSkipHeaderRecord());
    tmcfCsvParser.delimiter = delimiter;
    tmcfCsvParser.logCtx = logCtx;
    assert tmcfCsvParser.csvParser.getHeaderMap() != null
        : "Unable to parse header from file " + csv_file;
    return tmcfCsvParser;
  }

  class RowProcessor {
    private Mcf.McfGraph.Builder instanceMcf;
    // If there is a dcid prop, then this is a map from the entity name (Table->E1) to the dcid.
    private HashMap<String, String> entityToDcid;
    private String currentNode;
    private String currentProp;
    private String rowId;

    public RowProcessor() {
      instanceMcf = Mcf.McfGraph.newBuilder();
      instanceMcf.setType(Mcf.McfType.INSTANCE_MCF);
      currentNode = new String();
      currentProp = new String();
      entityToDcid = new HashMap<>();
      rowId =
          TEST_mode
              ? String.valueOf(csvParser.getCurrentLineNumber())
              : UUID.randomUUID().toString();
    }

    public Mcf.McfGraph instanceMcf() {
      return instanceMcf.build();
    }

    public void process(CSVRecord dataRow) {
      if (dataRow.isConsistent()) {
        incrementCounter("NumInconsistentCSVRows", logCtx);
        return;
      }

      // Process DCIDs from all the nodes first and add to entityToDcid map, which will be consulted
      // to resolve entity references in processValues() function.
      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        Map<String, Mcf.McfGraph.Values> pvs = tableEntity.getValue().getPvsMap();
        if (!pvs.containsKey(Vocabulary.DCID)) continue;
        currentNode = toNodeName(tableEntity.getKey());
        currentProp = Vocabulary.DCID;
        Mcf.McfGraph.Values dcidValues = parseValues(pvs.get(currentProp), dataRow);
        String dcid = "";
        if (dcidValues == null) {
          incrementCounter("NumDCIDParseNodeFailures", logCtx);
          incrementCounterBy("NumDCIDParsePVFailures", pvs.size(), logCtx);
        } else {
          if (dcidValues.getTypedValuesList().size() > 0) {
            if (dcidValues.getTypedValues(0).getType() == Mcf.ValueType.TEXT) {
              dcid = dcidValues.getTypedValues(0).getValue();
            } else {
              incrementCounter("NumMalformedDCIDNodeFailures", logCtx);
              incrementCounterBy("NumMalformedDCIDPVFailures", pvs.size(), logCtx);
            }
          } else {
            incrementCounter("NumEmptyDCIDNodeFailures", logCtx);
            incrementCounterBy("NumEmptyDCIDPVFailures", pvs.size(), logCtx);
          }
        }
        // Register the fact that the user has mapped dcid, even if its empty.
        entityToDcid.put(tableEntity.getKey(), dcid);
      }

      Mcf.McfGraph.PropertyValues.Builder nodeBuilder = Mcf.McfGraph.PropertyValues.newBuilder();
      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        currentNode = toNodeName(tableEntity.getKey());
        if (currentNode == Vocabulary.DCID_PREFIX) {
          // Case of malformed/empty DCID. SKip this node (counters were updated above).
          continue;
        }

        // Go over each property within the template.
        for (Map.Entry<String, Mcf.McfGraph.Values> pv :
            tableEntity.getValue().getPvsMap().entrySet()) {
          currentProp = pv.getKey();

          // Don't process functionalDeps
          if (currentProp == Vocabulary.FUNCTIONAL_DEPS) continue;

          // Replace column names with values
          Mcf.McfGraph.Values values = parseValues(pv.getValue(), dataRow);
          if (values == null) {
            incrementCounter("NumParsePVFailures_" + currentProp, logCtx);
            continue;
          }
          if (values.getTypedValuesCount() == 0) {
            incrementCounter("NumEmptyPVFailures_" + currentProp, logCtx);
            continue;
          }
          nodeBuilder.getPvsMap().put(currentProp, values);
        }
        // TODO: Add node sanity check
        instanceMcf.getNodesMap().put(currentNode, nodeBuilder.build());
      }
    }

    private Mcf.McfGraph.Values parseValues(Mcf.McfGraph.Values templateValues, CSVRecord dataRow) {
      Mcf.McfGraph.Values.Builder instanceValues = Mcf.McfGraph.Values.newBuilder();
      for (Mcf.McfGraph.TypedValue typedValue : templateValues.getTypedValuesList()) {
        if (typedValue.getType() == Mcf.ValueType.TABLE_ENTITY) {
          assert currentProp != Vocabulary.DCID
              : "Unexpected value for dcid " + typedValue.getValue();
          String referenceNode = toNodeName(typedValue.getValue());
          Mcf.McfGraph.TypedValue.Builder newTypedValue = Mcf.McfGraph.TypedValue.newBuilder();
          if (referenceNode.startsWith(Vocabulary.DCID_PREFIX)) {
            String dcid = referenceNode.substring(Vocabulary.DCID_PREFIX.length());
            if (dcid.isEmpty()) {
              incrementCounter("NumEmptyDCIDValues", logCtx);
              continue;
            }
            newTypedValue.setType(Mcf.ValueType.RESOLVED_REF);
            newTypedValue.setValue(dcid);
          } else {
            // This is an internal reference, so prefix "l:"
            newTypedValue.setType(Mcf.ValueType.UNRESOLVED_REF);
            newTypedValue.setValue(Vocabulary.INTERNAL_REF_PREFIX + referenceNode);
          }
        } else if (typedValue.getType() == Mcf.ValueType.TABLE_COLUMN) {
          // Replace column-name with cell-value
          int columnIndex = toColumnIndex(typedValue.getValue());
          assert columnIndex < dataRow.size() : "Unexpected inconsistent row " + dataRow.toString();

          McfParser.SplitAndStripArg ssArg = new McfParser.SplitAndStripArg();
          ssArg.delimiter = delimiter;
          ssArg.includeEmpty = false;
          ssArg.stripEnclosingQuotes = false;
          // TODO: set stripEscapesBeforeQuotes
          List<String> values =
              McfParser.splitAndStripWithQuoteEscape(dataRow.get(columnIndex), ssArg);
          for (String value : values) {
            McfParser.parseTypedValue(
                Mcf.McfType.INSTANCE_MCF,
                false,
                currentNode,
                currentProp,
                value,
                instanceValues.addTypedValuesBuilder());
          }
        } else {
          // Pass through constant value.
          instanceValues.addTypedValues(typedValue);
        }
      }
      return instanceValues.build();
    }

    private String toNodeName(String entityId) {
      if (entityToDcid.containsKey(entityId)) {
        return entityToDcid.get(entityId);
      }
      McfParser.SchemaTerm term = McfParser.parseSchemaTerm(entityId);
      // Already validated in template-MCF sanity check.
      assert term.type == McfParser.SchemaTerm.Type.ENTITY : "Unexpected non-entity " + entityId;
      return term.table + "/" + term.value + "/" + rowId;
    }

    private int toColumnIndex(String columnName) {
      McfParser.SchemaTerm term = McfParser.parseSchemaTerm(columnName);
      // Already validated in template-MCF sanity check.
      assert term.type == McfParser.SchemaTerm.Type.COLUMN : "Unexpected non-column " + columnName;
      assert csvParser.getHeaderMap().containsKey(term.value) : "Missing column " + term.value;
      return csvParser.getHeaderMap().get(term.value);
    }
  }

  public Mcf.McfGraph parseNextRow() {
    if (!csvParser.iterator().hasNext()) {
      return null;
    }
    RowProcessor processor = new RowProcessor();
    processor.process(csvParser.iterator().next());
    return processor.instanceMcf();
  }

  private void incrementCounter(String counter, Debug.Log.Builder logCtx) {
    incrementCounterBy(counter, 1, logCtx);
  }

  private void incrementCounterBy(String counter, int incr, Debug.Log.Builder logCtx) {
    Long c = Long.valueOf(incr);
    if (logCtx.getCounterSet().getCountersMap().containsKey(counter)) {
      c = logCtx.getCounterSet().getCountersMap().get(counter) + Long.valueOf(incr);
    }
    logCtx.getCounterSetBuilder().getCountersMap().put(counter, c);
  }

  public boolean TEST_mode = false;
}
