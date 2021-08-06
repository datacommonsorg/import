// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BiConsumer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;

// Converts a Template MCF file and an associated CSV into instance MCF.
public class TmcfCsvParser {
  public static boolean TEST_mode = false;

  private Mcf.McfGraph tmcf;
  private char delimiter;
  private CSVParser csvParser;
  private LogWrapper logCtx;
  private HashMap<String, Integer> cleanedColumnMap;

  // Build a parser given a TMCF file, CSV file, CSV delimiter and a log context.
  public static TmcfCsvParser init(
      String tmcfFile, String csvFile, char delimiter, LogWrapper logCtx) throws IOException {
    TmcfCsvParser tmcfCsvParser = new TmcfCsvParser();
    tmcfCsvParser.tmcf = McfParser.parseTemplateMcfFile(tmcfFile, logCtx);
    tmcfCsvParser.logCtx = logCtx;
    tmcfCsvParser.csvParser =
        CSVParser.parse(
            new FileReader(csvFile),
            CSVFormat.DEFAULT
                .withDelimiter(delimiter)
                .withEscape('\\')
                .withHeader()
                .withSkipHeaderRecord()
                .withIgnoreEmptyLines()
                .withIgnoreSurroundingSpaces());
    tmcfCsvParser.delimiter = delimiter;

    // Clean and keep a copy of the header map.
    if (tmcfCsvParser.csvParser.getHeaderMap() == null) {
      tmcfCsvParser.logCtx.addEntry(
          Debug.Log.Level.LEVEL_FATAL,
          "CSV_HeaderFailure",
          "Unable to parse header from file " + csvFile,
          0);
      return null;
    }
    tmcfCsvParser.cleanedColumnMap = new HashMap<>();
    for (Map.Entry<String, Integer> e : tmcfCsvParser.csvParser.getHeaderMap().entrySet()) {
      tmcfCsvParser.cleanedColumnMap.put(e.getKey().strip(), e.getValue());
    }
    return tmcfCsvParser;
  }

  // Parse the next row from the CSV. Returns null on EOF.
  public Mcf.McfGraph parseNextRow() {
    if (!csvParser.iterator().hasNext()) {
      return null;
    }
    RowProcessor processor = new RowProcessor();
    processor.process(csvParser.iterator().next());
    return processor.instanceMcf();
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
      entityToDcid = new HashMap<>();
      rowId =
          TEST_mode
              ? String.valueOf(csvParser.getCurrentLineNumber() - 1)
              : UUID.randomUUID().toString();
    }

    public Mcf.McfGraph instanceMcf() {
      return instanceMcf.build();
    }

    public void process(CSVRecord dataRow) {
      if (!dataRow.isConsistent()) {
        addLog(
            Debug.Log.Level.LEVEL_ERROR,
            "CSV_InconsistentRows",
            "Found inconsistent row with different number of columns");
        return;
      }

      BiConsumer<String, String> errCb =
          (counter, message) -> {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                counter,
                "Failed to parse TMCF entity term: " + message);
          };
      // Process DCIDs from all the nodes first and add to entityToDcid map, which will be consulted
      // to resolve entity references in processValues() function.
      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        Map<String, Mcf.McfGraph.Values> pvs = tableEntity.getValue().getPvsMap();
        if (!pvs.containsKey(Vocabulary.DCID)) continue;

        // Register the fact that the user has mapped dcid, in case we continue below.
        entityToDcid.put(tableEntity.getKey(), "");

        currentNode = toNodeName(tableEntity.getKey(), errCb);
        if (currentNode == null) continue;

        currentProp = Vocabulary.DCID;
        Mcf.McfGraph.Values dcidValues =
            parseValues(tableEntity.getKey(), currentProp, pvs.get(currentProp), dataRow);
        if (dcidValues.getTypedValuesList().size() == 0) {
          // In this case parseValues must have logged accurate msg.
          continue;
        }
        Mcf.McfGraph.TypedValue tv = dcidValues.getTypedValues(0);
        if (tv.getType() == Mcf.ValueType.TEXT || tv.getType() == Mcf.ValueType.RESOLVED_REF) {
          entityToDcid.put(tableEntity.getKey(), tv.getValue());
        } else {
          addLog(
              Debug.Log.Level.LEVEL_WARNING,
              "CSV_MalformedDCIDFailures",
              "Malformed CSV value malformed for entity "
                  + tableEntity.getKey()
                  + ": "
                  + tv.getValue());
          logCtx.incrementCounterBy("CSV_MalformedDCIDPVFailures", pvs.size());
        }
      }

      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        currentNode = toNodeName(tableEntity.getKey(), errCb);
        if (currentNode == null) continue;
        // Case of malformed/empty DCID. SKip this node (counters were updated above).
        if (currentNode.equals(Vocabulary.DCID_PREFIX)) continue;

        // Go over each property within the template.
        Mcf.McfGraph.PropertyValues.Builder nodeBuilder = Mcf.McfGraph.PropertyValues.newBuilder();
        for (Map.Entry<String, Mcf.McfGraph.Values> pv :
            tableEntity.getValue().getPvsMap().entrySet()) {
          currentProp = pv.getKey();

          // Don't process functionalDeps
          if (currentProp.equals(Vocabulary.FUNCTIONAL_DEPS)) continue;

          // Replace column names with values
          Mcf.McfGraph.Values values =
              parseValues(tableEntity.getKey(), currentProp, pv.getValue(), dataRow);
          if (values.getTypedValuesCount() == 0) {
            // In this case parseValues must have logged accurate msg.
            continue;
          }
          nodeBuilder.putPvs(currentProp, values);
        }
        // TODO: Add node sanity check
        instanceMcf.putNodes(currentNode, nodeBuilder.build());
      }
    }

    private Mcf.McfGraph.Values parseValues(
        String templateEntity,
        String currentProp,
        Mcf.McfGraph.Values templateValues,
        CSVRecord dataRow) {
      Mcf.McfGraph.Values.Builder instanceValues = Mcf.McfGraph.Values.newBuilder();

      // Used for parseSchemaTerm() and splitAndStripWithQuoteEscape()
      BiConsumer<String, String> errCb =
          (counter, message) -> {
            addLog(Debug.Log.Level.LEVEL_ERROR, counter, message);
          };
      BiConsumer<String, String> warnCb =
          (counter, message) -> {
            addLog(Debug.Log.Level.LEVEL_WARNING, counter, message);
          };

      for (Mcf.McfGraph.TypedValue typedValue : templateValues.getTypedValuesList()) {
        if (typedValue.getType() == Mcf.ValueType.TABLE_ENTITY) {
          if (currentProp.equals(Vocabulary.DCID)) {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "CSV_TmcfEntityAsDcid",
                "dcid property in TMCF entity " + templateEntity + " must not be an entity");
            continue;
          }
          String referenceNode = toNodeName(typedValue.getValue(), errCb);
          Mcf.McfGraph.TypedValue.Builder newTypedValue = Mcf.McfGraph.TypedValue.newBuilder();
          if (referenceNode.startsWith(Vocabulary.DCID_PREFIX)) {
            String dcid = referenceNode.substring(Vocabulary.DCID_PREFIX.length());
            if (dcid.isEmpty()) {
              addLog(
                  Debug.Log.Level.LEVEL_WARNING,
                  "CSV_EmptyDcidReferences",
                  "In dcid:{entity} reference, found {entity} to be empty for property "
                      + currentProp
                      + " of TMCF entity "
                      + templateEntity);
              continue;
            }
            newTypedValue.setType(Mcf.ValueType.RESOLVED_REF);
            newTypedValue.setValue(dcid);
          } else {
            // This is an internal reference, so prefix "l:"
            newTypedValue.setType(Mcf.ValueType.UNRESOLVED_REF);
            newTypedValue.setValue(Vocabulary.INTERNAL_REF_PREFIX + referenceNode);
          }
          instanceValues.addTypedValues(newTypedValue.build());
        } else if (typedValue.getType() == Mcf.ValueType.TABLE_COLUMN) {
          // Replace column-name with cell-value
          McfParser.SchemaTerm term = McfParser.parseSchemaTerm(typedValue.getValue(), errCb);
          if (term == null) {
            continue;
          }
          if (term.type != McfParser.SchemaTerm.Type.COLUMN) {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "CSV_UnexpectedNonColumn",
                "Unable to parse TMCF column "
                    + typedValue.getValue()
                    + " in property "
                    + currentProp
                    + " of TMCF entity "
                    + templateEntity);
            continue;
          }
          if (!cleanedColumnMap.containsKey(term.value)) {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "CSV_MissingTmcfColumn",
                "Column " + term.value + " referred in TMCF is missing from CSV header");
            continue;
          }
          int columnIndex = cleanedColumnMap.get(term.value);
          if (columnIndex >= dataRow.size()) {
            addLog(
                Debug.Log.Level.LEVEL_WARNING,
                "CSV_UnexpectedRow",
                "Fewer columns than expected in the row: '" + dataRow.toString() + "'");
            continue;
          }

          McfParser.SplitAndStripArg ssArg = new McfParser.SplitAndStripArg();
          ssArg.delimiter = delimiter;
          ssArg.includeEmpty = false;
          ssArg.stripEnclosingQuotes = false;
          ssArg.context = "CSV column " + term.value;
          // TODO: set stripEscapesBeforeQuotes
          List<String> values =
              McfParser.splitAndStripWithQuoteEscape(dataRow.get(columnIndex), ssArg, warnCb);
          for (String value : values) {
            McfParser.parseTypedValue(
                Mcf.McfType.INSTANCE_MCF,
                false,
                currentNode,
                currentProp,
                value,
                instanceValues.addTypedValuesBuilder(),
                errCb);
          }
        } else {
          // Pass through constant value.
          instanceValues.addTypedValues(typedValue);
        }
      }
      return instanceValues.build();
    }

    private String toNodeName(String entityId, BiConsumer<String, String> errCb) {
      if (entityToDcid.containsKey(entityId)) {
        return Vocabulary.DCID_PREFIX + entityToDcid.get(entityId);
      }

      McfParser.SchemaTerm term = McfParser.parseSchemaTerm(entityId, errCb);
      if (term == null) return null;
      if (term.type != McfParser.SchemaTerm.Type.ENTITY) {
        errCb.accept(
            "CSV_UnexpectedNonEntity", "Unexpected parsing error in TMCF entity " + entityId);
        return null;
      }

      return term.table + "/" + term.value + "/" + rowId;
    }
  }

  private void addLog(Debug.Log.Level level, String counter, String message) {
    logCtx.addEntry(level, counter, message, csvParser.getCurrentLineNumber());
  }
}
