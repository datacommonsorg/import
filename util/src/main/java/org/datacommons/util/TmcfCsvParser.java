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
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfUtil.LogCb;

// Converts a Template MCF file and an associated CSV into instance MCF.
//
// NOTE: Sets the location file in LogWrapper (at different times to point to TMCF and CSV).
// TODO: Add more column info in generated MCF, even when values are missing.
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
    logCtx.setLocationFile(tmcfFile);
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

    logCtx.setLocationFile(csvFile);
    // Clean and keep a copy of the header map.
    if (tmcfCsvParser.csvParser.getHeaderMap() == null) {
      tmcfCsvParser.logCtx.addEntry(
          Debug.Log.Level.LEVEL_FATAL,
          "CSV_HeaderFailure",
          "Unable to parse header from CSV file :: file: '" + csvFile + "'",
          0);
      return null;
    }
    // Check TMCF.
    boolean success =
        McfChecker.check(
            tmcfCsvParser.tmcf, tmcfCsvParser.csvParser.getHeaderMap().keySet(), logCtx);
    if (!success) {
      // Set location file to make sure log entry refers to the tmcf file.
      tmcfCsvParser.logCtx.setLocationFile(tmcfFile);
      tmcfCsvParser.logCtx.addEntry(
          Debug.Log.Level.LEVEL_FATAL,
          "CSV_TmcfCheckFailure",
          "Found fatal sanity error in TMCF; check Sanity_ counter messages :: TMCF-file: "
              + Path.of(tmcfFile).getFileName().toString(),
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
    private HashMap<String, Mcf.McfGraph.TypedValue> entityToDcid;
    private String currentNodeId;
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
            "Found CSV row with different number of columns");
        return;
      }

      LogCb logCb =
          new LogCb(logCtx, Debug.Log.Level.LEVEL_ERROR, csvParser.getCurrentLineNumber());

      // Process DCIDs from all the nodes first and add to entityToDcid map, which will be consulted
      // to resolve entity references in processValues() function.
      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        Map<String, Mcf.McfGraph.Values> pvs = tableEntity.getValue().getPvsMap();
        if (!pvs.containsKey(Vocabulary.DCID)) continue;

        // Register the fact that the user has mapped dcid, in case we continue below.
        // TODO: Maybe this should move to after currentNodeId setting below.
        entityToDcid.put(tableEntity.getKey(), Mcf.McfGraph.TypedValue.newBuilder().build());
        logCb.setDetail(LogCb.VALUE_KEY, tableEntity.getKey());
        currentNodeId = toNodeName(tableEntity.getKey(), logCb);
        if (currentNodeId == null) continue;

        currentProp = Vocabulary.DCID;
        Mcf.McfGraph.Values dcidValues =
            parseValues(tableEntity.getKey(), currentProp, pvs.get(currentProp), dataRow);
        if (dcidValues.getTypedValuesList().size() == 0) {
          // In this case parseValues must have logged accurate msg.
          continue;
        }
        Mcf.McfGraph.TypedValue tv = dcidValues.getTypedValues(0);
        if (tv.getType() == Mcf.ValueType.TEXT || tv.getType() == Mcf.ValueType.RESOLVED_REF) {
          entityToDcid.put(tableEntity.getKey(), tv);
        } else {
          addLog(
              Debug.Log.Level.LEVEL_WARNING,
              "CSV_MalformedDCIDFailures",
              "Malformed CSV value for dcid property; must be a text or reference :: value: '"
                  + tv.getValue()
                  + "', node: '"
                  + tableEntity.getKey()
                  + "'");
          logCtx.incrementCounterBy("CSV_MalformedDCIDPVFailures", pvs.size());
        }
      }

      for (Map.Entry<String, Mcf.McfGraph.PropertyValues> tableEntity :
          tmcf.getNodesMap().entrySet()) {
        logCb.setDetail(LogCb.VALUE_KEY, tableEntity.getKey());
        currentNodeId = toNodeName(tableEntity.getKey(), logCb);
        if (currentNodeId == null) continue;
        // Case of malformed/empty DCID. SKip this node (counters were updated above).
        if (currentNodeId.equals(Vocabulary.DCID_PREFIX)) continue;

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
        nodeBuilder.setTemplateNode(tableEntity.getKey());
        Debug.Log.Location.Builder loc = nodeBuilder.addLocationsBuilder();
        loc.setFile(logCtx.getLocationFile());
        loc.setLineNumber(csvParser.getCurrentLineNumber());

        Mcf.McfGraph.PropertyValues newNode = nodeBuilder.build();
        boolean success =
            McfChecker.check(Mcf.McfType.INSTANCE_MCF, currentNodeId, newNode, logCtx);
        if (success) {
          logCtx.incrementCounterBy("NumNodeSuccesses", 1);
          logCtx.incrementCounterBy("NumPVSuccesses", newNode.getPvsCount());
          instanceMcf.putNodes(currentNodeId, newNode);
        }
      }
    }

    private Mcf.McfGraph.Values parseValues(
        String templateEntity,
        String currentProp,
        Mcf.McfGraph.Values templateValues,
        CSVRecord dataRow) {
      Mcf.McfGraph.Values.Builder instanceValues = Mcf.McfGraph.Values.newBuilder();

      // Used for parseSchemaTerm() and splitAndStripWithQuoteEscape()
      LogCb errCb =
          new LogCb(logCtx, Debug.Log.Level.LEVEL_ERROR, csvParser.getCurrentLineNumber())
              .setDetail(LogCb.PROP_KEY, currentProp)
              .setDetail(LogCb.NODE_KEY, templateEntity);
      LogCb warnCb =
          new LogCb(logCtx, Debug.Log.Level.LEVEL_WARNING, csvParser.getCurrentLineNumber())
              .setDetail(LogCb.PROP_KEY, currentProp)
              .setDetail(LogCb.NODE_KEY, templateEntity);

      for (Mcf.McfGraph.TypedValue typedValue : templateValues.getTypedValuesList()) {
        if (typedValue.getType() == Mcf.ValueType.TABLE_ENTITY) {
          if (currentProp.equals(Vocabulary.DCID)) {
            // TODO: Add this check to checkTemplateNode(), and assert here
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "TMCF_TmcfEntityAsDcid",
                "Value of dcid property is an 'E:' entity; must be a 'C:' column or "
                    + "a constant :: value: '"
                    + templateEntity
                    + "'");
            continue;
          }
          errCb.setDetail(LogCb.VALUE_KEY, typedValue.getValue());
          String referenceNode = toNodeName(typedValue.getValue(), errCb);
          Mcf.McfGraph.TypedValue.Builder newTypedValue = Mcf.McfGraph.TypedValue.newBuilder();
          if (referenceNode.startsWith(Vocabulary.DCID_PREFIX)) {
            Mcf.McfGraph.TypedValue dcidTypedVal =
                entityToDcid.getOrDefault(
                    typedValue.getValue(), Mcf.McfGraph.TypedValue.getDefaultInstance());
            if (dcidTypedVal.getValue().isEmpty()) {
              addLog(
                  Debug.Log.Level.LEVEL_WARNING,
                  "CSV_EmptyDcidReferences",
                  "In dcid:{entity} reference, found {entity} to be empty :: property: '"
                      + currentProp
                      + "', node: '"
                      + templateEntity
                      + "'");
              continue;
            }
            newTypedValue.setType(Mcf.ValueType.RESOLVED_REF);
            newTypedValue.setValue(dcidTypedVal.getValue());
            if (dcidTypedVal.hasColumn()) {
              newTypedValue.setColumn(dcidTypedVal.getColumn());
            }
          } else {
            // This is an internal reference, so prefix "l:"
            newTypedValue.setType(Mcf.ValueType.UNRESOLVED_REF);
            newTypedValue.setValue(Vocabulary.INTERNAL_REF_PREFIX + referenceNode);
          }
          instanceValues.addTypedValues(newTypedValue.build());
        } else if (typedValue.getType() == Mcf.ValueType.TABLE_COLUMN) {
          // Replace column-name with cell-value
          errCb.setDetail(LogCb.VALUE_KEY, typedValue.getValue());
          McfParser.SchemaTerm term = McfParser.parseSchemaTerm(typedValue.getValue(), errCb);
          if (term == null) {
            continue;
          }
          if (term.type != McfParser.SchemaTerm.Type.COLUMN) {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "TMCF_UnexpectedNonColumn",
                "Expected value to be a TMCF column that starts with 'C:' :: value: '"
                    + typedValue.getValue()
                    + "', property: '"
                    + currentProp
                    + "', node: '"
                    + templateEntity
                    + "'");
            continue;
          }
          String column = term.value;
          if (!cleanedColumnMap.containsKey(column)) {
            addLog(
                Debug.Log.Level.LEVEL_ERROR,
                "CSV_TmcfMissingColumn",
                "Column referred to in TMCF is missing from CSV header :: column: '"
                    + column
                    + "'");
            continue;
          }
          int columnIndex = cleanedColumnMap.get(column);
          if (columnIndex >= dataRow.size()) {
            addLog(
                Debug.Log.Level.LEVEL_WARNING,
                "CSV_UnexpectedRow",
                "Found row with fewer columns than expected :: row: '" + dataRow.toString() + "'");
            continue;
          }

          McfParser.SplitAndStripArg ssArg = new McfParser.SplitAndStripArg();
          ssArg.delimiter = delimiter;
          ssArg.includeEmpty = false;
          ssArg.stripEnclosingQuotes = false;
          // TODO: set stripEscapesBeforeQuotes
          String origValue = dataRow.get(columnIndex);
          warnCb.setDetail(LogCb.VALUE_KEY, origValue);
          warnCb.setDetail(LogCb.COLUMN_KEY, column);
          warnCb.setCounterSuffix(currentProp);
          List<String> values = McfParser.splitAndStripWithQuoteEscape(origValue, ssArg, warnCb);
          for (String value : values) {
            Mcf.McfGraph.TypedValue.Builder newTypedValue = instanceValues.addTypedValuesBuilder();
            errCb.setDetail(LogCb.VALUE_KEY, value);
            McfParser.parseTypedValue(
                Mcf.McfType.INSTANCE_MCF,
                false,
                currentNodeId,
                currentProp,
                value,
                newTypedValue,
                errCb);
            newTypedValue.setColumn(column);
          }
        } else {
          // Pass through constant value.
          instanceValues.addTypedValues(typedValue);
        }
      }
      return instanceValues.build();
    }

    private String toNodeName(String entityId, LogCb logCb) {
      if (entityToDcid.containsKey(entityId)) {
        return Vocabulary.DCID_PREFIX + entityToDcid.get(entityId).getValue();
      }

      McfParser.SchemaTerm term = McfParser.parseSchemaTerm(entityId, logCb);
      if (term == null) return null;
      if (term.type != McfParser.SchemaTerm.Type.ENTITY) {
        // TODO: Consider making this an assertion failure
        logCb.logError(
            "CSV_UnexpectedNonEntity", "Expected value to be a TMCF entity that starts with 'E:'");
        return null;
      }

      return term.table + "/" + term.value + "/" + rowId;
    }
  }

  private void addLog(Debug.Log.Level level, String counter, String message) {
    logCtx.addEntry(level, counter, message, csvParser.getCurrentLineNumber());
  }
}
