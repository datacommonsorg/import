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

import static org.datacommons.proto.Mcf.ValueType.RESOLVED_REF;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.McfUtil.LogCb;

// A parser for converting text in Instance or Template MCF format into the McfGraph proto.
//
// NOTE: Expects caller to set location file in LogWrapper.
public class McfParser {
  private static final Logger logger = LogManager.getLogger(McfParser.class);

  private McfGraph.Builder graph;
  private LogWrapper logCtx;
  private boolean isResolved;
  private String curEntity;
  private long lineNum = 0;
  private int curEntityLineIdx = 0;
  private String prevEntity;
  private boolean finished = false;
  private Iterator<String> lines;

  // Create an McfParser instance based on type and a bool indicating whether the MCF is resolved
  // (DCIDs assigned).
  public static McfParser init(
      Mcf.McfType type, String fileName, boolean isResolved, LogWrapper logCtx) throws IOException {
    McfParser parser = init(type, isResolved);
    parser.logCtx = logCtx;
    parser.lines =
        Files.lines(FileSystems.getDefault().getPath(fileName), StandardCharsets.UTF_8).iterator();
    return parser;
  }

  // Parse a string with instance nodes in MCF format into the McfGraph proto.
  public static McfGraph parseInstanceMcfString(
      String mcfString, boolean isResolved, LogWrapper logCtx) throws IOException {
    return parseMcfString(mcfString, Mcf.McfType.INSTANCE_MCF, isResolved, logCtx);
  }

  // Parse a file with instance nodes in MCF format into the McfGraph proto.
  public static McfGraph parseInstanceMcfFile(
      String fileName, boolean isResolved, LogWrapper logCtx) throws IOException {
    return parseMcfFile(fileName, Mcf.McfType.INSTANCE_MCF, isResolved, logCtx);
  }

  // Parse a string with template nodes in MCF format into the McfGraph proto.
  public static McfGraph parseTemplateMcfString(String mcfString, LogWrapper logCtx)
      throws IOException {
    return parseMcfString(mcfString, Mcf.McfType.TEMPLATE_MCF, false, logCtx);
  }

  // Parse a file with template nodes in MCF format into the McfGraph proto.
  public static McfGraph parseTemplateMcfFile(String fileName, LogWrapper logCtx)
      throws IOException {
    return parseMcfFile(fileName, Mcf.McfType.TEMPLATE_MCF, false, logCtx);
  }

  public McfGraph parseNextNode() throws IOException {
    if (finished) return null;
    while (lines.hasNext()) {
      String line = lines.next();
      lineNum++;
      parseLine(line);
      McfGraph g = extractNode();
      if (g != null) {
        return McfUtil.mergeGraphs(Collections.singletonList(g));
      }
    }
    // End of file.
    McfGraph g = finish();
    if (g == null) return null;
    return McfUtil.mergeGraphs(Collections.singletonList(g));
  }

  // Parse a line of MCF file.
  private void parseLine(String line) throws AssertionError {
    if (finished) {
      throw new AssertionError("Calling after finish()");
    }
    line = line.trim();

    // Skip empty lines and comments.
    if (line.isEmpty() || line.startsWith("//") || line.startsWith("#")) {
      return;
    }

    // Split across the first ':' (but not at the colon in an HTTP url)
    int prefixLen = 0;
    if (line.startsWith("http:")) {
      prefixLen = 5;
    } else if (line.startsWith("https:")) {
      prefixLen = 6;
    }
    int colon = line.substring(prefixLen).indexOf(Vocabulary.REFERENCE_DELIMITER);
    if (colon < 1) {
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_MalformedColonLessLine",
          "Malformed line without a colon delimiter :: line: '" + line + "'",
          lineNum);
      return;
    }

    String lhs = line.substring(0, colon).trim();
    String rhs = line.substring(colon + 1).trim();
    if (lhs.equals(Vocabulary.NODE)) {
      if (rhs.indexOf(',') != -1) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "MCF_MalformedNodeName",
            "Found malformed Node value with a comma; must be a unary value :: node: '" + rhs + "'",
            lineNum);
        return;
      }
      if (rhs.startsWith("\"")) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "MCF_MalformedNodeName",
            "Found malformed Node value with quotes; must be a non-quoted value :: node: '"
                + rhs
                + "'",
            lineNum);
        return;
      }
      if (graph.getType() == Mcf.McfType.TEMPLATE_MCF) {
        LogCb logCb = getLogCb().setDetail(LogCb.VALUE_KEY, rhs);
        SchemaTerm term = parseSchemaTerm(rhs, logCb);
        if (term.type != SchemaTerm.Type.ENTITY) {
          logCtx.addEntry(
              Debug.Log.Level.LEVEL_ERROR,
              "TMCF_MalformedEntity",
              "Found malformed entity name that is not an entity prefix (E:) :: name: '"
                  + rhs
                  + "'",
              lineNum);
          return;
        }
      } else {
        // New entity scope begins.
        parseNodeName(rhs);
      }
      prevEntity = curEntity;
      curEntity = rhs;
      curEntityLineIdx = 0;
      addNodeLocation();
    } else {
      if (curEntity.isEmpty()) {
        logCtx.addEntry(
            Debug.Log.Level.LEVEL_ERROR,
            "MCF_UnexpectedProperty",
            "Property found without a preceding line with 'Node' :: line: '" + line + "'",
            lineNum);
        return;
      }
      parseValues(lhs, rhs);

      curEntityLineIdx++;
    }
  }

  private void addNodeLocation() {
    assert !curEntity.isEmpty();
    assert curEntityLineIdx == 0;

    McfGraph.PropertyValues.Builder pvs =
        graph
            .getNodesOrDefault(curEntity, McfGraph.PropertyValues.getDefaultInstance())
            .toBuilder();
    Debug.Log.Location.Builder location = pvs.addLocationsBuilder();
    location.setFile(logCtx.getLocationFile());
    location.setLineNumber(lineNum);
    graph.putNodes(curEntity, pvs.build());
  }

  private static McfParser init(Mcf.McfType type, boolean isResolved) {
    McfParser parser = new McfParser();
    parser.graph = McfGraph.newBuilder();
    parser.graph.setType(type);
    parser.isResolved = isResolved;
    parser.curEntity = "";
    parser.prevEntity = "";
    return parser;
  }

  // Extracts and returns the previous full node, if any, as a single-node McfGraph proto. This is
  // typically used to extract single-node graphs.
  private McfGraph extractNode() throws IllegalArgumentException {
    // If we are at the beginning or not at node boundary, nothing to extract.
    if (prevEntity.length() == 0 || curEntityLineIdx != 0) {
      return null;
    }
    McfGraph.Builder node = McfGraph.newBuilder();
    node.setType(graph.getType());
    node.putNodes(prevEntity, graph.getNodesOrThrow(prevEntity));
    graph.removeNodes(prevEntity);
    prevEntity = "";
    return node.build();
  }

  // To be called after processing all lines of an MCF file (by calling parseLine()).
  private McfGraph finish() throws AssertionError {
    if (finished) {
      throw new AssertionError("Called finish() more than once!");
    }
    finished = true;
    if (curEntity.length() == 0) {
      return null;
    }
    if (curEntityLineIdx == 0) {
      // TODO: This should happen on seeing a new node too.
      logCtx.addEntry(
          Debug.Log.Level.LEVEL_ERROR,
          "MCF_MalformedNode",
          "Found a 'Node' without properties :: node: '" + curEntity + "'",
          lineNum);
    }
    return graph.build();
  }

  private static McfGraph parseMcfString(
      String mcfString, Mcf.McfType type, boolean isResolved, LogWrapper logCtx)
      throws IOException {
    McfParser parser = McfParser.init(type, isResolved);
    parser.logCtx = logCtx;
    parser.lines = Arrays.asList(mcfString.split("\\r?\\n")).iterator();
    return parser.parseLines();
  }

  private static McfGraph parseMcfFile(
      String fileName, Mcf.McfType type, boolean isResolved, LogWrapper logCtx) throws IOException {
    McfParser parser = McfParser.init(type, fileName, isResolved, logCtx);
    return parser.parseLines();
  }

  private McfGraph parseLines() throws IOException {
    McfGraph g;
    ArrayList<McfGraph> graphs = new ArrayList<>();
    while ((g = parseNextNode()) != null) {
      graphs.add(g);
    }
    return McfUtil.mergeGraphs(graphs);
  }

  private void parseNodeName(String node) {
    if (Vocabulary.isGlobalReference(node)) {
      String prop = Vocabulary.DCID;
      McfGraph.PropertyValues.Builder pvs =
          graph.getNodesOrDefault(node, McfGraph.PropertyValues.getDefaultInstance()).toBuilder();
      McfGraph.Values.Builder vals =
          pvs.getPvsOrDefault(prop, McfGraph.Values.getDefaultInstance()).toBuilder();
      McfGraph.TypedValue.Builder tval = vals.addTypedValuesBuilder();
      tval.setValue(node.substring(node.indexOf(Vocabulary.REFERENCE_DELIMITER) + 1));
      tval.setType(Mcf.ValueType.TEXT);
      pvs.putPvs(prop, vals.build());
      graph.putNodes(node, pvs.build());
    }
  }

  private void parseValues(String prop, String values) {
    if (prop.isEmpty() || values.isEmpty()) return;

    McfGraph.PropertyValues.Builder pvs =
        graph
            .getNodesOrDefault(curEntity, McfGraph.PropertyValues.getDefaultInstance())
            .toBuilder();
    McfGraph.Values.Builder vals =
        pvs.getPvsOrDefault(prop, McfGraph.Values.getDefaultInstance()).toBuilder();
    if (prop.equals(Vocabulary.DCID) && Vocabulary.isGlobalReference(curEntity)) {
      vals.clearTypedValues();
    }

    SplitAndStripArg ssArg = new SplitAndStripArg();
    ssArg.delimiter = Vocabulary.VALUE_SEPARATOR;
    ssArg.includeEmpty = false;
    ssArg.stripEnclosingQuotes = false;
    LogCb logCb =
        getLogCb()
            .setDetail(LogCb.PROP_KEY, prop)
            .setDetail(LogCb.VALUE_KEY, values)
            .setDetail(LogCb.NODE_KEY, curEntity)
            .setCounterSuffix(prop);
    List<String> fields = splitAndStripWithQuoteEscape(values, ssArg, logCb);
    logCb.setCounterSuffix("");
    for (String field : fields) {
      logCb.setDetail(LogCb.VALUE_KEY, field);
      parseTypedValue(
          graph.getType(), isResolved, curEntity, prop, field, vals.addTypedValuesBuilder(), logCb);
    }
    pvs.putPvs(prop, vals.build());
    graph.putNodes(curEntity, pvs.build());
  }

  private LogCb getLogCb() {
    return new LogCb(logCtx, Debug.Log.Level.LEVEL_ERROR, lineNum);
  }

  public static void parseTypedValue(
      Mcf.McfType mcfType,
      boolean isResolved,
      String node,
      String prop,
      String val,
      McfGraph.TypedValue.Builder tval,
      LogCb logCb) {
    if (mcfType == Mcf.McfType.TEMPLATE_MCF) {
      if (prop.equals("C")) {
        logCb.logError(
            "TMCF_UnsupportedColumnNameInProperty",
            "TMCF properties cannot refer to CSV columns yet");
        return;
      }
      SchemaTerm term = parseSchemaTerm(val, logCb);
      if (term == null) return;
      if (term.type == SchemaTerm.Type.ENTITY) {
        tval.setValue(val);
        tval.setType(Mcf.ValueType.TABLE_ENTITY);
        return;
      } else if (term.type == SchemaTerm.Type.COLUMN) {
        tval.setValue(val);
        tval.setType(Mcf.ValueType.TABLE_COLUMN);
        return;
      }
      // Fallthrough...
    }

    boolean expectRef = Vocabulary.isReferenceProperty(prop);

    if (val.startsWith("\"")) {
      // If a value starts with double quotes, consider it a string.
      //
      // Strip enclosing double quotes.
      val = stripEnclosingQuotePair(val);

      if (!expectRef) {
        tval.setValue(val);
        tval.setType(Mcf.ValueType.TEXT);
        return;
      }
    }

    if (val.startsWith("[")) {
      if (!val.endsWith("]")) {
        logCb.logError(
            "MCF_MalformedComplexValue",
            "Found malformed Complex value without a closing ] bracket");
        return;
      }
      tval.setValue(val);
      tval.setType(Mcf.ValueType.COMPLEX_VALUE);
      return;
    }

    int colon = val.indexOf(Vocabulary.REFERENCE_DELIMITER);
    if (colon != -1) {
      // This maybe a reference, or something with an unfortunate ":" in it which
      // the user failed to encapsulate in quotes (e.g., url: http://goo.gl).
      if (Vocabulary.isGlobalReference(val)) {
        // Strip the prefix and set the value.
        tval.setValue(val.substring(colon + 1));
        tval.setType(RESOLVED_REF);
        return;
      } else if (Vocabulary.isInternalReference(val)) {
        if (isResolved) {
          logCb.logError(
              "MCF_LocalReferenceInResolvedFile",
              "Found an internal 'l:' reference in resolved entity value");
          return;
        }
        tval.setValue(val);
        tval.setType(Mcf.ValueType.UNRESOLVED_REF);
        return;
      }
      // Fallthrough...
    }

    if (expectRef) {
      // If we're here, user likely forgot to add a "dcid:", "dcs:" or "schema:"
      // prefix.
      //
      // NOTE: We cannot tell apart if they failed to add an internal reference
      // prefix ("l:"), but we err on the side of user being careful about adding
      // local refs and accept the MCF without failing.
      tval.setValue(val);
      tval.setType(RESOLVED_REF);
      return;
    }

    if (McfUtil.isNumber(val) || McfUtil.isBool(val)) {
      // This parses to a number or bool.
      tval.setValue(val);
      tval.setType(Mcf.ValueType.NUMBER);
      return;
    }

    // Instead of failing an unquoted value, treat it as a string.
    //
    // NOTE: This could be an uncommon reference property that user forgot to add
    // a "dcid:" prefix for. We err on the side of accepting the value as a string
    // instead of failing.
    tval.setValue(val);
    tval.setType(Mcf.ValueType.TEXT);
  }

  // Parses a token into a schema term.  This is relevant for values in a template MCF format.
  public static final class SchemaTerm {
    public enum Type {
      COLUMN,
      ENTITY,
      CONSTANT
    }

    public Type type;
    // For CONSTANT, this is raw MCF value. For ENTITY and COLUMN it is the entity name (E1, E2) or
    // column name.
    public String value;
    // Set only when type != CONSTANT
    public String table;
  }

  public static SchemaTerm parseSchemaTerm(String value, LogCb logCb) {
    SchemaTerm term = new SchemaTerm();
    boolean isEntity = value.startsWith(Vocabulary.ENTITY_PREFIX);
    boolean isColumn = value.startsWith(Vocabulary.COLUMN_PREFIX);
    if (isEntity || isColumn) {
      term.type = isEntity ? SchemaTerm.Type.ENTITY : SchemaTerm.Type.COLUMN;
      String strippedValue =
          value.substring(
              isEntity ? Vocabulary.ENTITY_PREFIX.length() : Vocabulary.COLUMN_PREFIX.length());
      int delimiter = strippedValue.indexOf(Vocabulary.TABLE_DELIMITER);
      if (delimiter == -1) {
        logCb.logError(
            "TMCF_MalformedSchemaTerm",
            "Malformed " + (isEntity ? "entity" : "column") + " value; must have a '->' delimiter");
        return null;
      }
      term.table = strippedValue.substring(0, delimiter);
      term.value = strippedValue.substring(delimiter + Vocabulary.TABLE_DELIMITER.length());
    } else {
      term.type = SchemaTerm.Type.CONSTANT;
      term.value = value;
    }
    return term;
  }

  // Splits a string using the delimiter character. A field is not split if the delimiter is within
  // a pair of double
  // quotes. If "includeEmpty" is true, then empty field is included.
  //
  // For example "1,2,3" will be split into ["1","2","3"] but "'1,234',5" will be
  // split into ["1,234", "5"].
  // TODO: Support stripEscapesBeforeQuotes control like in internal version.
  // TODO: Move this to its own file.
  public static final class SplitAndStripArg {
    public char delimiter = ',';
    public boolean includeEmpty = false;
    public boolean stripEnclosingQuotes = true;
    public boolean stripEscapesBeforeQuotes = false;
  }

  // NOTE: We do not strip enclosing quotes in this function.
  public static List<String> splitAndStripWithQuoteEscape(
      String orig, SplitAndStripArg arg, LogCb logCb) throws AssertionError {
    List<String> results = new ArrayList<>();
    if (orig.contains("\n")) {
      if (logCb != null) {
        logCb.logError("StrSplit_MultiToken", "Found a new-line in value");
      }
      return results;
    }
    List<String> parts = new ArrayList<>();
    if (!StringUtil.SplitStructuredLineWithEscapes(orig, arg.delimiter, '"', parts)) {
      if (logCb != null) {
        logCb.logError(
            "StrSplit_BadQuotesInToken", "Found token with incorrectly double-quoted value");
      }
      return results;
    }
    for (String s : parts) {
      String ss = arg.stripEnclosingQuotes ? stripEnclosingQuotePair(s.trim()) : s.trim();
      // After stripping whitespace some terms could become empty.
      if (arg.includeEmpty || !ss.isEmpty()) {
        if (arg.stripEscapesBeforeQuotes) {
          // replace instances of \" with just "
          results.add(ss.replaceAll("\\\\\"", "\""));
        } else {
          results.add(ss);
        }
      }
    }
    if (results.isEmpty()) {
      if (logCb != null) {
        logCb.logError("StrSplit_EmptyToken", "Empty value found");
      }
      return results;
    }
    return results;
  }

  private static String stripEnclosingQuotePair(String val) {
    if (val.length() > 1) {
      if (val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
        return val.length() == 2 ? "" : val.substring(1, val.length() - 1);
      }
    }
    return val;
  }
}
