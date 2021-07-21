// Copyright 2019 Google LLC
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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

// A parser for converting text in Instance or Template MCF format into the McfGraph proto.
// TODO: Implement COMPLEX_VALUE parsing
// TODO: Implement TEMPLATE_MCF parsing
public class McfParser {
  private McfGraph.Builder graph;
  private boolean isResolved;
  private String curEntity;
  private int curEntityLineIdx = 0;
  private String prevEntity;
  private boolean finished = false;

  // Create an McfParser instance based on type and a bool indicating whether the MCF is resolved (DCIDs assigned).
  public static McfParser init(Mcf.McfType type, boolean isResolved) {
    McfParser parser = new McfParser();
    parser.graph = McfGraph.newBuilder();
    parser.graph.setType(type);
    parser.isResolved = isResolved;
    parser.curEntity = "";
    parser.prevEntity = "";
    return parser;
  }

  // Parse a string with nodes in MCF format into the McfGraph proto.
  public static McfGraph parseInstanceMcfString(String mcf_string, boolean isResolved) {
    McfParser parser = McfParser.init(Mcf.McfType.INSTANCE_MCF, isResolved);
    for (String l : mcf_string.split("\n")) {
      parser.parseLine(l);
    }
    return parser.finish();
  }

  // Parse a file with nodes in MCF format into the McfGraph proto.
  public static McfGraph parseInstanceMcfFile(String file_name, boolean isResolved) throws IOException {
    McfParser parser = McfParser.init(Mcf.McfType.INSTANCE_MCF, isResolved);
    Stream<String> lines = Files.lines(FileSystems.getDefault().getPath(file_name), StandardCharsets.UTF_8);
    lines.forEach(parser::parseLine);
    return parser.finish();
  }

  // Parse a line of MCF file.
  public void parseLine(String line) throws AssertionError {
    assert !finished;
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
    assert colon > 0 : "Found malformed line without a colon delimiter (" + line + ")";

    String lhs = line.substring(0, colon).trim();
    String rhs = line.substring(colon + 1).trim();
    if (lhs.equals(Vocabulary.NODE)) {
      assert rhs.indexOf(',') == -1 : "Found malformed 'Node' name (" + rhs +
              ") with comma. Node name must be a unary value.";
      assert !rhs.startsWith("\"") : "Found malformed 'Node' name (" + rhs +
              ") that includes quotes. Node name must be a non-quoted value.";
      // New entity scope begins.
      parseNodeName(rhs);

      prevEntity = curEntity;
      curEntity = rhs;
      curEntityLineIdx = 0;
    } else {
      assert !curEntity.isEmpty() : " Property found without a 'Node' term: " + line;
      parseValues(lhs, rhs);

      curEntityLineIdx++;
    }
  }

  // Returns true if we are at the boundary of a node after a sequence of parseLine() calls.
  public boolean isNodeBoundary() { return curEntityLineIdx == 0; }

  // Extracts and returns the previous node as a single-node McfGraph proto. This is typically used to
  // extract single-node graphs, and is called when isNodeBoundary() is true.
  public McfGraph extractPreviousNode() throws IllegalArgumentException {
    if (prevEntity.length() == 0) {
      return null;
    }
    McfGraph.Builder node = McfGraph.newBuilder();
    node.setType(graph.getType());
    node.putNodes(prevEntity, graph.getNodesOrThrow(prevEntity));
    graph.removeNodes(prevEntity);
    return node.build();
  }

  // To be called after processing all lines of an MCF file (by calling parseLine()).
  public McfGraph finish() throws AssertionError {
    assert !finished;
    finished = true;
    if (curEntity.length() == 0) {
      return null;
    }
    assert curEntityLineIdx != 0 : "Found a 'Node' (" + curEntity + ") with properties at the end of the file!";
    return graph.build();
  }

  private void parseNodeName(String node) {
    if (Vocabulary.isGlobalReference(node)) {
      String prop = Vocabulary.DCID;
      McfGraph.PropertyValues.Builder pvs = graph.getNodesOrDefault(node,
              McfGraph.PropertyValues.getDefaultInstance()).toBuilder();
      McfGraph.Values.Builder vals = pvs.getPvsOrDefault(prop,
              McfGraph.Values.getDefaultInstance()).toBuilder();
      McfGraph.TypedValue.Builder tval = vals.addTypedValuesBuilder();
      tval.setValue(node.substring(node.indexOf(Vocabulary.REFERENCE_DELIMITER) + 1));
      tval.setType(Mcf.ValueType.TEXT);
      pvs.putPvs(prop, vals.build());
      graph.putNodes(node, pvs.build());
    }
  }

  // TODO(shanth): Make Map updates less inefficient.
  private void parseValues(String prop, String values) {
    if (prop.isEmpty() || values.isEmpty()) return;

    McfGraph.PropertyValues.Builder pvs = graph.getNodesOrDefault(curEntity,
            McfGraph.PropertyValues.getDefaultInstance()).toBuilder();
    McfGraph.Values.Builder vals = pvs.getPvsOrDefault(prop,
            McfGraph.Values.getDefaultInstance()).toBuilder();
    if (prop.equals(Vocabulary.DCID) && Vocabulary.isGlobalReference(curEntity)) {
      vals.clearTypedValues();
    }

    SplitAndStripArg ssArg = new SplitAndStripArg();
    ssArg.delimiter = Vocabulary.VALUE_SEPARATOR;
    ssArg.includeEmpty = false;
    ssArg.stripEnclosingQuotes = false;
    List<String> fields = splitAndStripWithQuoteEscape(values, ssArg);
    for (String field : fields) {
      parseTypedValue(curEntity, prop, field, vals.addTypedValuesBuilder());
    }
    pvs.putPvs(prop, vals.build());
    graph.putNodes(curEntity, pvs.build());
  }

  private void parseTypedValue(String node, String prop, String val, McfGraph.TypedValue.Builder tval) {
    assert graph.getType() != Mcf.McfType.TEMPLATE_MCF;

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
      assert val.endsWith("]") : "Found malformed Complex value (" + val +
              ") without a closing bracket in property " + prop + " of node " + node;
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
        tval.setType(Mcf.ValueType.RESOLVED_REF);
        return;
      } else if (Vocabulary.isInternalReference(val)) {
        assert !isResolved : "Found an internal reference " + val + " in resolved entity " + node;
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
      tval.setType(Mcf.ValueType.RESOLVED_REF);
      return;
    }

    if (isNumber(val) || isBool(val)) {
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

  // Splits a string using the delimiter character. A field is not split if the delimiter is within a pair of double
  // quotes. If "includeEmpty" is true, then empty field is included.
  //
  // For example "1,2,3" will be split into ["1","2","3"] but "'1,234',5" will be
  // split into ["1,234", "5"].
  // TODO: Support strip escapes before quotes for TMCF/CSV flow.
  public static final class SplitAndStripArg {
    public char delimiter = ',';
    public boolean includeEmpty = false;
    public boolean stripEnclosingQuotes = true;
  }
  // NOTE: We do not strip enclosing quotes in this function.
  public static List<String> splitAndStripWithQuoteEscape(String orig, SplitAndStripArg arg) throws AssertionError {
    List<String> splits = new ArrayList<>();
    try {
      // withIgnoreSurroundingSpaces() is important to treat something like:
      //    `first, "second, with comma"`
      // as two fields: 1. `first`, 2. `second, with comma`
      CSVParser parser = new CSVParser(new StringReader(orig),
              CSVFormat.DEFAULT.withDelimiter(arg.delimiter).withIgnoreSurroundingSpaces());
      List<CSVRecord> records = parser.getRecords();
      assert records.size() == 1 : orig;
      for (String s : records.get(0)) {
        String ss = arg.stripEnclosingQuotes ? stripEnclosingQuotePair(s.trim()) : s.trim();
        // After stripping whitespace some terms could become empty.
        if (arg.includeEmpty || !ss.isEmpty()) {
          splits.add(ss);
        }
      }
    } catch (IOException e) {
      throw new AssertionError("Failed to parse prop value: " + orig + " :: " + e.toString());
    }
    return splits;
  }

  private static String stripEnclosingQuotePair(String val) {
    if (val.length() > 1) {
      if (val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
        return val.length() == 2 ? "" : val.substring(1, val.length() - 2);
      }
    }
    return val;
  }

  private boolean isNumber(String val) {
    try {
      long l = Long.parseLong(val);
      return true;
    } catch (NumberFormatException e) {}
    try {
      long l = Long.parseUnsignedLong(val);
      return true;
    } catch (NumberFormatException e) {}
    try {
      double d = Double.parseDouble(val);
      return true;
    } catch (NumberFormatException e) {}
    return false;
  }

  private boolean isBool(String val) {
    String v = val.toLowerCase();
    return v.equals("true") || v.equals("1") || v.equals("false") || v.equals("0");
  }
}
