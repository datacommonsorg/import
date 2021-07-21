package org.datacommons.util;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.datacommons.proto.MCF;
import org.datacommons.proto.MCF.MCFGraph;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class MCFParser {
  private MCFGraph.Builder graph;
  private boolean resolved;
  private String curEntity;
  private int curEntityLineIdx = 0;
  private String prevEntity;
  private boolean finished = false;

  public static MCFParser Init(MCF.MCFType type, boolean isResolved) {
    MCFParser parser = new MCFParser();
    parser.graph = MCFGraph.newBuilder();
    parser.graph.setType(type);
    parser.resolved = isResolved;
    parser.curEntity = "";
    parser.prevEntity = "";
    return parser;
  }

  // TODO(shanth): Convert to UTF-8.
  public void ParseLine(String line) throws AssertionError {
    assert !finished;

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
    int colon = line.substring(prefixLen).indexOf(':');
    assert colon > 0 : "Colon parsing problem in " + line;

    String lhs = line.substring(0, colon).trim();
    String rhs = line.substring(colon + 1).trim();
    if (lhs.equals("Node")) {
      assert rhs.indexOf(',') == -1 : "RHS of Node must be a unary value: " + rhs;
      assert !rhs.startsWith("\"") : "RHS of Node must not be a quoted string: " + rhs;
      // New entity scope begins.
      SetMCFNodeName(rhs);

      prevEntity = curEntity;
      curEntity = rhs;
      curEntityLineIdx = 0;
    } else {
      assert !curEntity.isEmpty() : " Property found without a 'Node' term: " + line;
      SetMCFPropVals(lhs, rhs);

      curEntityLineIdx++;
    }
  }

  public boolean isNodeBoundary() { return curEntityLineIdx == 0; }

  public MCFGraph extractPreviousNode() throws IllegalArgumentException {
    if (prevEntity.length() == 0) {
      return null;
    }
    MCFGraph.Builder graph = MCFGraph.newBuilder();
    graph.setType(graph.getType());
    graph.putNodes(prevEntity, graph.getNodesOrThrow(prevEntity));
    graph.removeNodes(prevEntity);
    return graph.build();
  }

  public MCFGraph Finish() {
    assert !finished;
    finished = true;
    return graph.build();
  }

  // NOTE: We do not strip enclosing quotes in this function.
  public static List<String> SplitAndStripWithQuoteEscape(String orig, boolean includeEmpty,
                                                          boolean stripEnclosingQuotes) throws AssertionError {
    List<String> splits = new ArrayList<>();
    try {
      // withIgnoreSurroundingSpaces() is important to treat something like:
      //    `first, "second, with comma"`
      // as two fields: 1. `first`, 2. `second, with comma`
      CSVParser parser = new CSVParser(new StringReader(orig),
              CSVFormat.DEFAULT.withIgnoreSurroundingSpaces());
      List<CSVRecord> records = parser.getRecords();
      assert records.size() == 1 : orig;
      for (String s : records.get(0)) {
        String ss = stripEnclosingQuotes ? StripEnclosingQuotePair(s.trim()) : s.trim();
        if (includeEmpty || !ss.isEmpty()) {
          splits.add(ss);
        }
      }
    } catch (IOException e) {
      throw new AssertionError("Failed to parse prop value: " + orig + " :: " + e.toString());
    }
    return splits;
  }

  // TODO(shanth): Make Map updates less inefficient.
  private void SetMCFPropVals(String prop, String values) {
    if (prop.isEmpty() || values.isEmpty()) return;

    MCFGraph.PropertyValues.Builder pvs = graph.getNodesOrDefault(curEntity,
            MCFGraph.PropertyValues.getDefaultInstance()).toBuilder();
    MCFGraph.Values.Builder vals = pvs.getPvsOrDefault(prop,
            MCFGraph.Values.getDefaultInstance()).toBuilder();
    List<String> fields = SplitAndStripWithQuoteEscape(values,
            false, false);
    for (String field : fields) {
      ParseTypedValue(curEntity, prop, field, vals.addTypedValueBuilder());
    }
    pvs.putPvs(prop, vals.build());
    graph.putNodes(curEntity, pvs.build());
  }

  private void SetMCFNodeName(String node) {
    if (IsGlobalRef(node)) {
      String prop = "dcid";
      MCFGraph.PropertyValues.Builder pvs = graph.getNodesOrDefault(node,
              MCFGraph.PropertyValues.getDefaultInstance()).toBuilder();
      MCFGraph.Values.Builder vals = pvs.getPvsOrDefault(prop,
              MCFGraph.Values.getDefaultInstance()).toBuilder();
      MCFGraph.TypedValue.Builder tval = vals.addTypedValueBuilder();
      tval.setValue(node.substring(node.indexOf(':') + 1));
      tval.setType(MCF.ValueType.TEXT);
      pvs.putPvs(prop, vals.build());
      graph.putNodes(node, pvs.build());
    }
  }

  private void ParseTypedValue(String node, String prop, String val, MCFGraph.TypedValue.Builder tval) {
    assert graph.getType() != MCF.MCFType.TEMPLATE_MCF;

    // TODO(shanth): Implement MCFVocab static-member struct.
    boolean expectRef = (prop.equals("location") || prop.equals("observedNode") ||
            prop.equals("containedInPlace") || prop.equals("containedIn") ||
            prop.equals("typeOf") || prop.equals("popType") ||
            prop.equals("subClassOf") || prop.equals("rangeIncludes") ||
            prop.equals("domainIncludes") || prop.equals("measuredProp") ||
            prop.equals("popGroup") || prop.equals("constraintProps") ||
            prop.equals("measurementMethod") || prop.equals("comparedNode"));

    if (val.startsWith("\"")) {
      // If a value starts with double quotes, consider it a string.
      //
      // Strip enclosing double quotes.
      val = StripEnclosingQuotePair(val);
      if (!expectRef) {
        tval.setValue(val);
        tval.setType(MCF.ValueType.TEXT);
        return;
      }
      // If we're here, user probably incorrectly quoted a reference, let's strip
      // quotes and proceed parsing.
    }

    if (val.startsWith("[")) {
      assert val.endsWith("]") : "Invalid Quantity value " + val + " for " + node;
      tval.setValue(val);
      tval.setType(MCF.ValueType.ENTITY_DEF);
      return;
    }

    int colon = val.indexOf(':');
    if (colon != -1) {
      // This maybe a reference, or something with an unfortunate ":" in it which
      // the user failed to encapsulate in quotes (e.g., url: http://goo.gl).
      if (IsGlobalRef(val)) {
        // Strip the prefix and set the value.
        tval.setValue(val.substring(colon + 1));
        tval.setType(MCF.ValueType.RESOLVED_REF);
        return;
      } else if (IsInternalRef(val)) {
        assert !resolved : "Found an internal reference " + val + " in resolved entity " + node;
        tval.setValue(val);
        tval.setType(MCF.ValueType.UNRESOLVED_REF);
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
      tval.setType(MCF.ValueType.RESOLVED_REF);
      return;
    }

    if (IsNumber(val) || IsBool(val)) {
      // This parses to a number or bool.
      tval.setValue(val);
      tval.setType(MCF.ValueType.NUMBER);
      return;
    }

    // Instead of failing an unquoted value, treat it as a string.
    //
    // NOTE: This could be an uncommon reference property that user forgot to add
    // a "dcid:" prefix for. We err on the side of accepting the value as a string
    // instead of failing.
    tval.setValue(val);
    tval.setType(MCF.ValueType.TEXT);
  }

  private static String StripEnclosingQuotePair(String val) {
    if (val.length() > 1) {
      if (val.charAt(0) == '"' && val.charAt(val.length() - 1) == '"') {
        return val.length() == 2 ? "" : val.substring(1, val.length() - 2);
      }
    }
    return val;
  }

  private static boolean IsGlobalRef(String val) {
    return(val.startsWith("dcid:")||val.startsWith("schema:")||val.startsWith("dcs:"));
  }

  private static boolean IsInternalRef(String val) {
    return val.startsWith("l:");
  }

  private boolean IsNumber(String val) {
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

  private boolean IsBool(String val) {
    String v = val.toLowerCase();
    return v.equals("true") || v.equals("1") || v.equals("false") || v.equals("0");
  }
}
