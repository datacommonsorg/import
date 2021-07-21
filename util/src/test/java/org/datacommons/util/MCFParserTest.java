package org.datacommons.util;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import static org.datacommons.util.McfParser.SplitAndStripArg;
import static org.datacommons.util.McfParser.splitAndStripWithQuoteEscape;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

import org.junit.Test;

public class McfParserTest {
  @Test
  public void funcSplitAndStripWithQuoteEscape() {
    SplitAndStripArg arg = new SplitAndStripArg();
    assertThat(splitAndStripWithQuoteEscape("one,two,three", arg)).
            containsExactly("one", "two", "three");

    // Single quote (unterminated or otherwise) should be preserved.
    assertThat(splitAndStripWithQuoteEscape("\"O'Brien\", 20", arg)).
            containsExactly("O'Brien", "20");

    // Whitespace surrounding ',' is excluded, but within is included.
    assertThat(splitAndStripWithQuoteEscape(" o ne, two ,th ree", arg)).
            containsExactly("o ne", "two", "th ree");

    // One pair of double quotes are removed.
    assertThat(splitAndStripWithQuoteEscape(" '\"one\"',two,\"three\"", arg)).
            containsExactly("'\"one\"'", "two", "three");

    // Comma within double quotes are not split.
    assertThat(splitAndStripWithQuoteEscape("'one, two', three, \"four, five\"", arg)).
            containsExactly("'one", "two'", "three", "four, five");

    // Empty strings are by default removed.
    assertThat(splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg)).
            containsExactly("one", "two", "three");

    // Empty strings are kept when specifically requested.
    arg = new SplitAndStripArg();
    arg.includeEmpty = true;
    assertThat(splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg)).
            containsExactly("one", "", "two", "", "three");
  }

  @Test
  public void funcParseUnresolvedGraphLine() throws IOException {
    McfGraph act = actual("McfParserTest_UnresolvedGraph.mcf", false);
    McfGraph exp = expected("McfParserTest_UnresolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphLine() throws IOException {
    McfGraph act = actual("McfParserTest_ResolvedGraph.mcf", true);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsString() throws IOException {
    String mcf_string = IOUtils.toString(this.getClass().getResourceAsStream("McfParserTest_ResolvedGraph.mcf"), StandardCharsets.UTF_8);
    McfGraph act = McfParser.parseInstanceMcfString(mcf_string, true);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsFile() throws IOException, URISyntaxException {
    String mcf_file = this.getClass().getResource("McfParserTest_ResolvedGraph.mcf").toURI().toString();
    McfGraph act = McfParser.parseInstanceMcfFile(mcf_file.substring(/* skip 'file:' */ 5), true);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsSingleNodeGraphs() throws IOException {
    String[] lines = IOUtils.toString(this.getClass().getResourceAsStream("McfParserTest_ResolvedGraph.mcf"), StandardCharsets.UTF_8).split("\n");
    McfParser parser = McfParser.init(Mcf.McfType.INSTANCE_MCF, true);
    McfGraph.Builder act = McfGraph.newBuilder();
    act.setType(Mcf.McfType.INSTANCE_MCF);
    for (String l : lines) {
      parser.parseLine(l);
      if (parser.isNodeBoundary()) {
        McfGraph node = parser.extractPreviousNode();
        if (node != null) {
          Map.Entry<String, McfGraph.PropertyValues> entry = node.getNodesMap().entrySet().iterator().next();
          act.putNodes(entry.getKey(), entry.getValue());
        }
      }
    }
    McfGraph node = parser.finish();
    if (node != null) {
      Map.Entry<String, McfGraph.PropertyValues> entry = node.getNodesMap().entrySet().iterator().next();
      act.putNodes(entry.getKey(), entry.getValue());
    }
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act.build()).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  private McfGraph actual(String mcf_file, boolean isResolved) throws IOException {
    String[] lines = IOUtils.toString(this.getClass().getResourceAsStream(mcf_file), StandardCharsets.UTF_8).split("\n");
    McfParser parser = McfParser.init(Mcf.McfType.INSTANCE_MCF, isResolved);
    for (String l : lines) {
      parser.parseLine(l);
    }
    return parser.finish();
  }

  private McfGraph expected(String proto_file) throws IOException {
    McfGraph.Builder expected = McfGraph.newBuilder();
    String proto = IOUtils.toString(this.getClass().getResourceAsStream(proto_file), StandardCharsets.UTF_8);
    TextFormat.getParser().merge(new StringReader(proto), expected);
    return expected.build();
  }
}
