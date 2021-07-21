package org.datacommons.util;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.MCF;
import org.datacommons.proto.MCF.MCFGraph;
import static org.datacommons.util.MCFParser.SplitAndStripWithQuoteEscape;
import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;

import org.junit.Test;

public class MCFParserTest {
  @Test
  public void funcSplitAndStripWithQuoteEscape() {
    assertThat(SplitAndStripWithQuoteEscape("one,two,three",
            false, true)).
            containsExactly("one", "two", "three");

    // Single quote (unterminated or otherwise) should be preserved.
    assertThat(SplitAndStripWithQuoteEscape("\"O'Brien\", 20",
            false, true)).
            containsExactly("O'Brien", "20");

    // Whitespace surrounding ',' is excluded, but within is included.
    assertThat(SplitAndStripWithQuoteEscape(" o ne, two ,th ree",
            false, true)).
            containsExactly("o ne", "two", "th ree");

    // One pair of double quotes are removed.
    assertThat(SplitAndStripWithQuoteEscape(" '\"one\"',two,\"three\"",
            false, true)).
            containsExactly("'\"one\"'", "two", "three");

    // Comma within double quotes are not split.
    assertThat(SplitAndStripWithQuoteEscape("'one, two', three, \"four, five\"",
            false, true)).
            containsExactly("'one", "two'", "three", "four, five");

    // Empty strings are by default removed.
    assertThat(SplitAndStripWithQuoteEscape("one,   ,two, \"\" , three",
            false, true)).
            containsExactly("one", "two", "three");

    // Empty strings are kept when specifically requested.
    assertThat(SplitAndStripWithQuoteEscape("one,   ,two, \"\" , three",
            true, true)).
            containsExactly("one", "", "two", "", "three");
  }

  @Test
  public void funcParseUnresolvedGraph() throws IOException {
    MCFGraph act = actual("MCFParserTest_UnresolvedGraph.mcf");
    MCFGraph exp = expected("MCFParserTest_UnresolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraph() throws IOException {
    MCFGraph act = actual("MCFParserTest_ResolvedGraph.mcf");
    MCFGraph exp = expected("MCFParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  private MCFGraph actual(String mcf_file) throws IOException {
    String[] lines = IOUtils.toString(this.getClass().getResourceAsStream(mcf_file),"UTF-8").split("\n");
    MCFParser parser = MCFParser.Init(MCF.MCFType.INSTANCE_MCF, false);
    for (String l : lines) {
      parser.ParseLine(l);
    }
    MCFGraph mcf = parser.Finish();
    return mcf;
  }

  private MCFGraph expected(String proto_file) throws IOException {
    MCFGraph.Builder expected = MCFGraph.newBuilder();
    String proto = IOUtils.toString(this.getClass().getResourceAsStream(proto_file), "UTF-8");
    TextFormat.getParser().merge(new StringReader(proto), expected);
    return expected.build();
  }
}
