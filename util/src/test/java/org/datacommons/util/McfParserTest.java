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

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;
import static org.datacommons.util.McfParser.*;

import com.google.protobuf.TextFormat;
import java.io.IOException;
import java.io.StringReader;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfType;
import org.junit.Test;

public class McfParserTest {
  @Test
  public void funcSplitAndStripWithQuoteEscape() {
    SplitAndStripArg arg = new SplitAndStripArg();
    assertThat(splitAndStripWithQuoteEscape("one,two,three", arg, null))
        .containsExactly("one", "two", "three");

    // Single quote (unterminated or otherwise) should be preserved.
    assertThat(splitAndStripWithQuoteEscape("\"O'Brien\", 20", arg, null))
        .containsExactly("O'Brien", "20");

    // Whitespace surrounding ',' is excluded, but within is included.
    assertThat(splitAndStripWithQuoteEscape(" o ne, two ,th ree", arg, null))
        .containsExactly("o ne", "two", "th ree");

    // One pair of double quotes are removed.
    assertThat(splitAndStripWithQuoteEscape(" '\"one\"',two,\"three\"", arg, null))
        .containsExactly("'\"one\"'", "two", "three");

    // Comma within double quotes are not split.
    assertThat(splitAndStripWithQuoteEscape("'one, two', three, \"four, five\"", arg, null))
        .containsExactly("'one", "two'", "three", "four, five");

    // Empty strings are by default removed.
    assertThat(splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg, null))
        .containsExactly("one", "two", "three");

    // Empty strings are kept when specifically requested.
    arg = new SplitAndStripArg();
    arg.includeEmpty = true;
    assertThat(splitAndStripWithQuoteEscape("one,   ,two, \"\" , three", arg, null))
        .containsExactly("one", "", "two", "", "three");

    // Strings that are escaped normally show up without character.
    // TODO: make this behavior match prod.
    arg = new SplitAndStripArg();
    arg.includeEmpty = false;
    assertThat(splitAndStripWithQuoteEscape("\"{ \\\"type\\\": \\\"feature\\\" }\"", arg, null))
        .containsExactly("{ \"type\": \"feature\" }");
  }

  @Test
  public void funcParseUnresolvedGraph() throws IOException, URISyntaxException {
    McfGraph act = actual("McfParserTest_UnresolvedGraph.mcf", false);
    McfGraph exp = expected("McfParserTest_UnresolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraph() throws IOException, URISyntaxException {
    McfGraph act = actual("McfParserTest_ResolvedGraph.mcf", true);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsString() throws IOException {
    String mcfString =
        IOUtils.toString(
            this.getClass().getResourceAsStream("McfParserTest_ResolvedGraph.mcf"),
            StandardCharsets.UTF_8);
    McfGraph act = McfParser.parseInstanceMcfString(mcfString, true, null);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsFile() throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource("McfParserTest_ResolvedGraph.mcf").getPath();
    McfGraph act = McfParser.parseInstanceMcfFile(mcfFile, true, null);
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseTemplateMcfAsString() throws IOException {
    String mcfString =
        IOUtils.toString(
            this.getClass().getResourceAsStream("McfParserTest_Template.tmcf"),
            StandardCharsets.UTF_8);
    McfGraph act = McfParser.parseTemplateMcfString(mcfString, null);
    McfGraph exp = expected("McfParserTest_Template.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseTemplateMcfAsFile() throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource("McfParserTest_Template.tmcf").getPath();
    McfGraph act = McfParser.parseTemplateMcfFile(mcfFile, null);
    McfGraph exp = expected("McfParserTest_Template.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcMergeGraphs() throws IOException {
    List<McfGraph> graphs =
        Arrays.asList(
            parseInstanceMcfString(
                "Node: MadCity\n"
                    + "typeOf: dcs:City\n"
                    + "dcid: dcid:dc/maa\n"
                    + "overlapsWith: dcid:dc/456, dcid:dc/134\n"
                    + "name: \"Madras\"\n",
                true,
                null),
            parseInstanceMcfString(
                "Node: MadCity\n"
                    + "typeOf: dcs:Corporation\n"
                    + "dcid: dcid:dc/maa\n"
                    + "overlapsWith: dcid:dc/134\n"
                    + "containedInPlace: dcid:dc/tn\n"
                    + "name: \"Chennai\"\n",
                true,
                null),
            parseInstanceMcfString(
                "Node: MadState\n"
                    + "typeOf: dcs:State\n"
                    + "dcid: dcid:dc/tn\n"
                    + "containedInPlace: dcid:country/india\n",
                true,
                null),
            parseInstanceMcfString(
                "Node: MadState\n"
                    + "typeOf: dcs:State\n"
                    + "dcid: dcid:dc/tn\n"
                    + "capital: dcid:dc/maa\n"
                    + "containedInPlace: dcid:dc/southindia\n",
                true,
                null));
    // Output should be the second node which is the largest, with other PVs
    // patched in, and all types included.
    McfGraph want =
        parseInstanceMcfString(
            "Node: MadCity\n"
                + "typeOf: dcs:City, dcs:Corporation\n"
                + "dcid: dcid:dc/maa\n"
                + "containedInPlace: dcid:dc/tn\n"
                + "overlapsWith: dcid:dc/134, dcid:dc/456\n"
                + "name: \"Chennai\", \"Madras\"\n"
                + "\n"
                + "Node: MadState\n"
                + "typeOf: dcs:State\n"
                + "dcid: dcid:dc/tn\n"
                + "capital: dcid:dc/maa\n"
                + "containedInPlace: dcid:country/india, dcid:dc/southindia\n",
            true,
            null);
    assertThat(mergeGraphs(graphs)).ignoringRepeatedFieldOrder().isEqualTo(want);
  }

  private McfGraph actual(String file_name, boolean isResolved)
      throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource(file_name).getPath();
    McfParser parser = McfParser.init(McfType.INSTANCE_MCF, mcfFile, isResolved, null);
    McfGraph.Builder act = McfGraph.newBuilder();
    act.setType(McfType.INSTANCE_MCF);
    McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      Map.Entry<String, McfGraph.PropertyValues> entry =
          n.getNodesMap().entrySet().iterator().next();
      act.putNodes(entry.getKey(), entry.getValue());
    }
    return act.build();
  }

  private McfGraph expected(String protoFile) throws IOException {
    McfGraph.Builder expected = McfGraph.newBuilder();
    String proto =
        IOUtils.toString(this.getClass().getResourceAsStream(protoFile), StandardCharsets.UTF_8);
    TextFormat.getParser().merge(new StringReader(proto), expected);
    return expected.build();
  }
}
