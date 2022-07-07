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

import static com.google.common.truth.extensions.proto.ProtoTruth.assertThat;
import static org.datacommons.util.McfParser.parseTypedValue;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Debug.Log.Level;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfType;
import org.datacommons.proto.Mcf.ValueType;
import org.junit.Test;

public class McfParserTest {
  @Test
  public void testQuoting() throws IOException, URISyntaxException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(logCtx, Path.of("InMemory"));

    // Parsing weird double quotes fails.
    String mcf =
        "Node: US1\n"
            + "typeOf: schema:State\n"
            + "description: \"Odd\"double\"quotes"
            + "\"fails\"\n";
    McfParser.parseInstanceMcfString(mcf, true, lw);
    assertTrue(TestUtil.checkLog(lw.getLog(), "StrSplit_BadQuotesInToken_description", "US1"));

    // New-line in value.
    mcf = "Node: US2\n" + "typeOf: schema:State\n" + "description: \"Line with\nnewline\"\n";
    McfParser.parseInstanceMcfString(mcf, true, lw);
    assertTrue(TestUtil.checkLog(logCtx.build(), "StrSplit_BadQuotesInToken_description", "US2"));
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
    McfParser.IN_MEMORY_FILE_NAME = "McfParserTest_ResolvedGraph.mcf";
    McfGraph act = McfParser.parseInstanceMcfString(mcfString, true, TestUtil.newLogCtx());
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseResolvedGraphAsFile() throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource("McfParserTest_ResolvedGraph.mcf").getPath();
    McfGraph act = McfParser.parseInstanceMcfFile(mcfFile, true, TestUtil.newLogCtx());
    McfGraph exp = expected("McfParserTest_ResolvedGraph.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseTemplateMcfAsString() throws IOException {
    String mcfString =
        IOUtils.toString(
            this.getClass().getResourceAsStream("McfParserTest_Template.tmcf"),
            StandardCharsets.UTF_8);
    McfParser.IN_MEMORY_FILE_NAME = "McfParserTest_Template.tmcf";
    McfGraph act = McfParser.parseTemplateMcfString(mcfString, TestUtil.newLogCtx());
    McfGraph exp = expected("McfParserTest_Template.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  @Test
  public void funcParseTemplateMcfAsFile() throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource("McfParserTest_Template.tmcf").getPath();
    McfGraph act = McfParser.parseTemplateMcfFile(mcfFile, TestUtil.newLogCtx());
    McfGraph exp = expected("McfParserTest_Template.textproto");
    assertThat(act).ignoringRepeatedFieldOrder().isEqualTo(exp);
  }

  private McfGraph actual(String file_name, boolean isResolved)
      throws IOException, URISyntaxException {
    String mcfFile = this.getClass().getResource(file_name).getPath();
    McfParser parser =
        McfParser.init(McfType.INSTANCE_MCF, mcfFile, isResolved, TestUtil.newLogCtx());
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
    return TestUtil.graphFromProto(
        IOUtils.toString(this.getClass().getResourceAsStream(protoFile), StandardCharsets.UTF_8));
  }

  @Test
  public void funcParseTypedValue() {
    McfGraph.TypedValue.Builder expected = McfGraph.TypedValue.newBuilder();
    String refProp = "unit";
    String regProp = "testProp";

    expected.setValue("E: Test->E1");
    expected.setType(ValueType.TABLE_ENTITY);
    McfGraph.TypedValue.Builder actual =
        parseTypedValue(McfType.TEMPLATE_MCF, false, regProp, "E: Test->E1", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("C: Test->testCol");
    expected.setType(ValueType.TABLE_COLUMN);
    actual = parseTypedValue(McfType.TEMPLATE_MCF, false, regProp, "C: Test->testCol", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("testQuoted");
    expected.setType(ValueType.TEXT);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "\"testQuoted\"", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("[1 2]");
    expected.setType(ValueType.COMPLEX_VALUE);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "[1 2]", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    actual =
        parseTypedValue(
            McfType.INSTANCE_MCF,
            false,
            regProp,
            "[",
            new LogCb(TestUtil.newLogCtx(), Level.LEVEL_ERROR, "InMemory", 1));
    assertNull(actual);

    expected.clear();
    expected.setValue("Person");
    expected.setType(ValueType.RESOLVED_REF);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "dcid:Person", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("l:Person");
    expected.setType(ValueType.UNRESOLVED_REF);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "l:Person", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("Person");
    expected.setType(ValueType.RESOLVED_REF);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, refProp, "Person", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("1");
    expected.setType(ValueType.NUMBER);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "1", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());

    expected.clear();
    expected.setValue("testVal");
    expected.setType(ValueType.TEXT);
    actual = parseTypedValue(McfType.INSTANCE_MCF, false, regProp, "testVal", null);
    assertNotNull(actual);
    assertEquals(expected.build(), actual.build());
  }
}
