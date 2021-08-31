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
import static org.datacommons.util.McfUtil.mergeGraphs;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.IOUtils;
import org.datacommons.proto.Mcf;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class McfUtilTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  private static String SERIALIZE_INPUT =
      "Node: USState[1]\n"
          + "dcid: dcid:dc/abcd\n"
          + "typeOf: schema:State\n"
          + "name: \"California\"\n"
          + "\n"
          + "Node: USStateFemales[1]\n"
          + "dcid: dcid:dc/efgh\n"
          + "typeOf: schema:Population\n"
          + "populationType: schema:Person\n"
          + "location: l:USState[1]\n"
          + "age: [Years 20 -]\n"
          + "gender: \"Female\"\n"
          + "\n";

  @Test
  public void serializeMcfGraph() throws IOException {
    String output =
        "Node: USStateFemales[1]\n"
            + "age: [Years 20 -]\n"
            + "dcid: dcid:dc/efgh\n"
            + "gender: \"Female\"\n"
            + "location: l:USState[1]\n"
            + "populationType: dcid:Person\n"
            + "typeOf: dcid:Population\n"
            + "\n"
            + "Node: USState[1]\n"
            + "dcid: dcid:dc/abcd\n"
            + "name: \"California\"\n"
            + "typeOf: dcid:State\n"
            + "\n";
    Mcf.McfGraph graph =
        McfParser.parseInstanceMcfString(SERIALIZE_INPUT, false, TestUtil.newLogCtx("InMemory"));
    assertEquals(McfUtil.serializeMcfGraph(graph, true), output);
  }

  @Test
  public void funcMergeGraphs() throws IOException, AssertionError {
    List<Mcf.McfGraph> graphs =
        Arrays.asList(
            McfParser.parseInstanceMcfString(
                "Node: MadCity\n"
                    + "typeOf: dcs:City\n"
                    + "dcid: dcid:dc/maa\n"
                    + "overlapsWith: dcid:dc/456, dcid:dc/134\n"
                    + "name: \"Madras\"\n",
                true,
                TestUtil.newLogCtx("f1.mcf")),
            McfParser.parseInstanceMcfString(
                "Node: MadCity\n"
                    + "typeOf: dcs:Corporation\n"
                    + "dcid: dcid:dc/maa\n"
                    + "overlapsWith: dcid:dc/134\n"
                    + "containedInPlace: dcid:dc/tn\n"
                    + "name: \"Chennai\"\n",
                true,
                TestUtil.newLogCtx("f2.mcf")),
            McfParser.parseInstanceMcfString(
                "Node: MadState\n"
                    + "typeOf: dcs:State\n"
                    + "dcid: dcid:dc/tn\n"
                    + "containedInPlace: dcid:country/india\n",
                true,
                TestUtil.newLogCtx("f3.mcf")),
            McfParser.parseInstanceMcfString(
                "Node: MadState\n"
                    + "typeOf: dcs:State\n"
                    + "dcid: dcid:dc/tn\n"
                    + "capital: dcid:dc/maa\n"
                    + "containedInPlace: dcid:dc/southindia\n",
                true,
                TestUtil.newLogCtx("f4.mcf")));
    // Output should be the second node which is the largest, with other PVs
    // patched in, and all types included.
    Mcf.McfGraph want =
        McfParser.parseInstanceMcfString(
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
            TestUtil.newLogCtx("f5.mcf"));
    Mcf.McfGraph act = mergeGraphs(graphs);
    assertThat(TestUtil.trimLocations(act))
        .ignoringRepeatedFieldOrder()
        .isEqualTo(TestUtil.trimLocations(want));

    // Match locations.
    Mcf.McfGraph expLoc =
        TestUtil.graphFromProto(
            IOUtils.toString(
                this.getClass().getResourceAsStream("McfUtilTest_MergedLocations.textproto"),
                StandardCharsets.UTF_8));
    assertThat(TestUtil.getLocations(act)).ignoringRepeatedFieldOrder().isEqualTo(expLoc);
  }
}
