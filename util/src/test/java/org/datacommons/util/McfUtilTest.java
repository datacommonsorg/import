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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import org.datacommons.proto.Mcf;
import org.junit.Test;

public class McfUtilTest {
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
    Mcf.McfGraph graph = McfParser.parseInstanceMcfString(SERIALIZE_INPUT, false, null);
    assertEquals(McfUtil.serializeMcfGraph(graph, true), output);
  }
}
