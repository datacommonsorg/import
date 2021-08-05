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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf.McfGraph;
import org.junit.Before;
import org.junit.Test;

// TODO: Add test once sanity-check is implemented.
public class TmcfCsvParserTest {
  private static final Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

  @Before
  public void setUp() {
    TmcfCsvParser.TEST_mode = true;
  }

  @Test
  public void statVarObs() throws IOException, URISyntaxException {
    String want = mcf("TmcfCsvParser_SVO.mcf");
    String got = run("TmcfCsvParser_SVO.tmcf", "TmcfCsvParser_SVO.csv");
    assertEquals(want, got);
  }

  @Test
  public void popObs() throws IOException, URISyntaxException {
    String want = mcf("TmcfCsvParser_PopObs.mcf");
    String got = run("TmcfCsvParser_PopObs.tmcf", "TmcfCsvParser_PopObs.csv");
    assertEquals(want, got);
  }

  @Test
  public void multiValue() throws IOException, URISyntaxException {
    String want = mcf("TmcfCsvParser_MultiValue.mcf");
    String got = run("TmcfCsvParser_MultiValue.tmcf", "TmcfCsvParser_MultiValue.csv");
    assertEquals(want, got);
  }

  private String mcf(String file_name) throws URISyntaxException, IOException {
    return McfUtil.serializeMcfGraph(
        McfParser.parseInstanceMcfFile(resourceToFile(file_name), false, null), true);
  }

  private String run(String mcf_file, String csv_file) throws IOException, URISyntaxException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    TmcfCsvParser parser =
        TmcfCsvParser.init(resourceToFile(mcf_file), resourceToFile(csv_file), ',', logCtx);
    List<McfGraph> result = new ArrayList<>();
    McfGraph graph;
    while ((graph = parser.parseNextRow()) != null) {
      result.add(graph);
    }
    System.out.println("CounterSet: " + logCtx.toString());
    return McfUtil.serializeMcfGraph(McfParser.mergeGraphs(result), true);
  }

  private String resourceToFile(String resource) throws URISyntaxException {
    String mcf_file = this.getClass().getResource(resource).toURI().toString();
    return mcf_file.substring(/* skip 'file:' */ 5);
  }
}
