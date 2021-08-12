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
import java.nio.file.Paths;
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
    String want = TestUtil.mcf(resourceFile("TmcfCsvParser_SVO.mcf"));
    String got = run("TmcfCsvParser_SVO.tmcf", "TmcfCsvParser_SVO.csv");
    assertEquals(want, got);
  }

  @Test
  public void popObs() throws IOException, URISyntaxException {
    String want = TestUtil.mcf(resourceFile("TmcfCsvParser_PopObs.mcf"));
    String got = run("TmcfCsvParser_PopObs.tmcf", "TmcfCsvParser_PopObs.csv");
    assertEquals(want, got);
  }

  @Test
  public void multiValue() throws IOException, URISyntaxException {
    String want = TestUtil.mcf(resourceFile("TmcfCsvParser_MultiValue.mcf"));
    String got = run("TmcfCsvParser_MultiValue.tmcf", "TmcfCsvParser_MultiValue.csv");
    assertEquals(want, got);
  }

  private String run(String mcfFile, String csvFile) throws IOException, URISyntaxException {
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), Paths.get("."));
    logCtx.setLocationFile(csvFile);
    TmcfCsvParser parser =
        TmcfCsvParser.init(resourceFile(mcfFile), resourceFile(csvFile), ',', logCtx);
    List<McfGraph> result = new ArrayList<>();
    McfGraph graph;
    while ((graph = parser.parseNextRow()) != null) {
      result.add(graph);
    }
    graph = McfUtil.mergeGraphs(result);
    return McfUtil.serializeMcfGraph(graph, true);
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
