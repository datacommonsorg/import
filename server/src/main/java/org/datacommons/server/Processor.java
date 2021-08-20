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

package org.datacommons.server;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;

import org.datacommons.proto.Mcf;
import org.datacommons.util.*;

public class Processor {

  private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());

  private LogWrapper logCtx;

  public Processor(LogWrapper logCtx) {
    this.logCtx = logCtx;
  }

  public void processTables(
      File tmcfFile, List<File> csvFiles, char delimiter, ObservationRepository obsRepo)
      throws IOException {
    LOGGER.info("=======  process tables");
    for (File csvFile : csvFiles) {
      LOGGER.info("0");
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      long numNodesProcessed = 0;
        LOGGER.info("1");
      while ((g = parser.parseNextRow()) != null) {
        LOGGER.info("2");
        g = McfMutator.mutate(g.toBuilder(), logCtx);

        // This will set counters/messages in logCtx.
        boolean success = McfChecker.check(g, logCtx);
        if (success) {
          logCtx.incrementCounterBy("NumRowSuccesses", 1);
        }

        LOGGER.info(g.toString());
        numNodesProcessed++;
        LOGGER.info("3");
        for (String nodeId : g.getNodesMap().keySet()) {
          LOGGER.info(nodeId);
          Mcf.McfGraph.PropertyValues node = g.toBuilder().getNodesOrThrow(nodeId);
          Observation o = new Observation();
          for (String typeOf : McfUtil.getPropVals(node, Vocabulary.TYPE_OF)) {
            LOGGER.info(typeOf);
            if (Vocabulary.isStatVarObs(typeOf)) {
              List<Mcf.McfGraph.TypedValue> tvs = McfUtil.getPropTvs(node, Vocabulary.OBSERVATION_ABOUT);
              o.setObservationAbout(tvs.get(0).getValue());
              tvs = McfUtil.getPropTvs(node, Vocabulary.OBSERVATION_DATE);
              o.setObservationDate(tvs.get(0).getValue());
              tvs = McfUtil.getPropTvs(node, Vocabulary.VALUE);
              o.setValue(tvs.get(0).getValue());
              obsRepo.save(o);
              LOGGER.info("add o");
              break;
            }
          }
        }
        if (numNodesProcessed == 1000) {
          LOGGER.info(g.toString());
          break;
        }
      }
      LOGGER.info("======= exit while loop");
    }
  }
}