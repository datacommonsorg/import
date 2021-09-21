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
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.datacommons.proto.Mcf;
import org.datacommons.util.*;

// Class to process input files and store the transformed data into storage.
public class Processor {

  private static final Logger LOGGER = Logger.getLogger(Processor.class.getName());
  private LogWrapper logCtx;

  public Processor(LogWrapper logCtx) {
    this.logCtx = logCtx;
  }

  // Take a set of tmcf and csv files, parse the nodes and store observations
  // into the in-memory Observation table.
  public void processTables(
      File tmcfFile,
      List<File> csvFiles,
      char delimiter,
      ObservationRepository observationRepository)
      throws IOException, InterruptedException {
    for (File csvFile : csvFiles) {
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      long numNodesProcessed = 0;
      List<Observation> cachedObservations = new ArrayList<Observation>();
      while ((g = parser.parseNextRow()) != null) {
        g = McfMutator.mutate(g.toBuilder(), logCtx);
        // This will set counters/messages in logCtx.
        boolean success = McfChecker.check(g, null, null, logCtx);
        if (success) {
          logCtx.incrementInfoCounterBy("NumRowSuccesses", 1);
        }
        numNodesProcessed++;
        for (String nodeId : g.getNodesMap().keySet()) {
          Mcf.McfGraph.PropertyValues node = g.toBuilder().getNodesOrThrow(nodeId);
          Observation o = new Observation();
          for (String typeOf : McfUtil.getPropVals(node, Vocabulary.TYPE_OF)) {
            if (Vocabulary.isStatVarObs(typeOf)) {
              List<Mcf.McfGraph.TypedValue> tvs =
                  McfUtil.getPropTvs(node, Vocabulary.OBSERVATION_ABOUT);
              o.setObservationAbout(tvs.get(0).getValue());
              tvs = McfUtil.getPropTvs(node, Vocabulary.OBSERVATION_DATE);
              o.setObservationDate(tvs.get(0).getValue());
              tvs = McfUtil.getPropTvs(node, Vocabulary.VALUE);
              o.setValue(tvs.get(0).getValue());
              tvs = McfUtil.getPropTvs(node, Vocabulary.VARIABLE_MEASURED);
              o.setVariableMeasured(tvs.get(0).getValue());
              cachedObservations.add(o);
              break;
            }
          }
        }
        if (numNodesProcessed % 1000 == 0) {
          LOGGER.info(String.valueOf(numNodesProcessed));
          // Save observations in batch
          observationRepository.saveAll(cachedObservations);
          cachedObservations.clear();
        }
      }
      observationRepository.saveAll(cachedObservations);
      LOGGER.log(Level.INFO, "Added all entries for {0}", csvFile.getName());
    }
  }
}
