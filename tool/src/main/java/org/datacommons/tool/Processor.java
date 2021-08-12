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

package org.datacommons.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Mcf;
import org.datacommons.util.*;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;

class DCTooManyFailuresException extends Exception {
  public DCTooManyFailuresException() {}

  public DCTooManyFailuresException(String message) {
    super(message);
  }
}

public class Processor {
  private static final Logger logger = LogManager.getLogger(Processor.class);
  private LogWrapper logCtx;

  public Processor(LogWrapper logCtx) {
    this.logCtx = logCtx;
  }

  public void processNodes(Mcf.McfType type, File file)
      throws IOException, DCTooManyFailuresException {
    long numNodesProcessed = 0;
    logger.debug("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    logCtx.setLocationFile(file.getName());
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      n = McfMutator.mutate(n.toBuilder(), logCtx);

      // This will set counters/messages in logCtx.
      McfChecker.check(n, logCtx);

      numNodesProcessed++;
      logCtx.provideStatus(numNodesProcessed, "nodes");
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("processNodes encountered too many failures");
      }
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  public void processTables(
      File tmcfFile, List<File> csvFiles, char delimiter, BufferedWriter writer)
      throws IOException, DCTooManyFailuresException {
    logger.debug("TMCF " + tmcfFile.getName());
    for (File csvFile : csvFiles) {
      logger.debug("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      long numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        g = McfMutator.mutate(g.toBuilder(), logCtx);

        // This will set counters/messages in logCtx.
        McfChecker.check(g, logCtx);

        if (writer != null) {
          writer.write(McfUtil.serializeMcfGraph(g, false));
        }

        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
        logCtx.provideStatus(numRowsProcessed, "rows");
        if (logCtx.loggedTooManyFailures()) {
          throw new DCTooManyFailuresException("processTables encountered too many failures");
        }
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
  }
}
