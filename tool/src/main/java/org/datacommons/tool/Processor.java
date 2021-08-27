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
import java.net.http.HttpClient;
import java.util.ArrayList;
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
  private ExistenceChecker existenceChecker;
  private List<Mcf.McfGraph> nodesForExistenceCheck;
  private boolean verbose;

  // NOTE: If doExistenceChecks is true, then it is important that the caller perform a
  // checkAllNodes() call *after* all instance MCF files are processed (via processNodes). This is
  // so that the newly added schema, StatVar, etc. are fully known to the Existence Checker first,
  // before existence checks are performed.
  public Processor(boolean doExistenceChecks, boolean verbose, LogWrapper logCtx) {
    this.logCtx = logCtx;
    this.verbose = verbose;
    if (doExistenceChecks) {
      existenceChecker = new ExistenceChecker(HttpClient.newHttpClient(), verbose, logCtx);
      nodesForExistenceCheck = new ArrayList<>();
    }
  }

  public void processNodes(Mcf.McfType type, File file)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    long numNodesProcessed = 0;
    if (verbose) logger.info("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    logCtx.setLocationFile(file.getName());
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      n = McfMutator.mutate(n.toBuilder(), logCtx);

      if (existenceChecker != null && type == Mcf.McfType.INSTANCE_MCF) {
        // Add instance MCF nodes to ExistenceChecker.  We load all the nodes up first
        // before we check them later in checkAllNodes().
        existenceChecker.addLocalGraph(n);
        nodesForExistenceCheck.add(n);
      } else {
        McfChecker.check(n, existenceChecker, logCtx);
      }

      numNodesProcessed++;
      logCtx.provideStatus(numNodesProcessed, "nodes processed");
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("processNodes encountered too many failures");
      }
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  public void processTables(
      File tmcfFile, List<File> csvFiles, char delimiter, BufferedWriter writer)
      throws IOException, DCTooManyFailuresException, InterruptedException {
    if (verbose) logger.info("TMCF " + tmcfFile.getName());
    for (File csvFile : csvFiles) {
      if (verbose) logger.info("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      // If there were too many failures when initializing the parser, parser will be null and we
      // don't want to continue processing.
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("processTables encountered too many failures");
      }
      Mcf.McfGraph g;
      long numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        g = McfMutator.mutate(g.toBuilder(), logCtx);

        // This will set counters/messages in logCtx.
        boolean success = McfChecker.check(g, existenceChecker, logCtx);
        if (success) {
          logCtx.incrementCounterBy("NumRowSuccesses", 1);
        }

        if (writer != null) {
          writer.write(McfUtil.serializeMcfGraph(g, false));
        }

        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
        logCtx.provideStatus(numRowsProcessed, "rows processed");
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

  // Called only when existenceChecker is enabled.
  public void checkAllNodes() throws IOException, InterruptedException, DCTooManyFailuresException {
    long numNodesChecked = 0;
    logger.info("Performing existence checks");
    logCtx.setLocationFile("");
    for (Mcf.McfGraph n : nodesForExistenceCheck) {
      McfChecker.check(n, existenceChecker, logCtx);
      numNodesChecked += n.getNodesCount();
      numNodesChecked++;
      logCtx.provideStatus(numNodesChecked, "nodes checked");
      if (logCtx.loggedTooManyFailures()) {
        throw new DCTooManyFailuresException("checkNodes encountered too many failures");
      }
    }
  }
}
