package org.datacommons.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Mcf;
import org.datacommons.util.*;

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
    logCtx.updateLocationFile(file.getName());
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      McfMutator mutator = new McfMutator(n, logCtx);
      mutator.apply();

      McfChecker checker = new McfChecker(n, logCtx);
      checker.check();

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
      logCtx.updateLocationFile(csvFile.getName());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      long numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        McfMutator mutator = new McfMutator(g, logCtx);
        mutator.apply();

        McfChecker checker = new McfChecker(g, logCtx);
        checker.check();

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
