package org.datacommons.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.LogWrapper;
import org.datacommons.util.McfParser;
import org.datacommons.util.McfUtil;
import org.datacommons.util.TmcfCsvParser;

class DCTooManyFailuresException extends Exception {
  public DCTooManyFailuresException() {}

  public DCTooManyFailuresException(String message) {
    super(message);
  }
}

public class Processor {
  private static final Logger logger = LogManager.getLogger(Processor.class);

  public static void processNodes(Mcf.McfType type, File file, Debug.Log.Builder logCtx)
      throws IOException, DCTooManyFailuresException {
    long numNodesProcessed = 0;
    logger.debug("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    boolean hasError = false;
    while ((n = parser.parseNextNode()) != null) {
      numNodesProcessed++;
      if (LogWrapper.loggedTooManyFailures(logCtx)) {
        throw new DCTooManyFailuresException("processNodes encountered too many failures");
      }
      maybePrintStatus(numNodesProcessed, "nodes", file.getName(), logCtx);
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  public static void processTables(
      File tmcfFile,
      List<File> csvFiles,
      char delimiter,
      BufferedWriter writer,
      Debug.Log.Builder logCtx)
      throws IOException, DCTooManyFailuresException {
    logger.debug("TMCF " + tmcfFile.getName());
    for (File csvFile : csvFiles) {
      logger.debug("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      long numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
        if (writer != null) {
          writer.write(McfUtil.serializeMcfGraph(g, false));
        }
        if (LogWrapper.loggedTooManyFailures(logCtx)) {
          throw new DCTooManyFailuresException("processTables encountered too many failures");
        }
        maybePrintStatus(numRowsProcessed, "rows", csvFile.getName(), logCtx);
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
  }

  private static void maybePrintStatus(
      long count, String thing, String fileName, Debug.Log.Builder logCtx) {
    if (count > 0 && count % 100000 == 0) {
      logger.info(
          "Processed {} {} of {}.  {}.", count, thing, fileName, LogWrapper.logSummary(logCtx));
    }
  }
}
