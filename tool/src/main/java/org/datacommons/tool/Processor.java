package org.datacommons.tool;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
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

  private static final int MAX_ERROR_LIMIT = 50;

  public static void processNodes(Mcf.McfType type, File file, Debug.Log.Builder logCtx)
      throws IOException, DCTooManyFailuresException {
    int numNodesProcessed = 0;
    logger.debug("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    boolean hasError = false;
    while ((n = parser.parseNextNode()) != null) {
      numNodesProcessed++;
      if (shouldBail(logCtx, logger)) {
        throw new DCTooManyFailuresException("processNodes encountered too many failures");
      }
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
    boolean hasError = false;
    for (File csvFile : csvFiles) {
      logger.debug("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      int numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
        if (writer != null) {
          writer.write(McfUtil.serializeMcfGraph(g, false));
        }
        if (shouldBail(logCtx, logger)) {
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

  public static void writeLog(Debug.Log.Builder logCtx, Path logPath)
      throws InvalidProtocolBufferException, IOException {
    if (logCtx.getLevelSummaryMap().isEmpty()) {
      logger.info("Found no warnings or errors!");
      return;
    }
    logger.info("Failures: {}.  Writing details to {}", logSummary(logCtx), logPath.toString());
    File logFile = new File(logPath.toString());
    FileUtils.writeStringToFile(
        logFile, JsonFormat.printer().print(logCtx), StandardCharsets.UTF_8);
  }

  private static String logSummary(Debug.Log.Builder logCtx) {
    return logCtx.getLevelSummaryMap().getOrDefault("LEVEL_FATAL", 0L)
        + " fatal, "
        + logCtx.getLevelSummaryMap().getOrDefault("LEVEL_ERROR", 0L)
        + " error(s), "
        + logCtx.getLevelSummaryMap().getOrDefault("LEVEL_WARNING", 0L)
        + " warning(s)";
  }

  private static boolean shouldBail(Debug.Log.Builder logCtx, Logger logger) {
    if (logCtx.getLevelSummaryOrDefault("LEVEL_FATAL", 0) > 0) {
      logger.error("Found a fatal failure. Quitting!");
      return true;
    }
    if (logCtx.getLevelSummaryOrDefault("LEVEL_ERROR", 0) > MAX_ERROR_LIMIT) {
      logger.error("Found too many failures. Quitting!");
      return true;
    }
    return false;
  }
}
