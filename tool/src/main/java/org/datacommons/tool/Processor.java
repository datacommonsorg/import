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
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfParser;
import org.datacommons.util.McfUtil;
import org.datacommons.util.TmcfCsvParser;

class TooManyFailuresException extends Exception {
  public TooManyFailuresException() {}

  public TooManyFailuresException(String message) {
    super(message);
  }
}

public class Processor {
  private static final int MAX_ERROR_LIMIT = 10;

  public static void processNodes(
      Mcf.McfType type, File file, Debug.Log.Builder logCtx, Logger logger)
      throws IOException, TooManyFailuresException {
    int numNodesProcessed = 0;
    logger.debug("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    McfParser parser = McfParser.init(type, file.getPath(), false, logCtx);
    Mcf.McfGraph n;
    boolean hasError = false;
    while ((n = parser.parseNextNode()) != null) {
      numNodesProcessed++;
      if (shouldBail(logCtx, logger)) {
        throw new TooManyFailuresException("processNodes encountered too many failures");
      }
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  public static void processTables(
      File tmcfFile,
      List<File> csvFiles,
      char delimiter,
      BufferedWriter writer,
      Debug.Log.Builder logCtx,
      Logger logger)
      throws IOException, TooManyFailuresException {
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
          throw new TooManyFailuresException("processTables encountered too many failures");
        }
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
  }

  public static void writeLog(Debug.Log.Builder logCtx, Path logPath, Logger logger)
      throws InvalidProtocolBufferException, IOException {
    logger.info("Writing log to {}", logPath.toString());
    File logFile = new File(logPath.toString());
    FileUtils.writeStringToFile(
        logFile, JsonFormat.printer().print(logCtx), StandardCharsets.UTF_8);
  }

  private static boolean shouldBail(Debug.Log.Builder logCtx, Logger logger) {
    if (logCtx.getLevelSummaryOrDefault("LEVEL_FATAL", 0) > 0) {
      logger.error("Found a fatal log error");
      return true;
    }
    if (logCtx.getLevelSummaryOrDefault("LEVEL_ERROR", 0) > MAX_ERROR_LIMIT) {
      logger.error("Exceeded over {} errors!", MAX_ERROR_LIMIT);
      return true;
    }
    return false;
  }
}
