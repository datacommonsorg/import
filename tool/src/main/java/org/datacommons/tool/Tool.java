package org.datacommons.tool;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfParser;
import org.datacommons.util.TmcfCsvParser;
import picocli.CommandLine;

@CommandLine.Command(
    name = "dc-import",
    mixinStandardHelpOptions = true,
    version = "dc-import 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = Lint.class)
class Tool {
  public static void main(String... args) {
    System.exit(new CommandLine(new Tool()).execute(args));
  }
}

@CommandLine.Command(name = "lint", description = "Run various checks on input")
class Lint implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(Lint.class);

  @CommandLine.Parameters(
      arity = "1..*",
      description =
          ("List of input files. The file extensions are used to infer the format. "
              + "Valid extensions include .mcf for Instance MCF, .tmcf for Template MCF, "
              + ".csv for a tabular text file by default separated by comma and .tsv "
              + "for tab-delimited tabular file."))
  private File[] files;

  @CommandLine.Option(
      names = {"-d", "--delimiter"},
      description =
          "Delimiter of the input CSV files. Default is ',' for .csv files and '\\t' for "
              + ".tsv files.",
      scope = CommandLine.ScopeType.INHERIT)
  private Character delimiter;

  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      description = "Directory to write output files. Default: /tmp",
      defaultValue = "/tmp",
      scope = CommandLine.ScopeType.INHERIT)
  private File outputDir;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec; // injected by picocli

  @Override
  public Integer call() throws IOException {
    List<File> mcfFiles = new ArrayList<>();
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();
    int nTsv = 0;
    for (File file : files) {
      String lowerPath = file.getPath().toLowerCase();
      if (lowerPath.endsWith(".mcf")) {
        mcfFiles.add(file);
      } else if (lowerPath.endsWith(".tmcf")) {
        tmcfFiles.add(file);
      } else if (lowerPath.endsWith(".csv")) {
        csvFiles.add(file);
      } else if (lowerPath.endsWith(".tsv")) {
        nTsv++;
        csvFiles.add(file);
      } else {
        throw new CommandLine.ParameterException(
            spec.commandLine(), "Found an unsupported file type: " + file.getPath());
      }
    }
    logger.info(
        "Input includes {} MCF files, {} TMCF files, {} CSV files",
        mcfFiles.size(),
        tmcfFiles.size(),
        csvFiles.size());
    // Various checks
    if (nTsv > 0 && nTsv != csvFiles.size()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please do not mix .tsv and .csv files");
    }
    if (delimiter == null) {
      delimiter = nTsv > 0 ? '\t' : ',';
    }
    if (!csvFiles.isEmpty() && tmcfFiles.size() != 1) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide exactly one .tmcf file with CSV/TSV files");
    }
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    for (File f : mcfFiles) {
      HandleNodeFormat(Mcf.McfType.INSTANCE_MCF, f, logCtx);
    }
    if (!csvFiles.isEmpty()) {
      HandleTableFormat(tmcfFiles.get(0), csvFiles, delimiter, logCtx);
    } else {
      for (File f : tmcfFiles) {
        HandleNodeFormat(Mcf.McfType.TEMPLATE_MCF, f, logCtx);
      }
    }
    return 0;
  }

  private void HandleNodeFormat(Mcf.McfType type, File file, Debug.Log.Builder logCtx)
      throws IOException {
    int numNodesProcessed = 0;
    logger.debug("Checking {}", file.getName());
    // TODO: isResolved is more allowing, be stricter.
    McfParser parser = McfParser.init(type, file.getPath(), false);
    Mcf.McfGraph n;
    while ((n = parser.parseNextNode()) != null) {
      numNodesProcessed++;
    }
    logger.info("Checked {} with {} nodes", file.getName(), numNodesProcessed);
  }

  private void HandleTableFormat(
      File tmcfFile, List<File> csvFiles, char delimiter, Debug.Log.Builder logCtx)
      throws IOException {
    logger.info("TMCF " + tmcfFile.getName());
    for (File csvFile : csvFiles) {
      logger.debug("Checking CSV " + csvFile.getPath());
      TmcfCsvParser parser =
          TmcfCsvParser.init(tmcfFile.getPath(), csvFile.getPath(), delimiter, logCtx);
      Mcf.McfGraph g;
      int numNodesProcessed = 0, numRowsProcessed = 0;
      while ((g = parser.parseNextRow()) != null) {
        numNodesProcessed += g.getNodesCount();
        numRowsProcessed++;
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
  }
}
