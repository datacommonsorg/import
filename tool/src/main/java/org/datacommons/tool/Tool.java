package org.datacommons.tool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfParser;
import org.datacommons.util.McfUtil;
import org.datacommons.util.TmcfCsvParser;
import picocli.CommandLine;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

// TODO: Add e2e tests once Debug.Log is fully plumbed in.
@CommandLine.Command(
    name = "dc-import",
    mixinStandardHelpOptions = true,
    version = "dc-import 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = {Lint.class, GenMcf.class})
class Tool {
  @CommandLine.Option(
      names = {"-o", "--output-dir"},
      description = "Directory to write output files. Default is current working directory.",
      scope = CommandLine.ScopeType.INHERIT)
  public File outputDir;

  public static void main(String... args) {
    System.exit(new CommandLine(new Tool()).execute(args));
  }

  public static void HandleNodeFormat(
      Mcf.McfType type, File file, Debug.Log.Builder logCtx, Logger logger) throws IOException {
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

  public static void HandleTableFormat(
      File tmcfFile,
      List<File> csvFiles,
      char delimiter,
      BufferedWriter writer,
      Debug.Log.Builder logCtx,
      Logger logger)
      throws IOException {
    logger.debug("TMCF " + tmcfFile.getName());
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
      }
      logger.info(
          "Checked CSV {} ({} rows, {} nodes)",
          csvFile.getName(),
          numRowsProcessed,
          numNodesProcessed);
    }
  }
}

@CommandLine.Command(name = "lint", description = "Run various checks on input MCF/TMCF/CSV files")
class Lint implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(Lint.class);

  @CommandLine.Parameters(
      arity = "1..*",
      description =
          ("List of input files. The file extensions are used to infer the format. "
              + "Valid extensions include .mcf for Instance MCF, .tmcf for Template MCF, "
              + ".csv for tabular text files delimited by comma (overridden with -d), and .tsv "
              + "for tab-delimited tabular files."))
  private File[] files;

  @CommandLine.Option(
      names = {"-d", "--delimiter"},
      description =
          "Delimiter of the input CSV files. Default is ',' for .csv files and '\\t' for "
              + ".tsv files.",
      scope = CommandLine.ScopeType.INHERIT)
  private Character delimiter;

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
        "Input includes {} MCF file(s), {} TMCF file(s), {} CSV file(s)",
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
          spec.commandLine(), "Please provide one .tmcf file with CSV/TSV files");
    }
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    for (File f : mcfFiles) {
      Tool.HandleNodeFormat(Mcf.McfType.INSTANCE_MCF, f, logCtx, logger);
    }
    if (!csvFiles.isEmpty()) {
      Tool.HandleTableFormat(tmcfFiles.get(0), csvFiles, delimiter, null, logCtx, logger);
    } else {
      for (File f : tmcfFiles) {
        Tool.HandleNodeFormat(Mcf.McfType.TEMPLATE_MCF, f, logCtx, logger);
      }
    }
    return 0;
  }
}

@CommandLine.Command(name = "genmcf", description = "Generate Instance MCF from TMCF/CSV files")
class GenMcf implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(GenMcf.class);

  @CommandLine.Parameters(
      arity = "1..*",
      description =
          ("List of input files. The file extensions are used to infer the format. "
              + "Valid extensions include .tmcf for Template MCF, "
              + ".csv for tabular text files delimited by comma (overridden with -d), and .tsv "
              + "for tab-delimited tabular files. Note that .mcf is not a valid input."))
  private File[] files;

  @CommandLine.Option(
      names = {"-d", "--delimiter"},
      description =
          "Delimiter of the input CSV files. Default is ',' for .csv files and '\\t' for "
              + ".tsv files.",
      scope = CommandLine.ScopeType.INHERIT)
  private Character delimiter;

  @CommandLine.ParentCommand private Tool parent;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec; // injected by picocli

  @Override
  public Integer call() throws IOException {
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();
    int nTsv = 0;
    for (File file : files) {
      String lowerPath = file.getPath().toLowerCase();
      if (lowerPath.endsWith(".tmcf")) {
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
        "Input includes {} TMCF file(s), {} CSV file(s)", tmcfFiles.size(), csvFiles.size());
    if (csvFiles.isEmpty()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide one or more CSV/TSV files");
    }
    if (tmcfFiles.size() != 1) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide exactly one .tmcf file");
    }
    if (nTsv > 0 && nTsv != csvFiles.size()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please do not mix .tsv and .csv files");
    }
    if (delimiter == null) {
      delimiter = nTsv > 0 ? '\t' : ',';
    }
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    Path outFile =
        Paths.get(parent.outputDir == null ? "." : parent.outputDir.getPath(), "generated.mcf");
    logger.info("Writing to {}", outFile.toString());
    BufferedWriter writer = new BufferedWriter(new FileWriter(outFile.toString()));
    Tool.HandleTableFormat(tmcfFiles.get(0), csvFiles, delimiter, writer, logCtx, logger);
    writer.close();
    return 0;
  }
}
