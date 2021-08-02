package org.datacommons.lint;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfParser;
import org.datacommons.util.TmcfCsvParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

@CommandLine.Command(
    name = "DCLint",
    mixinStandardHelpOptions = true,
    version = "Lint 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = {CommandLine.HelpCommand.class, Check.class})
class Lint {
  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      defaultValue = "/tmp",
      scope = CommandLine.ScopeType.INHERIT)
  private File outputDirectory;

  public static void main(String... args) {
    int exitCode = new CommandLine(new Lint()).execute(args);
    System.exit(exitCode);
  }
}

@CommandLine.Command(name = "check", description = "Check syntax of input files.")
class Check implements Callable<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(Check.class);

  @CommandLine.Parameters(
      arity = "1..*",
      description =
          "List of input files. The validity "
              + "depends on the --type option. For tmcfCsv, the first file should be the TMCF, followed"
              + " by a CSV, and there must exactly be two.")
  private File[] files;

  enum InputType {
    mcf,
    tmcf,
    tmcfCsv
  };

  @CommandLine.Option(
      names = {"-t", "--type"},
      description = "Types: ${COMPLETION-CANDIDATES}",
      required = true)
  private InputType type;

  @CommandLine.Option(
      names = {"-r", "--isResolved"},
      defaultValue = "false",
      description = "Whether the input MCF file is resolved. Relevant only when --type=mcf")
  private boolean isResolved;

  @CommandLine.Option(
      names = {"-d", "--delimiter"},
      defaultValue = ",",
      description = "Delimiter of the CSV file. Relevant only when --type=tmcfcsv")
  private char delimiter;

  @Override
  public Integer call() throws IOException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    if (type == InputType.tmcfCsv) {
      if (files.length <= 2) {
        System.err.println("Require two files for option 'tmcfcsv'");
        return -1;
      }
      logger.info("TMCF " + files[0].getPath());
      for (int i = 1; i < files.length; i++) {
        logger.info("Processing CSV " + files[i].getPath());
        TmcfCsvParser parser =
            TmcfCsvParser.init(files[0].getPath(), files[i].getPath(), delimiter, logCtx);
        Mcf.McfGraph g;
        int numNodesProcessed = 0, numRowsProcessed = 0;
        while ((g = parser.parseNextRow()) != null) {
          numRowsProcessed++;
          numNodesProcessed += g.getNodesCount();
        }
        logger.info(
            "Processed CSV {} ({} rows, {} nodes)",
            files[i].getPath(),
            numRowsProcessed,
            numNodesProcessed);
      }
    } else {
      // MCF or TMCF
      Mcf.McfType mcf_type =
          (type == InputType.mcf ? Mcf.McfType.INSTANCE_MCF : Mcf.McfType.TEMPLATE_MCF);
      boolean resolved = (type == InputType.mcf ? isResolved : false);
      for (File file : files) {
        int numNodesProcessed = 0;
        logger.info("Processing {}", file.getPath());
        McfParser parser = McfParser.init(mcf_type, file.getPath(), resolved);
        Mcf.McfGraph n;
        while ((n = parser.parseNextNode()) != null) {
          numNodesProcessed++;
        }
        logger.info("Processed {} ({} nodes)", file.getPath(), numNodesProcessed);
      }
    }
    return 0;
  }
}
