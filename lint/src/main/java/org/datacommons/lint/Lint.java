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
    version = "DCLint 0.1",
    description = "Tool for use in developing datasets for Data Commons.")
class Lint implements Callable<Integer> {
  private static final Logger logger = LoggerFactory.getLogger(Lint.class);

  @CommandLine.Parameters(
      arity = "1..*",
      description =
          ("List of input files. This depends on the --format option. "
              + "For 'tmcf', the files are expected to be "
              + "Template MCF files. For 'mcf', the files are expected to be Instance MCF "
              + "files. For tmcfCsv, the first file should be a Template MCF file, followed "
              + "by one or more CSVs compatible with the provided TMCF file."))
  private File[] files;

  enum FormatType {
    mcf,
    tmcf,
    tmcfCsv
  };

  @CommandLine.Option(
      names = {"-f", "--format"},
      description = "Format of input files: ${COMPLETION-CANDIDATES}",
      required = true)
  private FormatType formatType;

  enum CommandType {
    chk,
    gen
  };

  @CommandLine.Option(
      names = {"-c", "--cmd"},
      description =
          "The command to run: ${COMPLETION-CANDIDATES}.  'chk' runs "
              + "syntax checks on the input files.  'gen' produces instance MCF output, "
              + "and only makes sense when --format is 'tmcfCsv'. Default: 'chk'",
      defaultValue = "chk",
      required = true)
  private CommandType cmdType;

  @CommandLine.Option(
      names = {"-d", "--delimiter"},
      defaultValue = ",",
      description =
          "Delimiter of the input CSV file. Relevant only when --format is 'tmcfCsv'. "
              + "Default: ',' (comma)")
  private char delimiter;

  @CommandLine.Option(
      names = {"-o", "--outputDir"},
      description = "Directory to write output files. Default: /tmp",
      defaultValue = "/tmp",
      scope = CommandLine.ScopeType.INHERIT)
  private File outputDir;

  @CommandLine.Option(
      names = {"-r", "--isResolved"},
      defaultValue = "false",
      description =
          "Indicates whether the input file is resolved. Relevant only when "
              + "--format is "
              + "'mcf'. Default: false")
  private boolean isResolved;

  public static void main(String... args) {
    int exitCode = new CommandLine(new Lint()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public Integer call() throws IOException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    if (formatType == FormatType.tmcfCsv) {
      if (files.length < 2) {
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
          (formatType == FormatType.mcf ? Mcf.McfType.INSTANCE_MCF : Mcf.McfType.TEMPLATE_MCF);
      boolean resolved = (formatType == FormatType.mcf ? isResolved : false);
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
