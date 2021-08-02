package org.datacommons.lint;

import java.io.File;
import java.util.concurrent.Callable;
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

  @Override
  public Integer call() {
    if (type == InputType.tmcfCsv) {
    } else {
      // MCF or TMCF
      for (File file : files) {}
    }
    return 0;
  }
}
