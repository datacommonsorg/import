package org.datacommons.lint;

import java.io.File;
import java.util.concurrent.Callable;
import picocli.CommandLine;

@CommandLine.Command(
    name = "DCLint",
    mixinStandardHelpOptions = true,
    version = "Lint 0.1",
    description = "Tool for use in importing datasets into Data Commons.",
    subcommands = {CommandLine.HelpCommand.class, Mcf.class, Csv.class})
class Lint {
  @CommandLine.Option(
      names = {"-o", "-outputDir"},
      defaultValue = "/tmp",
      scope = CommandLine.ScopeType.INHERIT)
  private File outputDirectory;

  @CommandLine.Option(
      names = {"-m", "-mode"},
      defaultValue = "CHECK",
      scope = CommandLine.ScopeType.INHERIT)
  private RunMode runMode;

  enum RunMode {
    CHECK,
    GENERATE
  }

  public static void main(String... args) {
    int exitCode = new CommandLine(new Lint()).execute(args);
    System.exit(exitCode);
  }
}

@CommandLine.Command(name = "mcf", description = "Tools for importing instance MCF.")
class Mcf implements Callable<Integer> {
  @CommandLine.Parameters(arity = "1..*", description = "List of MCF files.")
  private File[] mcfFiles;

  @Override
  public Integer call() {
    System.out.println("Called mcf!");
    return 0;
  }
}

@CommandLine.Command(name = "csv", description = "Tools for import CSV with a TMCF.")
class Csv implements Callable<Integer> {
  @CommandLine.Parameters(arity = "1", description = "CSV File")
  private File csvFile;

  @CommandLine.Option(
      names = {"-tmcf"},
      description = "TMCF file",
      required = true)
  private File tmcfFile;

  @Override
  public Integer call() {
    System.out.println("Called csv!");
    return 0;
  }
}
