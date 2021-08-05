package org.datacommons.tool;

import java.io.File;
import picocli.CommandLine;

// TODO: Add e2e tests once Debug.Log is fully plumbed in.
@CommandLine.Command(
    name = "dc-import",
    mixinStandardHelpOptions = true,
    version = "dc-import 0.1",
    description = "Tool for use in developing datasets for Data Commons.",
    subcommands = {Lint.class, GenMcf.class})
class Main {
  @CommandLine.Option(
      names = {"-o", "--output-dir"},
      description = "Directory to write output files. Default is current working directory.",
      scope = CommandLine.ScopeType.INHERIT)
  public File outputDir;

  public static void main(String... args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }
}
