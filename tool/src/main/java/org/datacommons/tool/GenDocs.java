package org.datacommons.tool;

import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

@CommandLine.Command(name = "docs", description = "Generate documentation .md classes")
class GenDocs implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(GenDocs.class);

  @CommandLine.ParentCommand private Main parent;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec; // injected by picocli

  @Override
  public Integer call() {

    System.out.println("hello, docs!");
    return 0;
  }
}
