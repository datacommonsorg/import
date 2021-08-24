package org.datacommons.server;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.util.FileGroup;
import org.datacommons.util.LogWrapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;

@Component
@Command(name = "command", mixinStandardHelpOptions = true)
public class ServerCommand implements Callable<Integer> {
  private static final Logger logger = LogManager.getLogger(ServerCommand.class);

  @Parameters(
      arity = "1..*",
      description =
          ("List of input files. The file extensions are used to infer the format. "
              + "Valid extensions include .tmcf for Template MCF, "
              + ".csv for tabular text files delimited by comma (overridden with -d), and .tsv "
              + "for tab-delimited tabular files."))
  private File[] files;

  @Spec Model.CommandSpec spec; // injected by picocli

  @Autowired private ObservationRepository obsRepo;

  @Override
  public Integer call() {
    FileGroup fg = FileGroup.Build(files, spec, logger);
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
    Processor processor = new Processor(logCtx);
    try {
      processor.processTables(fg.GetTmcf(), fg.GetCsv(), ',', obsRepo);
    } catch (IOException ex) {
      System.out.printf("Get error %s", ex);
    }
    return 0;
  }
}
