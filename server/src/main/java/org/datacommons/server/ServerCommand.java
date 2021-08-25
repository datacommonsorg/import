// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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

// Defines the command line argument and execute startup operations.
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
  @Autowired private ObservationRepository observationRepository;

  @Override
  public Integer call() throws IOException {
    FileGroup fg = FileGroup.Build(files, spec, logger);
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), new File(".").toPath());
    Processor processor = new Processor(logCtx);
    processor.processTables(fg.GetTmcf(), fg.GetCsv(), ',', observationRepository);
    return 0;
  }
}
