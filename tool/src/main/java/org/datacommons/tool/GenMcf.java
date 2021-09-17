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

package org.datacommons.tool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.util.FileGroup;
import org.datacommons.util.LogWrapper;
import picocli.CommandLine;

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

  @CommandLine.ParentCommand private Main parent;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec; // injected by picocli

  @Override
  public Integer call() throws IOException {
    if (!parent.outputDir.exists()) {
      parent.outputDir.mkdirs();
    }
    Processor.Args args = new Processor.Args();
    args.doExistenceChecks = parent.doExistenceChecks;
    args.resolutionMode = parent.resolutionMode;
    args.verbose = parent.verbose;
    args.fileGroup = FileGroup.build(files, spec, delimiter, logger);
    args.logCtx = new LogWrapper(Debug.Log.newBuilder(), parent.outputDir.toPath());
    args.outputFiles = new HashMap<>();
    for (Processor.OutputFileType type : Processor.OutputFileType.values()) {
      var fName = type.name().toLowerCase() + ".mcf";
      args.outputFiles.put(type, Paths.get(parent.outputDir.getPath(), fName));
    }

    // Process all the things.
    return Processor.process(args);
  }
}
