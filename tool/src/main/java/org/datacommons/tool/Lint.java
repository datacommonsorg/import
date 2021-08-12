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

import com.google.protobuf.InvalidProtocolBufferException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.datacommons.util.LogWrapper;
import picocli.CommandLine;

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

  @CommandLine.ParentCommand private Main parent;

  @CommandLine.Spec CommandLine.Model.CommandSpec spec; // injected by picocli

  @Override
  public Integer call() throws IOException, InvalidProtocolBufferException {
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
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), parent.outputDir.toPath());
    Processor processor = new Processor(logCtx);
    Integer retVal = 0;
    try {
      for (File f : mcfFiles) {
        processor.processNodes(Mcf.McfType.INSTANCE_MCF, f);
      }
      if (!csvFiles.isEmpty()) {
        processor.processTables(tmcfFiles.get(0), csvFiles, delimiter, null);
      } else {
        for (File f : tmcfFiles) {
          processor.processNodes(Mcf.McfType.TEMPLATE_MCF, f);
        }
      }
    } catch (DCTooManyFailuresException ex) {
      // Regardless of the failures, we will dump the logCtx and exit.
      retVal = -1;
    }
    logCtx.persistLog(false);
    return retVal;
  }
}
