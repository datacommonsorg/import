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
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datacommons.proto.Debug;
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
  public Integer call() throws IOException, InvalidProtocolBufferException {
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();
    int nTsv = 0;
    for (File file : files) {
      String lowerPath = file.getPath().toLowerCase();
      if (lowerPath.endsWith(".tmcf")) {
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
        "Input includes {} TMCF file(s), {} CSV file(s)", tmcfFiles.size(), csvFiles.size());
    if (csvFiles.isEmpty()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide one or more CSV/TSV files");
    }
    if (tmcfFiles.size() != 1) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide exactly one .tmcf file");
    }
    if (nTsv > 0 && nTsv != csvFiles.size()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please do not mix .tsv and .csv files");
    }
    if (delimiter == null) {
      delimiter = nTsv > 0 ? '\t' : ',';
    }

    Path outPath = Paths.get(parent.outputDir.getPath(), "generated.mcf");
    logger.info("Writing generated MCF to {}", outPath.toAbsolutePath().normalize().toString());
    BufferedWriter writer = new BufferedWriter(new FileWriter(outPath.toString()));

    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), parent.outputDir.toPath());
    Processor processor = new Processor(false, logCtx);
    Integer retVal = 0;
    try {
      processor.processTables(tmcfFiles.get(0), csvFiles, delimiter, writer);
    } catch (DCTooManyFailuresException | InterruptedException ex) {
      // Regardless of the failures, we will dump the logCtx and exit.
      retVal = -1;
    }
    writer.close();
    logCtx.persistLog(false);
    return retVal;
  }
}
