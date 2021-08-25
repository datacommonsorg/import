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

package org.datacommons.util;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

public class FileGroup {
  private File tmcfFile;
  private List<File> csvFiles;
  private List<File> mcfFiles;
  int nTsv;

  public FileGroup(File tmcfFile, List<File> csvFiles, List<File> mcfFiles, int nTsv) {
    this.tmcfFile = tmcfFile;
    this.csvFiles = csvFiles;
    this.mcfFiles = mcfFiles;
    this.nTsv = nTsv;
  }

  public File GetTmcf() {
    return this.tmcfFile;
  }

  public List<File> GetCsv() {
    return this.csvFiles;
  }

  public List<File> GetMcf() {
    return this.mcfFiles;
  }

  public int GetNumTsv() {
    return this.nTsv;
  }

  public static FileGroup Build(File[] files, CommandLine.Model.CommandSpec spec, Logger logger) {
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();
    List<File> mcfFiles = new ArrayList<>();
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
    if (!csvFiles.isEmpty() && tmcfFiles.size() != 1) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please provide one .tmcf file with CSV/TSV files");
    }
    if (tmcfFiles.isEmpty()) {
      return new FileGroup(null, csvFiles, mcfFiles, nTsv);
    } else {
      return new FileGroup(tmcfFiles.get(0), csvFiles, mcfFiles, nTsv);
    }
  }
}
