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
import java.util.List;
import org.apache.logging.log4j.Logger;
import picocli.CommandLine;

public abstract class FileGroup {
  protected List<File> csvFiles;
  protected char delimiter;

  public FileGroup(List<File> csvFiles, char delimiter) {
    this.csvFiles = csvFiles;
    this.delimiter = delimiter;
  }

  public List<File> getCsvs() {
    return csvFiles;
  }

  public char delimiter() {
    return delimiter;
  }

  // TODO: Remove this method, it's only used for the server command.
  public File getTmcf() {
    return null;
  }

  public static FileGroup build(
      File[] files,
      CommandLine.Model.CommandSpec spec,
      Character overrideDelimiter,
      Logger logger) {
    List<File> fileList = java.util.Arrays.asList(files);
    List<ParserStrategy> strategies =
        java.util.Arrays.asList(new JsonLdParserStrategy(), new McfParserStrategy());

    for (ParserStrategy strategy : strategies) {
      if (strategy.supports(fileList)) {
        logger.info(
            "Delegating file grouping to strategy: {}", strategy.getClass().getSimpleName());
        return strategy.createGroup(fileList, overrideDelimiter);
      }
    }

    throw new CommandLine.ParameterException(
        spec.commandLine(), "Found no supporting strategy for the provided input files.");
  }
}
