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

  public static FileGroup build(
      File[] files,
      CommandLine.Model.CommandSpec spec,
      Character overrideDelimiter,
      Logger logger) {
    List<File> tmcfFiles = new ArrayList<>();
    List<File> csvFiles = new ArrayList<>();
    List<File> mcfFiles = new ArrayList<>();
    List<File> jsonLdFiles = new ArrayList<>();
    List<File> tmplJsonLdFiles = new ArrayList<>();
    int nTsv = 0;
    for (File file : files) {
      String lowerPath = file.getPath().toLowerCase();
      if (lowerPath.endsWith(".tmpl.jsonld")) {
        tmplJsonLdFiles.add(file);
      } else if (lowerPath.endsWith(".jsonld")) {
        jsonLdFiles.add(file);
      } else if (lowerPath.contains(".mcf")) {
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

    boolean hasMcf = !mcfFiles.isEmpty() || !tmcfFiles.isEmpty();
    boolean hasJsonLd = !jsonLdFiles.isEmpty() || !tmplJsonLdFiles.isEmpty();

    if (hasMcf && hasJsonLd) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Cannot mix MCF and JSON-LD files in the same run.");
    }

    logger.info(
        "Input includes {} MCF file(s), {} TMCF file(s), {} CSV file(s), {} JSON-LD file(s), {} JSON-LD Template file(s)",
        mcfFiles.size(),
        tmcfFiles.size(),
        csvFiles.size(),
        jsonLdFiles.size(),
        tmplJsonLdFiles.size());

    // Various checks
    if (nTsv > 0 && nTsv != csvFiles.size()) {
      throw new CommandLine.ParameterException(
          spec.commandLine(), "Please do not mix .tsv and .csv files");
    }

    if (!csvFiles.isEmpty()) {
      if (hasMcf && tmcfFiles.size() != 1) {
        throw new CommandLine.ParameterException(
            spec.commandLine(), "Please provide exactly one .tmcf file with CSV/TSV files");
      }
      if (hasJsonLd && tmplJsonLdFiles.size() != 1) {
        throw new CommandLine.ParameterException(
            spec.commandLine(), "Please provide exactly one .tmpl.jsonld file with CSV/TSV files");
      }
    }

    char delim = (overrideDelimiter == null ? (nTsv > 0 ? '\t' : ',') : overrideDelimiter);

    if (hasJsonLd) {
      return new JsonLdFileGroup(csvFiles, jsonLdFiles, tmplJsonLdFiles, delim);
    } else {
      return new McfFileGroup(csvFiles, mcfFiles, tmcfFiles, delim);
    }
  }
}
