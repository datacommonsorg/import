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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class GenMcfTest {

  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void GenMcfTest_FatalTmcf() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("TmcfWithErrors.tmcf");
    String csv = resourceFile("Csv1.csv");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("genmcf", tmcf, csv, "--output-dir=" + testFolder.getRoot().getPath());
    File gotReportFile =
        new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    File gotGeneratedMcfFile =
        new File(Paths.get(testFolder.getRoot().getPath(), "generated.mcf").toString());
    File wantReportFile =
        new File(this.getClass().getResource("GenMcfTest_FatalTmcfReport.json").getPath());
    System.out.println(FileUtils.readFileToString(wantReportFile, StandardCharsets.UTF_8));
    System.out.println(FileUtils.readFileToString(gotReportFile, StandardCharsets.UTF_8));
    assertEquals(
        FileUtils.readFileToString(wantReportFile, StandardCharsets.UTF_8),
        FileUtils.readFileToString(gotReportFile, StandardCharsets.UTF_8));
    assertTrue(FileUtils.readFileToString(gotGeneratedMcfFile, StandardCharsets.UTF_8).isEmpty());
  }

  @Test
  public void GenMcfTest_SuccessTmcf() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("Tmcf1.tmcf");
    String csv = resourceFile("Csv1.csv");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("genmcf", tmcf, csv, "--output-dir=" + testFolder.getRoot().getPath());
    File gotReportFile =
        new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    File wantReportFile =
        new File(this.getClass().getResource("GenMcfTest_SuccessTmcfReport.json").getPath());
    Path actualGeneratedFilePath = Paths.get(testFolder.getRoot().getPath(), "generated.mcf");
    Path expectedGeneratedFilePath =
        Paths.get(this.getClass().getResource("GenMcfTest_SuccessTmcfGenerated.mcf").getPath());
    assertTrue(areSimilarGeneratedMcf(expectedGeneratedFilePath, actualGeneratedFilePath));
    assertEquals(
        FileUtils.readFileToString(wantReportFile, StandardCharsets.UTF_8),
        FileUtils.readFileToString(gotReportFile, StandardCharsets.UTF_8));
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }

  private boolean areSimilarGeneratedMcf(Path expectedFilePath, Path actualFilePath)
      throws IOException {
    Iterator<String> expectedFileLines = Files.lines(expectedFilePath).iterator();
    Iterator<String> actualFileLines = Files.lines(actualFilePath).iterator();
    while (expectedFileLines.hasNext() && actualFileLines.hasNext()) {
      String expectedLine = expectedFileLines.next();
      String actualLine = actualFileLines.next();
      if (expectedLine.contains("Node") && !expectedLine.contains("dcid")) {
        continue;
      }
      if (!actualLine.trim().equals(expectedLine.trim())) {
        return false;
      }
    }
    return true;
  }
}
