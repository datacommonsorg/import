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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

// To add a new test case, add a new directory in resources/org/datacommons/tool/lint. In that new
// directory, add an input directory and an output directory. In the input directory, put the test
// files you want to run the lint tool against. In the output directory, put a report.json file with
// the expected report output.
public class GenMcfTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void GenMcfTest() throws IOException {
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("genmcf")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      List<String> argsList = new ArrayList<>();
      argsList.add("genmcf");
      File[] inputFiles = new File(Path.of(directory.getPath(), "input").toString()).listFiles();
      for (File inputFile : inputFiles) {
        argsList.add(inputFile.getPath());
      }
      argsList.add("--output-dir=" + testFolder.getRoot().getPath());
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);
      String actualReportString = TestUtil.getStringFromTestFile(testFolder, "report.json");
      String expectedReportString = TestUtil.getStringFromOutputReport(directory.getPath());
      TestUtil.assertReportFilesAreSimilar(expectedReportString, actualReportString);
      Path actualGeneratedFilePath = Paths.get(testFolder.getRoot().getPath(), "generated.mcf");
      Path expectedGeneratedFilePath = Path.of(directory.getPath(), "output", "generated.mcf");
      assertTrue(areSimilarGeneratedMcf(expectedGeneratedFilePath, actualGeneratedFilePath));
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }

  // When testing GeneratedMcf, can't just check against an expected file because When generating
  // SVO MCF from csv and tmcf, Nodes will be assigned an ID that may not always be the same
  private boolean areSimilarGeneratedMcf(Path expectedFilePath, Path actualFilePath)
      throws IOException {
    Iterator<String> actualFileLines = Files.lines(actualFilePath).iterator();
    if (!new File(expectedFilePath.toString()).isFile()) {
      return !actualFileLines.hasNext();
    }
    Iterator<String> expectedFileLines = Files.lines(expectedFilePath).iterator();
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
