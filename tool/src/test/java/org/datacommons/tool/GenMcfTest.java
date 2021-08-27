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

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.datacommons.util.TmcfCsvParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

// To add a new test case, add a new directory in resources/org/datacommons/tool/lint. In that new
// directory, add an input directory and an output directory. In the input directory, put the test
// files you want to run the lint tool against. In the output directory, put a report.json file with
// the expected report output.
//
// These tests can be run in a mode to produce golden files, as below:
//    mvn -DgoldenFilesPrefix=$PWD/tool/src/test/resources/org/datacommons/tool test
//
public class GenMcfTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void GenMcfTest() throws IOException {
    // Set this so that the generated node IDs are deterministic
    TmcfCsvParser.TEST_mode = true;

    String goldenFilesPrefix = System.getProperty("goldenFilesPrefix");
    goldenFilesPrefix =
        "/Users/shanth/work/git/import/tool/src/test/resources/org/datacommons" + "/tool";
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("genmcf")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      System.err.println("Processing " + directory.getName());
      List<String> argsList = new ArrayList<>();
      argsList.add("genmcf");
      File[] inputFiles = new File(Path.of(directory.getPath(), "input").toString()).listFiles();
      for (File inputFile : inputFiles) {
        argsList.add(inputFile.getPath());
      }
      argsList.add("--output-dir=" + testFolder.getRoot().getPath());
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);

      Path actualGeneratedFilePath = TestUtil.getTestFilePath(testFolder, "generated.mcf");
      Path actualReportPath = TestUtil.getTestFilePath(testFolder, "report.json");

      if (goldenFilesPrefix != null && !goldenFilesPrefix.isEmpty()) {
        Path goldenGeneratedPath =
            Path.of(goldenFilesPrefix, "genmcf", directory.getName(), "output", "generated.mcf");
        Files.copy(actualGeneratedFilePath, goldenGeneratedPath, REPLACE_EXISTING);
        Path goldenReportPath =
            Path.of(goldenFilesPrefix, "genmcf", directory.getName(), "output", "report.json");
        Files.copy(actualReportPath, goldenReportPath, REPLACE_EXISTING);
      } else {
        Path expectedGeneratedFilePath =
            TestUtil.getOutputFilePath(directory.getPath(), "generated.mcf");
        Path expectedReportPath = TestUtil.getOutputFilePath(directory.getPath(), "report.json");
        TestUtil.assertReportFilesAreSimilar(
            directory,
            TestUtil.readStringFromPath(expectedReportPath),
            TestUtil.readStringFromPath(actualReportPath));
        assertEquals(
            org.datacommons.util.TestUtil.mcfFromFile(expectedGeneratedFilePath.toString()),
            org.datacommons.util.TestUtil.mcfFromFile(actualGeneratedFilePath.toString()));
      }
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
