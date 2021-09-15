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

import com.google.common.truth.Expect;
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
  @Rule public final Expect expect = Expect.create();

  @Test
  public void GenMcfTest() throws IOException {
    // Set this so that the generated node IDs are deterministic
    TmcfCsvParser.TEST_mode = true;

    String goldenFilesPrefix = System.getProperty("goldenFilesPrefix");
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
      argsList.add("--resolution");
      argsList.add("--output-dir=" + testFolder.getRoot().getPath());
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);

      List<String> files =
          List.of(
              "report.json",
              "table_nodes.mcf",
              "failed_table_nodes.mcf",
              "nodes.mcf",
              "failed_nodes.mcf");

      if (goldenFilesPrefix != null && !goldenFilesPrefix.isEmpty()) {
        for (var f : files) {
          Path actual = TestUtil.getTestFilePath(testFolder, f);
          if (!f.equals("report.json") && !new File(actual.toString()).exists()) continue;

          Path golden = Path.of(goldenFilesPrefix, "genmcf", directory.getName(), "output", f);
          Files.copy(actual, golden, REPLACE_EXISTING);
        }
      } else {
        for (var f : files) {
          Path actual = TestUtil.getTestFilePath(testFolder, f);
          if (!f.equals("report.json") && !new File(actual.toString()).exists()) continue;

          Path expected = TestUtil.getOutputFilePath(directory.getPath(), f);
          if (f.equals("report.json")) {
            TestUtil.assertReportFilesAreSimilar(
                expect,
                directory,
                TestUtil.readStringFromPath(expected),
                TestUtil.readStringFromPath(actual));
          } else {
            assertEquals(
                org.datacommons.util.TestUtil.mcfFromFile(expected.toString()),
                org.datacommons.util.TestUtil.mcfFromFile(actual.toString()));
          }
        }
      }
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
