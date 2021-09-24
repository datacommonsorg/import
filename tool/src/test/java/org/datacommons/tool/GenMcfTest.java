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
import static org.junit.Assert.assertTrue;

import com.google.common.truth.Expect;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FilenameUtils;
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
  // To ensure we test the right number of files for every test, when you add a file, add the
  // count here.
  private static Map<String, Integer> EXPECTED_FILES_TO_CHECK =
      Map.of(
          "fataltmcf", 1,
          "resolution", 4,
          "statchecks", 2,
          "successtmcf", 2);

  @Test
  public void GenMcfTest() throws IOException {
    // Set this so that the generated node IDs are deterministic
    TmcfCsvParser.TEST_mode = true;

    String goldenFilesPrefix = System.getProperty("goldenFilesPrefix");
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("genmcf")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      String testName = directory.getName();
      System.err.println(testName + ": BEGIN");
      assertTrue(EXPECTED_FILES_TO_CHECK.containsKey(testName));
      List<String> argsList = new ArrayList<>();
      argsList.add("genmcf");
      File[] inputFiles = new File(Path.of(directory.getPath(), "input").toString()).listFiles();
      List<String> expectedOutputFiles =
          new ArrayList<>(
              List.of("report.json", "instance_mcf_nodes.mcf", "failed_instance_mcf_nodes.mcf"));
      for (File inputFile : inputFiles) {
        argsList.add(inputFile.getPath());
        String fName = inputFile.getName();
        if (fName.endsWith(".csv") || fName.endsWith(".tsv")) {
          expectedOutputFiles.add(
              "table_mcf_nodes_" + FilenameUtils.removeExtension(fName) + ".mcf");
          expectedOutputFiles.add(
              "failed_table_mcf_nodes_" + FilenameUtils.removeExtension(fName) + ".mcf");
        }
      }
      argsList.add("--resolution=FULL");
      argsList.add("--stat-checks");
      argsList.add("--output-dir=" + Paths.get(testFolder.getRoot().getPath(), testName));
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);

      Integer numChecked = 0;
      if (goldenFilesPrefix != null && !goldenFilesPrefix.isEmpty()) {
        for (var f : expectedOutputFiles) {
          Path actual = TestUtil.getTestFilePath(testFolder, testName, f);
          if (!f.equals("report.json") && !new File(actual.toString()).exists()) continue;

          Path golden = Path.of(goldenFilesPrefix, "genmcf", testName, "output", f);
          Files.copy(actual, golden, REPLACE_EXISTING);
          numChecked++;
        }
      } else {
        for (var f : expectedOutputFiles) {
          Path actual = TestUtil.getTestFilePath(testFolder, testName, f);
          System.err.println("Checking for " + actual.toString());
          if (!f.equals("report.json") && !new File(actual.toString()).exists()) continue;

          Path expected = TestUtil.getOutputFilePath(directory.getPath(), f);
          if (f.equals("report.json")) {
            TestUtil.assertReportFilesAreSimilar(
                expect, TestUtil.readStringFromPath(expected), TestUtil.readStringFromPath(actual));
          } else {
            assertEquals(
                org.datacommons.util.TestUtil.mcfFromFile(expected.toString()),
                org.datacommons.util.TestUtil.mcfFromFile(actual.toString()));
          }
          numChecked++;
        }
      }
      assertEquals(numChecked, EXPECTED_FILES_TO_CHECK.get(testName));
      System.err.println(testName + ": PASSED");
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
