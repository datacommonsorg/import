package org.datacommons.tool;

import java.io.File;
import java.io.IOException;
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
// TODO(shanth): Incorporate e2e test-cases for existence checks once this is generalized.
public class LintTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void LintTest() throws IOException {
    // Set this so that the generated node IDs are deterministic
    TmcfCsvParser.TEST_mode = true;

    String goldenFilesPrefix = System.getProperty("goldenFilesPrefix");
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("lint")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      System.err.println("Processing " + directory.getName());
      List<String> argsList = new ArrayList<>();
      argsList.add("lint");
      File[] inputFiles = new File(Path.of(directory.getPath(), "input").toString()).listFiles();
      for (File inputFile : inputFiles) {
        argsList.add(inputFile.getPath());
      }
      argsList.add("--output-dir=" + testFolder.getRoot().getPath());
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);
      Path actualReportPath = TestUtil.getTestFilePath(testFolder, "report.json");
      if (goldenFilesPrefix != null && !goldenFilesPrefix.isEmpty()) {
        Path goldenPath =
            Path.of(goldenFilesPrefix, "lint", directory.getName(), "output", "report.json");
        TestUtil.writeSortedReport(actualReportPath, goldenPath);
      } else {
        Path expectedReportPath = TestUtil.getOutputFilePath(directory.getPath(), "report.json");
        TestUtil.assertReportFilesAreSimilar(
            directory,
            TestUtil.readStringFromPath(expectedReportPath),
            TestUtil.readStringFromPath(actualReportPath));
      }
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
