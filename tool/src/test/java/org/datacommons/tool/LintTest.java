package org.datacommons.tool;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import com.google.common.truth.Expect;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.datacommons.util.SummaryReportGenerator;
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
  @Rule public final Expect expect = Expect.create();

  @Test
  public void LintTest() throws IOException {
    // Set this so that the generated node IDs are deterministic
    TmcfCsvParser.TEST_mode = true;
    SummaryReportGenerator.TEST_mode = true;

    String goldenFilesPrefix = System.getProperty("goldenFilesPrefix");
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("lint")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      String testName = directory.getName();
      System.err.println(testName + ": BEGIN");
      List<String> argsList = new ArrayList<>();
      argsList.add("lint");
      File[] inputFiles = new File(Path.of(directory.getPath(), "input").toString()).listFiles();
      List<String> expectedOutputFiles =
          new ArrayList<>(List.of("report.json", "summary_report.html"));
      for (File inputFile : inputFiles) {
        argsList.add(inputFile.getPath());
      }
      argsList.add(
          "--output-dir=" + Paths.get(testFolder.getRoot().getPath(), directory.getName()));
      argsList.add("--summary-report=True");
      String[] args = argsList.toArray(new String[argsList.size()]);
      cmd.execute(args);

      if (goldenFilesPrefix != null && !goldenFilesPrefix.isEmpty()) {
        for (var f : expectedOutputFiles) {
          Path actual = TestUtil.getTestFilePath(testFolder, testName, f);
          Path golden = Path.of(goldenFilesPrefix, "lint", testName, "output", f);
          Files.copy(actual, golden, REPLACE_EXISTING);
        }
      } else {
        for (var f : expectedOutputFiles) {
          Path actual = TestUtil.getTestFilePath(testFolder, testName, f);
          Path expected = TestUtil.getOutputFilePath(directory.getPath(), f);
          if (f.equals("report.json")) {
            TestUtil.assertReportFilesAreSimilar(
                expect, TestUtil.readStringFromPath(expected), TestUtil.readStringFromPath(actual));
          } else if (f.equals("summary_report.html")) {
            TestUtil.assertHtmlFilesAreSimilar(
                TestUtil.readStringFromPath(expected), TestUtil.readStringFromPath(actual));
          }
        }
      }
      System.err.println(testName + ": PASSED");
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
