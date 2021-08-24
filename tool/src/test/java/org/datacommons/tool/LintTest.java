package org.datacommons.tool;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class LintTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void LintTest() throws IOException {
    Main app = new Main();
    CommandLine cmd = new CommandLine(app);
    File[] testDirectories = new File(resourceFile("lint")).listFiles(File::isDirectory);
    for (File directory : testDirectories) {
      List<String> argsList = new ArrayList<>();
      argsList.add("lint");
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
    }
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
