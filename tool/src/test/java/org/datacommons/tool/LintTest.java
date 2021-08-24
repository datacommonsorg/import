package org.datacommons.tool;

import java.io.IOException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

// TODO(shanth): Incorporate e2e test-cases for existence checks once this is generalized.
public class LintTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void LintTest_McfAndTmcf() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("TmcfWithErrors.tmcf");
    String mcf = resourceFile("LintTest.mcf");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("lint", mcf, tmcf, "--output-dir=" + testFolder.getRoot().getPath());
    String actualReportString = TestUtil.getStringFromTestFile(testFolder, "report.json");
    String expectedReportString =
        TestUtil.getStringFromResource(this.getClass(), "LintTest_McfAndTmcfReport.json");
    TestUtil.assertReportFilesAreSimilar(expectedReportString, actualReportString);
  }

  @Test
  public void LintTest_AllThreeFiles() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("Tmcf1.tmcf");
    String mcf = resourceFile("LintTest.mcf");
    String csv = resourceFile("Csv1.csv");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("lint", mcf, tmcf, csv, "--output-dir=" + testFolder.getRoot().getPath());
    String actualReportString = TestUtil.getStringFromTestFile(testFolder, "report.json");
    String expectedReportString =
        TestUtil.getStringFromResource(this.getClass(), "LintTest_AllThreeFilesReport.json");
    TestUtil.assertReportFilesAreSimilar(expectedReportString, actualReportString);
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
