package org.datacommons.tool;


import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import picocli.CommandLine;

public class LintTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void LintTest_McfAndTmcf() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("TmcfWithErrors.tmcf");
    String mcf = resourceFile("LintTest.mcf");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("lint", mcf, tmcf, "--output-dir=" + testFolder.getRoot().getPath());
    File actualReportFile =
        new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    File expectedReportFile =
        new File(this.getClass().getResource("LintTest_McfAndTmcfReport.json").getPath());
    TestUtil.assertReportFilesAreSimilar(expectedReportFile, actualReportFile);
  }

  @Test
  public void LintTest_AllThreeFiles() throws IOException {
    Main app = new Main();
    String tmcf = resourceFile("Tmcf1.tmcf");
    String mcf = resourceFile("LintTest.mcf");
    String csv = resourceFile("Csv1.csv");
    CommandLine cmd = new CommandLine(app);
    cmd.execute("lint", mcf, tmcf, csv, "--output-dir=" + testFolder.getRoot().getPath());
    File actualReportFile =
        new File(Paths.get(testFolder.getRoot().getPath(), "report.json").toString());
    File expectedReportFile =
        new File(this.getClass().getResource("LintTest_AllThreeFilesReport.json").getPath());
    TestUtil.assertReportFilesAreSimilar(expectedReportFile, actualReportFile);
  }

  private String resourceFile(String resource) {
    return this.getClass().getResource(resource).getPath();
  }
}
