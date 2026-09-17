package org.datacommons.ingestion.differ;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.Test;

public class DifferTest {

  PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void testDiffer() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);
    options.setRunner(DirectRunner.class);
    DifferOptions differOptions = options.as(DifferOptions.class);

    // Create an input PCollection.
    String currentFile = getClass().getClassLoader().getResource("current").getPath();
    String previousFile = getClass().getClassLoader().getResource("previous").getPath();

    String tmpDir = "/tmp/differ_test_output_" + System.currentTimeMillis();
    new File(tmpDir).mkdirs();

    differOptions.setCurrentData(Paths.get(currentFile, "*.mcf").toString());
    differOptions.setPreviousData(Paths.get(previousFile, "*.mcf").toString());
    differOptions.setOutputLocation(tmpDir);
    differOptions.setUseOptimizedGraphFormat(false);

    DifferPipeline.buildPipeline(p, differOptions);

    p.run().waitUntilFinish();

    try {
      verifyUnifiedDiffOutput(tmpDir);
    } catch (IOException e) {
      fail("Failed to verify diff: " + e.getMessage());
    }
  }

  private void verifyUnifiedDiffOutput(String outputDir) throws IOException {
    File dir = new File(outputDir);
    File[] actualFiles =
        dir.listFiles((d, name) -> name.startsWith("diff") && name.endsWith(".mcf"));
    if (actualFiles == null || actualFiles.length == 0) {
      fail("No diff files found in " + outputDir);
    }

    List<String> actualNodesNormalized = new ArrayList<>();
    for (File f : actualFiles) {
      String content = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
      for (String node : content.split("\n\n")) {
        if (!node.trim().isEmpty()) {
          actualNodesNormalized.add(normalizeMcfNode(node.trim()));
        }
      }
    }

    List<String> expectedNodesNormalized = new ArrayList<>();
    java.net.URL expectedResource = getClass().getClassLoader().getResource("expected");
    if (expectedResource != null) {
      File expectedDir = new File(expectedResource.getPath());
      File[] expectedFiles =
          expectedDir.listFiles((d, name) -> name.startsWith("diff") && name.endsWith(".mcf"));
      if (expectedFiles != null) {
        for (File f : expectedFiles) {
          String content = FileUtils.readFileToString(f, StandardCharsets.UTF_8);
          for (String node : content.split("\n\n")) {
            if (!node.trim().isEmpty()) {
              expectedNodesNormalized.add(normalizeMcfNode(node.trim()));
            }
          }
        }
      }
    }

    Collections.sort(actualNodesNormalized);
    Collections.sort(expectedNodesNormalized);

    if (!actualNodesNormalized.equals(expectedNodesNormalized)) {
      int size = Math.min(actualNodesNormalized.size(), expectedNodesNormalized.size());
      for (int i = 0; i < size; i++) {
        if (!actualNodesNormalized.get(i).equals(expectedNodesNormalized.get(i))) {
          fail(
              "Mismatch at node "
                  + i
                  + ":\nEXPECTED:\n"
                  + expectedNodesNormalized.get(i)
                  + "\nACTUAL:\n"
                  + actualNodesNormalized.get(i));
        }
      }
      org.junit.Assert.assertEquals(
          "Node count mismatch", expectedNodesNormalized.size(), actualNodesNormalized.size());
    }
    org.junit.Assert.assertTrue(
        "Should generate unified diff nodes", actualNodesNormalized.size() > 0);
  }

  private String normalizeMcfNode(String node) {
    List<String> lines = new ArrayList<>();
    for (String line : node.split("\n")) {
      String l = line.trim();
      if (l.isEmpty()) continue;
      int colonIdx = l.indexOf(":");
      if (colonIdx != -1) {
        String prop = l.substring(0, colonIdx).trim();
        if (prop.equals("keyString")) continue;

        String val = l.substring(colonIdx + 1).trim();
        if (val.startsWith("dcid:")) val = val.substring(5);
        if (val.startsWith("\"") && val.endsWith("\"")) val = val.substring(1, val.length() - 1);
        try {
          double d = Double.parseDouble(val);
          val = String.valueOf(d);
        } catch (NumberFormatException e) {
          // Not a number, keep as is
        }
        lines.add(prop + ": " + val);
      } else {
        lines.add(l);
      }
    }
    Collections.sort(lines);
    return String.join("\n", lines);
  }
}
