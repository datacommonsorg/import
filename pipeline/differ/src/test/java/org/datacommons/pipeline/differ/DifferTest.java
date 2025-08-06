package org.datacommons.pipeline.differ;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.pipeline.util.PipelineUtils;
import org.datacommons.proto.Mcf.McfGraph;
import org.junit.Rule;
import org.junit.Test;

public class DifferTest {

  PipelineOptions options = PipelineOptionsFactory.create();
  @Rule public TestPipeline p = TestPipeline.fromOptions(options);

  @Test
  public void testDiffer() {
    options.setStableUniqueNames(PipelineOptions.CheckEnabled.OFF);

    // Create an input PCollection.
    String currentFile = getClass().getClassLoader().getResource("current.mcf").getPath();
    String previousFile = getClass().getClassLoader().getResource("previous.mcf").getPath();

    // Process the input.
    PCollection<McfGraph> currentGraph = PipelineUtils.readMcfFile(currentFile, p);
    PCollection<KV<String, String>> currentNodes = DifferUtils.processGraph(currentGraph);
    PCollection<McfGraph> previousGraph = PipelineUtils.readMcfFile(previousFile, p);
    PCollection<KV<String, String>> previousNodes = DifferUtils.processGraph(previousGraph);
    PCollection<String> result = DifferUtils.performDiff(currentNodes, previousNodes);

    // Assert on the results.
    PAssert.that(result)
        .containsInAnyOrder(
            "dcid:Mean_Concentration_AirPollutant_CO,dcid:cpcpAq/Secretariat_Amaravati___APPCB,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,,41.0,DELETED",
            "dcid:Min_Concentration_AirPollutant_Ozone,dcid:cpcpAq/Secretariat_Amaravati___IMD,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,,18.0,DELETED",
            "dcid:Mean_Concentration_AirPollutant_CO,dcid:cpcpAq/Secretariat_Amaravati___IMD,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,42.0,41.0,MODIFIED",
            "dcid:Min_Concentration_AirPollutant_Ozone,dcid:cpcpAq/Secretariat_Amaravati___APPCB,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,,18.0,DELETED",
            "dcid:Max_Concentration_AirPollutant_Ozone,dcid:cpcpAq/Secretariat_Amaravati___APPCB,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,53.0,,ADDED",
            "dcid:Mean_Concentration_AirPollutant_CO,dcid:cpcpAq/Secretariat_Amaravati___APPCB,\"2024-09-25T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,,40.0,DELETED",
            "dcid:Mean_Concentration_AirPollutant_Ozone,dcid:cpcpAq/Secretariat_Amaravati___APPCB,\"2024-09-24T12:00:00\",,,dcid:MicrogramsPerCubicMeter,,28.0,29.0,MODIFIED");

    // Run the pipeline.
    p.run();
  }
}
