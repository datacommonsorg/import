package org.datacommons.pipeline.differ;

import java.nio.file.Paths;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.datacommons.pipeline.util.GraphUtils;
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
    String currentFile = getClass().getClassLoader().getResource("current").getPath();
    String previousFile = getClass().getClassLoader().getResource("previous").getPath();

    // Process the input.
    PCollection<McfGraph> currentGraph =
        GraphUtils.readMcfFile(Paths.get(currentFile, "*.mcf").toString(), p);
    PCollection<McfGraph> previousGraph =
        GraphUtils.readMcfFile(Paths.get(previousFile, "*.mcf").toString(), p);
    PCollectionTuple currentNodesTuple = DifferUtils.processGraph(currentGraph);
    PCollectionTuple previousNodesTuple = DifferUtils.processGraph(previousGraph);

    PCollection<KV<String, String>> currentNodes =
        currentNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<KV<String, String>> previousNodes =
        previousNodesTuple.get(DifferUtils.OBSERVATION_NODES_TAG);
    PCollection<String> obsDiff = DifferUtils.performDiff(currentNodes, previousNodes);

    currentNodes = currentNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    previousNodes = previousNodesTuple.get(DifferUtils.SCHEMA_NODES_TAG);
    PCollection<String> schemaDiff = DifferUtils.performDiff(currentNodes, previousNodes);

    // // Assert on the results.
    PAssert.that(obsDiff)
        .containsInAnyOrder(
            "dcid:InterestRate_TreasuryBond_20Year,dcid:country/USA,\"2025-01-30\",,dcid:ConstantMaturityRate,dcid:Percent,,4.85,4.81,MODIFIED",
            "dcid:InterestRate_TreasuryNote_10Year,dcid:country/USA,\"2025-01-31\",,dcid:ConstantMaturityRate,dcid:Percent,,4.58,,ADDED",
            "dcid:InterestRate_TreasuryBill_3Month,dcid:country/USA,\"2025-01-30\",,dcid:ConstantMaturityRate,dcid:Percent,,,4.30,DELETED",
            "dcid:InterestRate_TreasuryBill_3Month,dcid:country/USA,\"2025-01-31\",,dcid:ConstantMaturityRate,dcid:Percent,,,4.31,DELETED");

    PAssert.that(schemaDiff)
        .containsInAnyOrder(
            "dcid:InterestRate_TreasuryNote_2Year,dcid:InterestRate_TreasuryNote_2Year,[2"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryNote_2Year\",dcs:TreasuryNote,dcs:measuredValue,dcs:StatisticalVariable,dcid:InterestRate_TreasuryNote_2Year,[2"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryNote_02Year\",dcs:TreasuryNote,dcs:measuredValue,dcs:StatisticalVariable,MODIFIED",
            "dcid:InterestRate_TreasuryBill_1Year,dcid:InterestRate_TreasuryBill_1Year,[1"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryBill_1Year\",dcs:TreasuryBill,dcs:measuredValue,dcs:StatisticalVariable,dcid:InterestRate_TreasuryBill_1Year,[1"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryBill_01Year\",dcs:TreasuryBill,dcs:measuredValue,dcs:StatisticalVariable,MODIFIED",
            "dcid:InterestRate_TreasuryBill_3Month,,dcid:InterestRate_TreasuryBill_3Month,[3"
                + " Month],dcs:interestRate,\"InterestRate_TreasuryBill_3Month\",dcs:TreasuryBill,dcs:measuredValue,dcs:StatisticalVariable,DELETED",
            "dcid:InterestRate_TreasuryBill_1Month,,dcid:InterestRate_TreasuryBill_1Month,[1"
                + " Month],dcs:interestRate,\"InterestRate_TreasuryBill_1Month\",dcs:TreasuryBill,dcs:measuredValue,dcs:StatisticalVariable,DELETED",
            "dcid:InterestRate_TreasuryNote_7Year,dcid:InterestRate_TreasuryNote_7Year,[7"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryNote_7Year\",dcs:TreasuryNote,dcs:measuredValue,dcs:StatisticalVariable,,ADDED",
            "dcid:InterestRate_TreasuryNote_5Year,dcid:InterestRate_TreasuryNote_5Year,[5"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryNote_5Year\",dcs:TreasuryNote,dcs:measuredValue,dcs:StatisticalVariable,,ADDED",
            "dcid:InterestRate_TreasuryNote_3Year,dcid:InterestRate_TreasuryNote_3Year,[3"
                + " Year],dcs:interestRate,\"InterestRate_TreasuryNote_3Year\",dcs:TreasuryNote,dcs:measuredValue,dcs:StatisticalVariable,,ADDED");

    // Run the pipeline.
    p.run();
  }
}
