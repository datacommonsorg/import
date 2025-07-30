package org.datacommons.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;
import org.junit.Test;

public class PlaceSeriesSummaryTest {
  @Test
  public void testGetStatVarSummaryMap() throws IOException {
    String mcfPath = this.getClass().getResource("PlaceSeriesSummaryTest.mcf").getPath();
    Mcf.McfGraph graph = McfParser.parseInstanceMcfFile(mcfPath, false, TestUtil.newLogCtx());
    PlaceSeriesSummary placeSeriesSummary = new PlaceSeriesSummary();
    for (Map.Entry<String, McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      placeSeriesSummary.extractSeriesFromNode(node.getValue());
    }
    StatVarSummary countPersonExpectedSummary = new StatVarSummary();
    countPersonExpectedSummary.numObservations = 6;
    countPersonExpectedSummary.mMethods = Set.of("CensusACS5YrSurvey");
    countPersonExpectedSummary.seriesDates =
        List.of("2014", "2014", "2015", "2018", "2019", "2020");
    countPersonExpectedSummary.seriesValues =
        List.of("1000100", "1000101", "1000400", "1000400", "1000400", "1000400");
    StatVarSummary countFemaleExpectedSummary = new StatVarSummary();
    countFemaleExpectedSummary.numObservations = 2;
    countFemaleExpectedSummary.mMethods = Set.of("CensusACS5YrSurvey");
    countFemaleExpectedSummary.seriesDates = List.of("2015", "2016");
    countFemaleExpectedSummary.seriesValues = List.of("500200", "500300");
    Map<String, StatVarSummary> expectedMap =
        Map.of(
            "Count_Person",
            countPersonExpectedSummary,
            "Count_Person_Female",
            countFemaleExpectedSummary);
    Map<String, StatVarSummary> actualMap = placeSeriesSummary.getStatVarSummaryMap();
    assertEquals(expectedMap.size(), actualMap.size());
    for (Map.Entry<String, StatVarSummary> expectedEntry : expectedMap.entrySet()) {
      assertTrue(actualMap.containsKey(expectedEntry.getKey()));
      if (!actualMap.containsKey(expectedEntry.getKey())) continue;
      StatVarSummary actualEntry = actualMap.get(expectedEntry.getKey());
      assertEquals(expectedEntry.getValue().numObservations, actualEntry.numObservations);
      assertEquals(expectedEntry.getValue().mMethods, actualEntry.mMethods);
      assertEquals(expectedEntry.getValue().units, actualEntry.units);
      assertEquals(expectedEntry.getValue().scalingFactors, actualEntry.scalingFactors);
      assertEquals(expectedEntry.getValue().dates, actualEntry.dates);
      assertEquals(expectedEntry.getValue().seriesDates, actualEntry.seriesDates);
      assertEquals(expectedEntry.getValue().seriesValues, actualEntry.seriesValues);
    }
  }

  @Test
  public void testGetTimeSeriesSVGChart_pre1900() throws IOException {
    String mcfPath = this.getClass().getResource("Pre1900PlaceSeriesSummaryTest.mcf").getPath();
    Mcf.McfGraph graph = McfParser.parseInstanceMcfFile(mcfPath, false, TestUtil.newLogCtx());
    PlaceSeriesSummary placeSeriesSummary = new PlaceSeriesSummary();
    for (Map.Entry<String, McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      placeSeriesSummary.extractSeriesFromNode(node.getValue());
    }
    Map<String, Map<Long, PlaceSeriesSummary.SeriesSummary>> svSeriesSummaryMap =
        placeSeriesSummary.getSvSeriesSummaryMap();
    assertFalse(svSeriesSummaryMap.isEmpty());
    for (Map<Long, PlaceSeriesSummary.SeriesSummary> seriesSummaryMap :
        svSeriesSummaryMap.values()) {
      for (PlaceSeriesSummary.SeriesSummary summary : seriesSummaryMap.values()) {
        assertEquals(
            "<b>Charts for years before 1900 are not supported yet</b>",
            summary.getTimeSeriesSVGChart());
      }
    }
  }
}
