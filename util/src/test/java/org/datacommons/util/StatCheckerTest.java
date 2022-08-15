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

package org.datacommons.util;

import static org.junit.Assert.*;

import com.google.common.truth.Expect;
import com.google.common.truth.extensions.proto.ProtoTruth;
import com.google.protobuf.util.JsonFormat;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.commons.io.FileUtils;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Debug.DataPoint;
import org.datacommons.proto.Debug.DataPoint.DataValue;
import org.datacommons.proto.Debug.StatValidationResult;
import org.datacommons.proto.Debug.StatValidationResult.StatValidationEntry;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StatCheckerTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();
  @Rule public final Expect expect = Expect.create();

  @Test
  public void testEndToEnd() throws IOException, InterruptedException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    String mcfPath = this.getClass().getResource("StatCheckerTest.mcf").getPath();
    Mcf.McfGraph graph = McfParser.parseInstanceMcfFile(mcfPath, false, lw);
    // create stat checker, extract series from the graph and check the series from the graph
    StatChecker sc = new StatChecker(lw, null);
    sc.extractStatsFromGraph(graph);
    sc.check();
    // check statsCheckSummary in logCtx is as expected
    File expectedReport =
        new File(this.getClass().getResource("StatCheckerTestReport.json").getPath());
    String expectedReportStr = FileUtils.readFileToString(expectedReport, StandardCharsets.UTF_8);
    Debug.Log.Builder expectedLog = Debug.Log.newBuilder();
    // System.out.println(logCtx.getStatsCheckSummaryList().get(0).toString());
    JsonFormat.parser().merge(expectedReportStr, expectedLog);
    expect
        .about(ProtoTruth.protos())
        .that(logCtx.getStatsCheckSummaryList())
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(expectedLog.getStatsCheckSummaryList());
  }

  @Test
  public void testCheckSvObsInGraph() throws IOException {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatChecker sc = new StatChecker(lw, null);

    // Graph with no duplicate StatVarObs.
    String mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n"
            + "\n"
            + "Node: SFWomenIncome2021\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2021\"\n"
            + "\n"
            + "Node: SFWomenIncome2022\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2022\"\n";
    Mcf.McfGraph graph = McfParser.parseInstanceMcfString(mcf, false, lw);
    assertTrue(sc.checkSvObsInGraph(graph));

    // check node with duplicate StatVarObs but same value.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n";
    graph = McfParser.parseInstanceMcfString(mcf, false, lw);
    assertTrue(sc.checkSvObsInGraph(graph));

    // check node with a duplicate StatVarObs but different value.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000001.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n"
            + "\n";
    graph = McfParser.parseInstanceMcfString(mcf, false, lw);
    assertFalse(sc.checkSvObsInGraph(graph));
    assertTrue(
        TestUtil.checkLog(
            lw.getLog(),
            "Sanity_InconsistentSvObsValues",
            "Found nodes with different values for the same StatVarObservation :: observationAbout: 'geoId/SF', variableMeasured: 'WomenIncome', observationDate: '2020', value1: 1.0E7, value2: 1.0000001E7"));

    // check node that differs only in one property from an existing StatVarObservation node.
    mcf =
        "Node: SFWomenIncome2020\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcid:WomenIncome\n"
            + "value: 10000000.0\n"
            + "observationAbout: dcid:geoId/SF\n"
            + "observationDate: \"2020\"\n"
            + "unit: \"cubicMeters\"\n"
            + "\n";
    graph = McfParser.parseInstanceMcfString(mcf, false, lw);
    assertTrue(sc.checkSvObsInGraph(graph));
  }

  @Test
  public void testFuncCheckValueInconsistencies() {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    StatChecker.checkSeriesValueInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2011", 24.0);
    StatChecker.checkSeriesValueInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2012", 24.0);
    addDataPoint(timeSeries, "2013", 240.3);
    StatChecker.checkSeriesValueInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    addDataPoint(timeSeries, "2013", -20.3);
    StatChecker.checkSeriesValueInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Values", List.of("2013")));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Inconsistent_Values"));
  }

  @Test
  public void testFuncCheckTypeInconsistencies() {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();
    String counterKey = "StatsCheck_MultipleValueTypesInASeries";

    // Empty list does not fail
    StatChecker.checkSeriesTypeInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, counterKey, new ArrayList<>()));

    // List of one does not fail
    resBuilder.clear();
    addDataPoint(timeSeries, "2011", 24.0);
    StatChecker.checkSeriesTypeInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, counterKey, new ArrayList<>()));

    // List of two numbers does not fail, but ...
    resBuilder.clear();
    addDataPoint(timeSeries, "2012", 24.0);
    StatChecker.checkSeriesTypeInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, counterKey, new ArrayList<>()));

    // ... adding a String/Reference to that list of two numbers triggers log
    addDataPoint(timeSeries, "2014", "DataSuppressed");
    StatChecker.checkSeriesTypeInconsistencies(
        new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(checkHasCounter(resBuilder, counterKey, List.of("2011", "2014")));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), counterKey));
  }

  @Test
  public void testFuncCheckDates() {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    // Good example, year.
    addDataPoint(timeSeries, "2017", 1.0);
    addDataPoint(timeSeries, "2011", 1.0);
    addDataPoint(timeSeries, "2012", 1.0);
    addDataPoint(timeSeries, "2013", 1.0);
    addDataPoint(timeSeries, "2015", 1.0);
    addDataPoint(timeSeries, "2014", 1.0);
    addDataPoint(timeSeries, "2016", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));

    // Good example, year + month.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2011-09", 1.0);
    addDataPoint(timeSeries, "2012-03", 1.0);
    addDataPoint(timeSeries, "2011-12", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));

    // Data hole, year.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2017", 1.0);
    addDataPoint(timeSeries, "2013", 1.0);
    addDataPoint(timeSeries, "2011", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Data_Holes"));

    // Data hole, year + month.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2017-03", 1.0);
    addDataPoint(timeSeries, "2017-06", 1.0);
    addDataPoint(timeSeries, "2017-12", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));
    assertEquals(2, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Data_Holes"));

    // Inconsistent granularity.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2012", 1.0);
    addDataPoint(timeSeries, "2015-01", 1.0);
    addDataPoint(timeSeries, "2018", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertTrue(
        checkHasCounter(
            resBuilder, "StatsCheck_Inconsistent_Date_Granularity", List.of("2015-01")));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));
    assertEquals(
        1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Inconsistent_Date_Granularity"));
    assertEquals(2, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Data_Holes"));

    // Invalid date.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2012-01", 1.0);
    addDataPoint(timeSeries, "2012:02", 1.0);
    addDataPoint(timeSeries, "2012-03", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", List.of("2012:02")));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Invalid_Date"));
    assertEquals(
        1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Inconsistent_Date_Granularity"));
    assertEquals(2, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_Data_Holes"));
  }

  @Test
  public void testFuncCheckPercent() {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", new ArrayList<>()));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "1993", -2.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", new ArrayList<>()));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "1992", -1.25);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", new ArrayList<>()));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2001", -8.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", List.of("1993", "2001")));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));
    assertEquals(-300.00, resBuilder.getValidationCounters(0).getPercentDifference(), 0.0);
    assertEquals(
        1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_MaxPercentFluctuationGreaterThan100"));

    resBuilder.clear();
    addDataPoint(timeSeries, "2002", 0.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", List.of("1993", "2001")));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));
    assertEquals(-300.00, resBuilder.getValidationCounters(0).getPercentDifference(), 0.0);
    assertEquals(
        2, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_MaxPercentFluctuationGreaterThan100"));

    resBuilder.clear();
    addDataPoint(timeSeries, "2003", 1.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", new ArrayList<>()));
    assertTrue(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", List.of("2002", "2003")));
    assertEquals(1.0E8, resBuilder.getValidationCounters(0).getPercentDifference(), 0);
    assertEquals(
        2, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_MaxPercentFluctuationGreaterThan100"));
    assertEquals(
        1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_MaxPercentFluctuationGreaterThan500"));

    // Check that % deltas are not flagged when sawtooth exists.
    resBuilder.clear();
    log.clear();
    addDataPoint(timeSeries, "2003", -2.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan100", new ArrayList<>()));
    assertFalse(
        checkHasCounter(
            resBuilder, "StatsCheck_MaxPercentFluctuationGreaterThan500", new ArrayList<>()));
  }

  @Test
  public void testFuncCheckSigma() {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp/statCheckerTest"));
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    addDataPoint(timeSeries, "2010", 5.6);
    addDataPoint(timeSeries, "2011", 5.6);
    addDataPoint(timeSeries, "2012", 5.6);
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_3_Sigma", new ArrayList<>()));

    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2010", 50.0);
    addDataPoint(timeSeries, "2011", 10.0);
    addDataPoint(timeSeries, "2012", 50.0);
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_3_Sigma", new ArrayList<>()));

    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2010", 5.0);
    addDataPoint(timeSeries, "2011", 5.0);
    addDataPoint(timeSeries, "2012", 4.0);
    addDataPoint(timeSeries, "2013", 5.0);
    addDataPoint(timeSeries, "2014", 5.0);
    addDataPoint(timeSeries, "2015", 5.0);
    addDataPoint(timeSeries, "2016", 5.0);
    addDataPoint(timeSeries, "2017", 5.0);
    addDataPoint(timeSeries, "2018", 5.0);
    addDataPoint(timeSeries, "2019", 5.0);
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder, logCtx);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_3_Sigma", List.of("2012")));
    assertEquals(1, TestUtil.getCounter(logCtx.getLog(), "StatsCheck_3_Sigma"));
  }

  @Test
  public void testShouldExtractSeriesInfo() {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    StatChecker sc = new StatChecker(lw, null);

    // For type-inferred namespaces
    List<String> typeInferredNamespaces = List.of("geoId/", "nuts/");
    for (String namespace : typeInferredNamespaces) {
      // First 5 places of the same length should work
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "01"));
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "02"));
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "03"));
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "04"));
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "05"));

      // New places beyond the first 5 should not work
      assertFalse(sc.shouldExtractSeriesInfo(namespace + "06"));

      // Adding a different length should work
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "01000000"));

      // Querying for a place that already exists should return true
      assertTrue(sc.shouldExtractSeriesInfo(namespace + "05"));
    }

    // For typeless namespaces
    String typelessNamespace = "typeless/";
    String filler = "0";
    // First 25 places should work, even if different lengths
    for (int i = 0; i < 25; i++) {
      // typeless/0, typeless/01, typeless/002, typeless/0003, etc.
      assertTrue(
          sc.shouldExtractSeriesInfo(typelessNamespace + filler.repeat(i) + String.valueOf(i)));
    }
    // Adds beyond the first 25 should not work
    assertFalse(sc.shouldExtractSeriesInfo(typelessNamespace + "06"));

    // Querying for a place that already exists should return true
    assertTrue(sc.shouldExtractSeriesInfo(typelessNamespace + "002"));
  }

  private boolean checkHasCounter(
      StatValidationResult.Builder resBuilder, String counterName, List<String> problemPointDates) {
    boolean counterFound = false;
    for (StatValidationEntry counter : resBuilder.getValidationCountersList()) {
      if (counter.getCounterKey().equals(counterName)) {
        assertEquals(
            "Incorrect number of problem points for counter: " + counterName,
            problemPointDates.size(),
            counter.getProblemPointsCount());
        List<String> unexpectedDates = new ArrayList<>();
        for (DataPoint problemPoint : counter.getProblemPointsList()) {
          if (!problemPointDates.contains(problemPoint.getDate())) {
            unexpectedDates.add(problemPoint.getDate());
          }
        }
        assertTrue(
            "Problem points with unexpected dates found: " + String.join(",", unexpectedDates),
            unexpectedDates.isEmpty());
        counterFound = true;
      }
    }
    return counterFound;
  }

  private void addDataPoint(Map<String, DataPoint> timeSeries, String date, String val) {
    DataPoint.Builder dp = DataPoint.newBuilder().setDate(date);
    if (timeSeries.containsKey(date)) {
      dp = timeSeries.get(date).toBuilder();
    }
    TypedValue typedValue =
        McfParser.parseTypedValue(McfType.INSTANCE_MCF, false, Vocabulary.VALUE, val, null).build();
    DataValue dataVal = DataValue.newBuilder().setValue(typedValue).build();
    dp.addValues(dataVal);
    timeSeries.put(date, dp.build());
  }

  private void addDataPoint(Map<String, DataPoint> timeSeries, String date, double val) {
    addDataPoint(timeSeries, date, Double.toString(val));
  }
}
