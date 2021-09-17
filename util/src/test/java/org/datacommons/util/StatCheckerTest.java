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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class StatCheckerTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();
  @Rule public final Expect expect = Expect.create();

  @Test
  public void testEndToEnd() throws IOException {
    Debug.Log.Builder logCtx = Debug.Log.newBuilder();
    LogWrapper lw = new LogWrapper(logCtx, testFolder.getRoot().toPath());
    lw.setLocationFile("StatCheckerTest.mcf");
    String mcfPath = this.getClass().getResource("StatCheckerTest.mcf").getPath();
    Mcf.McfGraph graph = McfParser.parseInstanceMcfFile(mcfPath, false, lw);
    // create stat checker, extract series from the graph and check the series from the graph
    StatChecker sc = new StatChecker(lw, null, false);
    sc.extractSeriesInfoFromGraph(graph);
    sc.check();
    // check statsCheckSummary in logCtx is as expected
    File expectedReport =
        new File(this.getClass().getResource("StatCheckerTestReport.json").getPath());
    String expectedReportStr = FileUtils.readFileToString(expectedReport, StandardCharsets.UTF_8);
    Debug.Log.Builder expectedLog = Debug.Log.newBuilder();
    JsonFormat.parser().merge(expectedReportStr, expectedLog);
    expect
        .about(ProtoTruth.protos())
        .that(logCtx.getStatsCheckSummaryList())
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(expectedLog.getStatsCheckSummaryList());
  }

  @Test
  public void testFuncCheckValueInconsistencies() {
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    StatChecker.checkValueInconsistencies(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2011", 24.0);
    StatChecker.checkValueInconsistencies(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2012", 24.0);
    addDataPoint(timeSeries, "2013", 240.3);
    StatChecker.checkValueInconsistencies(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Value", new ArrayList<>()));

    resBuilder.clear();
    addDataPoint(timeSeries, "2013", -20.3);
    StatChecker.checkValueInconsistencies(new ArrayList<>(timeSeries.values()), resBuilder);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Values", List.of("2013")));
  }

  @Test
  public void testFuncCheckDates() {
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
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
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
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
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
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));

    // Data hole, year + month.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2017-03", 1.0);
    addDataPoint(timeSeries, "2017-06", 1.0);
    addDataPoint(timeSeries, "2017-12", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));

    // Inconsistent granularity.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2012", 1.0);
    addDataPoint(timeSeries, "2015-01", 1.0);
    addDataPoint(timeSeries, "2018", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", new ArrayList<>()));
    assertTrue(
        checkHasCounter(
            resBuilder, "StatsCheck_Inconsistent_Date_Granularity", List.of("2015-01")));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));

    // Invalid date.
    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2012-01", 1.0);
    addDataPoint(timeSeries, "2012:02", 1.0);
    addDataPoint(timeSeries, "2012-03", 1.0);
    StatChecker.checkDates(new ArrayList<>(timeSeries.values()), resBuilder);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_Invalid_Date", List.of("2012:02")));
    assertFalse(
        checkHasCounter(resBuilder, "StatsCheck_Inconsistent_Date_Granularity", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_Data_Holes", new ArrayList<>()));
  }

  @Test
  public void testFuncCheckPercent() {
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertEquals(0.0, resBuilder.getSeriesLargestPercentDiff().getPercentDifference(), 0.0);
    assertFalse(resBuilder.getSeriesLargestPercentDiff().hasDiffDataPoint());
    assertFalse(resBuilder.getSeriesLargestPercentDiff().hasBaseDataPoint());

    resBuilder.clear();
    addDataPoint(timeSeries, "2001", -8.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertEquals(0.0, resBuilder.getSeriesLargestPercentDiff().getPercentDifference(), 0.0);
    assertFalse(resBuilder.getSeriesLargestPercentDiff().hasDiffDataPoint());
    assertFalse(resBuilder.getSeriesLargestPercentDiff().hasBaseDataPoint());

    resBuilder.clear();
    addDataPoint(timeSeries, "1993", -2.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertEquals(-3.0, resBuilder.getSeriesLargestPercentDiff().getPercentDifference(), 0.0);
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getDiffDataPoint().getDate(), "2001");
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getBaseDataPoint().getDate(), "1993");

    resBuilder.clear();
    addDataPoint(timeSeries, "2002", 0.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertEquals(-3.0, resBuilder.getSeriesLargestPercentDiff().getPercentDifference(), 0.0);
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getDiffDataPoint().getDate(), "2001");
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getBaseDataPoint().getDate(), "1993");

    resBuilder.clear();
    addDataPoint(timeSeries, "2003", 1.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertEquals(1000000, resBuilder.getSeriesLargestPercentDiff().getPercentDifference(), 0.0);
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getDiffDataPoint().getDate(), "2003");
    assertEquals(resBuilder.getSeriesLargestPercentDiff().getBaseDataPoint().getDate(), "2002");

    // Check that % deltas are not flagged when sawtooth exists.
    resBuilder.clear();
    addDataPoint(timeSeries, "2003", -2.0);
    StatChecker.checkPercentFluctuations(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(resBuilder.hasSeriesLargestPercentDiff());
  }

  @Test
  public void testFuncCheckSigma() {
    StatValidationResult.Builder resBuilder = StatValidationResult.newBuilder();
    Map<String, DataPoint> timeSeries = new TreeMap<>();

    addDataPoint(timeSeries, "2010", 5.6);
    addDataPoint(timeSeries, "2011", 5.6);
    addDataPoint(timeSeries, "2012", 5.6);
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_1_Sigma", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_2_Sigma", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_3_Sigma", new ArrayList<>()));

    resBuilder.clear();
    timeSeries.clear();
    addDataPoint(timeSeries, "2010", 50.0);
    addDataPoint(timeSeries, "2011", 10.0);
    addDataPoint(timeSeries, "2012", 50.0);
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder);
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_1_Sigma", List.of("2011")));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_2_Sigma", new ArrayList<>()));
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
    StatChecker.checkSigmaDivergence(new ArrayList<>(timeSeries.values()), resBuilder);
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_1_Sigma", new ArrayList<>()));
    assertFalse(checkHasCounter(resBuilder, "StatsCheck_2_Sigma", new ArrayList<>()));
    assertTrue(checkHasCounter(resBuilder, "StatsCheck_3_Sigma", List.of("2012")));
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

  private void addDataPoint(Map<String, DataPoint> timeSeries, String date, double val) {
    DataPoint.Builder dp = DataPoint.newBuilder().setDate(date);
    if (timeSeries.containsKey(date)) {
      dp = timeSeries.get(date).toBuilder();
    }
    DataValue dataVal = DataValue.newBuilder().setValue(val).build();
    dp.addValues(dataVal);
    timeSeries.put(date, dp.build());
  }
}
