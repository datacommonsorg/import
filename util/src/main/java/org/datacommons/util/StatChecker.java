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

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import org.datacommons.proto.Debug.DataPoint;
import org.datacommons.proto.Debug.DataPoint.DataValue;
import org.datacommons.proto.Debug.Log.Level;
import org.datacommons.proto.Debug.StatValidationResult;
import org.datacommons.proto.Debug.StatValidationResult.StatValidationEntry;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.ValueType;

// A checker that checks time-series for holes, variance in values, etc.
// This class is thread-safe.
public class StatChecker {
  private static final class SeriesSummary {
    StatValidationResult.Builder validationResult;
    Map<String, DataPoint> timeSeries;
  }

  private static final int MAX_NUM_PLACE_PER_TYPE = 5;
  private static final double SMALL_NUMBER = 0.000001;
  private static final String RECEIVED_SAMPLE_PLACES_KEY = "Received_Sample_Places";
  private static final Character PLACE_NAMESPACE_DELIMITER = '/';
  private final boolean verbose;
  private final LogWrapper logCtx;
  // key is a string made up of place dcid, stat var dcid, measurement method, observation period,
  // scaling factor, and unit of the stat var observations of the series summary.
  private final Map<String, SeriesSummary> seriesSummaryMap;
  // key is place namespace + length of place dcid, value is set of place dcids
  private final Map<String, Set<String>> samplePlaces;
  private final boolean shouldGenerateSamplePlaces;
  // Tracks global state on StatVarObservations to detect whether there are multiple of the
  // same StatVarObservation with inconsistent values. The key is a hash made up of a set of
  // properties that distinguish a StatVarObservation and the value is the first value seen of that
  // StatVarObservation.
  private final Map<Long, Float> svObValues;
  private final String EMPTY_PROP_STRING = "EMPTY_PROP";

  // Creates a StatChecker instance. If no samplePlaces are provided, for each pair of (place
  // namespace, place dcid length), we will use the first 5 places that stat var observations are
  // added for as the sample places.
  public StatChecker(LogWrapper logCtx, Set<String> samplePlaces, boolean verbose) {
    this.logCtx = logCtx;
    this.verbose = verbose;
    this.seriesSummaryMap = new HashMap<>();
    this.samplePlaces = new HashMap<>();
    this.svObValues = new HashMap<>();
    if (samplePlaces == null) {
      this.shouldGenerateSamplePlaces = true;
    } else {
      this.shouldGenerateSamplePlaces = false;
      this.samplePlaces.put(RECEIVED_SAMPLE_PLACES_KEY, samplePlaces);
    }
  }

  // Given a graph, extract time series info (about the chosen sample places) from the
  // statVarObservation nodes and save it into seriesSummaryMap.
  public synchronized void extractSeriesInfoFromGraph(McfGraph graph) {
    for (Map.Entry<String, McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      if (isSvObWithNumberValue(node.getValue()) && shouldExtractSeriesInfo(node.getValue())) {
        extractSeriesInfoFromNode(node.getValue());
      }
    }
  }

  // Given a graph, for each node that is a statVarObservation node with a number value, check for
  // any value inconsistencies. Return false if there are any svObsValueInconsistencies found.
  public synchronized boolean checkSvObsInGraph(McfGraph graph) {
    boolean success = true;
    for (Map.Entry<String, McfGraph.PropertyValues> node : graph.getNodesMap().entrySet()) {
      if (isSvObWithNumberValue(node.getValue())) {
        success &= checkSvObsValueInconsistency(node.getValue());
      }
    }
    return success;
  }

  // Iterate through seriesSummaries, perform a list of checks (value inconsistencies, sigma
  // variance, percent fluctuations, holes in dates, invalid dates, etc) and add these results to
  // the logCtx.
  public synchronized void check() {
    for (String hash : seriesSummaryMap.keySet()) {
      List<DataPoint> timeSeries = new ArrayList<>(seriesSummaryMap.get(hash).timeSeries.values());
      StatValidationResult.Builder resBuilder = seriesSummaryMap.get(hash).validationResult;
      // Check inconsistent values (sawtooth).
      checkSeriesValueInconsistencies(timeSeries, resBuilder, logCtx);
      // Check N-Sigma variance.
      checkSigmaDivergence(timeSeries, resBuilder, logCtx);
      // Check N-Percent fluctuations.
      checkPercentFluctuations(timeSeries, resBuilder, logCtx);
      // Check for holes in dates, invalid dates, etc.
      checkDates(timeSeries, resBuilder, logCtx);
      // add result to log.
      if (!resBuilder.getValidationCountersList().isEmpty()) {
        logCtx.addStatsCheckSummaryEntry(resBuilder.build());
      }
    }
  }

  // Return whether the node is a statVarObservation node with value of type number.
  private boolean isSvObWithNumberValue(McfGraph.PropertyValues node) {
    List<String> types = McfUtil.getPropVals(node, Vocabulary.TYPE_OF);
    McfGraph.Values nodeValues =
        node.getPvsOrDefault(Vocabulary.VALUE, McfGraph.Values.getDefaultInstance());
    return types.contains(Vocabulary.STAT_VAR_OBSERVATION_TYPE)
        && nodeValues.getTypedValuesCount() != 0
        && nodeValues.getTypedValues(0).getType() == ValueType.NUMBER;
  }

  // Only extract series information for a statVarObservation node that is about a sample place.
  private boolean shouldExtractSeriesInfo(McfGraph.PropertyValues node) {
    String placeDcid = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT);
    if (placeDcid.isEmpty()) return false;
    if (shouldGenerateSamplePlaces) {
      int nameSpaceSplit = placeDcid.indexOf(PLACE_NAMESPACE_DELIMITER);
      String placeNameSpace = "";
      if (nameSpaceSplit >= 0) {
        placeNameSpace = placeDcid.substring(0, nameSpaceSplit);
      }
      String placekey = placeNameSpace + placeDcid.length();
      // If this is a new place type (determined by place dcid namespace and length) and/or the
      // place type hasn't hit the maximum number of sample places, add place to sample places map.
      samplePlaces.computeIfAbsent(placekey, k -> new HashSet<>());
      if (samplePlaces.get(placekey).size() >= MAX_NUM_PLACE_PER_TYPE
          && !samplePlaces.get(placekey).contains(placeDcid)) {
        return false;
      }
      samplePlaces.get(placekey).add(placeDcid);
    } else {
      return samplePlaces.get(RECEIVED_SAMPLE_PLACES_KEY).contains(placeDcid);
    }
    return true;
  }

  // Given a statVarObservation node, extract time series info (if it is about the chosen sample
  // places) and save it into seriesSummaryMap.
  private void extractSeriesInfoFromNode(McfGraph.PropertyValues node) {
    StatValidationResult.Builder vres = StatValidationResult.newBuilder();
    // Add information about the node to StatValidationResult
    vres.setPlaceDcid(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT));
    vres.setStatVarDcid(McfUtil.getPropVal(node, Vocabulary.VARIABLE_MEASURED));
    vres.setMeasurementMethod(McfUtil.getPropVal(node, Vocabulary.MEASUREMENT_METHOD));
    vres.setObservationPeriod(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_PERIOD));
    vres.setScalingFactor(McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR));
    vres.setUnit(McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR));

    // Get the series summary for this node. If the series summary for this node is not already in
    // in the seriesSummaryMap, add it to the seriesSummaryMap.
    String hash = vres.toString();
    SeriesSummary summary;
    if (seriesSummaryMap.containsKey(hash)) {
      summary = seriesSummaryMap.get(hash);
    } else {
      summary = new SeriesSummary();
      summary.validationResult = vres;
      summary.timeSeries = new TreeMap<>();
    }

    // Add the value of this StatVarObservation node to the timeseries of this node's SeriesSummary.
    String obsDate = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_DATE);
    String value = McfUtil.getPropVal(node, Vocabulary.VALUE);
    DataValue dataVal =
        DataValue.newBuilder()
            .setValue(Double.parseDouble(value))
            .addAllLocations(node.getLocationsList())
            .build();
    DataPoint.Builder dataPoint = DataPoint.newBuilder().setDate(obsDate);
    if (summary.timeSeries.containsKey(obsDate)) {
      dataPoint = summary.timeSeries.get(obsDate).toBuilder();
    }
    dataPoint.addValues(dataVal);
    summary.timeSeries.put(obsDate, dataPoint.build());
    seriesSummaryMap.put(hash, summary);
  }

  protected static void checkSeriesValueInconsistencies(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    StatValidationEntry.Builder inconsistentValueCounter = StatValidationEntry.newBuilder();
    String counterKey = "StatsCheck_Inconsistent_Values";
    inconsistentValueCounter.setCounterKey(counterKey);
    for (DataPoint dp : timeSeries) {
      double v = 0;
      boolean vInitialized = false;
      for (DataValue val : dp.getValuesList()) {
        if (vInitialized && val.getValue() != v) {
          inconsistentValueCounter.addProblemPoints(dp);
          logCtx.incrementWarningCounterBy(counterKey, 1);
        }
        vInitialized = true;
        v = val.getValue();
      }
    }
    if (!inconsistentValueCounter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(inconsistentValueCounter.build());
    }
  }

  protected static void checkSigmaDivergence(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    MeanAndStdDev meanAndStdDev = getStats(timeSeries);
    if (meanAndStdDev.stdDev == 0) {
      return;
    }
    String sigma3CounterKey = "StatsCheck_3_Sigma";
    StatValidationEntry.Builder sigma3Counter = StatValidationEntry.newBuilder();
    sigma3Counter.setCounterKey(sigma3CounterKey);
    // Only add data points to the counter of the greatest standard deviation that it belongs to.
    // ie. if the data point is beyond 3 std deviation, only add it to that counter.
    for (DataPoint dp : timeSeries) {
      double val = dp.getValues(0).getValue();
      if (Math.abs(val - meanAndStdDev.mean) > 3 * meanAndStdDev.stdDev) {
        sigma3Counter.addProblemPoints(dp);
        logCtx.incrementWarningCounterBy(sigma3CounterKey, 1);
      }
    }
    if (!sigma3Counter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(sigma3Counter.build());
    }
  }

  private static class MeanAndStdDev {
    double mean = 0;
    double stdDev = 0;
  }

  private static MeanAndStdDev getStats(List<DataPoint> timeSeries) {
    MeanAndStdDev result = new MeanAndStdDev();
    if (timeSeries.size() < 2) return result;
    double weights = 0;
    double sum = 0;
    double sumSqDev = 0;
    for (DataPoint dp : timeSeries) {
      double val = dp.getValues(0).getValue();
      if (weights > 0) {
        sumSqDev += 1 * weights / 1 / (weights + 1) * Math.pow((1 / weights * sum - val), 2);
      }
      weights++;
      sum += val;
    }
    if (weights > 0) {
      result.stdDev = Math.sqrt(sumSqDev * (1.0 / weights));
      result.mean = sum * (1.0 / weights);
    }
    return result;
  }

  // Goes through sorted (but possibly discontinuous) time series, saving the largest fluctuation
  // for each bucket of fluctuations >50, >100, and >500.
  protected static void checkPercentFluctuations(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    double maxDelta = 0;
    DataPoint maxDeltaDP = null;
    DataPoint maxDeltaBaseDP = null;
    DataPoint baseDataPoint = null;
    for (DataPoint dp : timeSeries) {
      // Don't try to compare between times because this is a Sawtooth
      if (dp.getValuesCount() > 1) return;
      if (dp.getValuesCount() == 0) continue;
      double currVal = dp.getValues(0).getValue();
      if (baseDataPoint != null) {
        double currDelta;
        double baseVal = baseDataPoint.getValues(0).getValue();
        if (baseVal == 0) {
          currDelta = (currVal) / SMALL_NUMBER;
        } else {
          currDelta = (currVal - baseVal) / Math.abs(baseVal);
        }
        if (Math.abs(maxDelta) < Math.abs(currDelta)) {
          maxDelta = currDelta;
          maxDeltaDP = dp;
          maxDeltaBaseDP = baseDataPoint;
        }
      }
      baseDataPoint = dp;
    }
    String counterKey = "";
    if (Math.abs(maxDelta) > 5) {
      counterKey = "StatsCheck_MaxPercentFluctuationGreaterThan500";
    } else if (Math.abs(maxDelta) > 1) {
      counterKey = "StatsCheck_MaxPercentFluctuationGreaterThan100";
    } else if (Math.abs(maxDelta) > 0.5) {
      counterKey = "StatsCheck_MaxPercentFluctuationGreaterThan50";
    }
    if (counterKey.isEmpty()) return;
    StatValidationEntry.Builder maxPercentFluctuationCounter = StatValidationEntry.newBuilder();
    maxPercentFluctuationCounter.setCounterKey(counterKey);
    maxPercentFluctuationCounter.addProblemPoints(maxDeltaBaseDP);
    maxPercentFluctuationCounter.addProblemPoints(maxDeltaDP);
    maxPercentFluctuationCounter.setPercentDifference(maxDelta);
    resBuilder.addValidationCounters(maxPercentFluctuationCounter.build());
    logCtx.incrementWarningCounterBy(counterKey, 1);
  }

  // Check if there are holes in the dates by inferring based on whether the successive dates have
  // equal duration. If the sizes of the date strings don't match or there are invalid dates, don't
  // try to infer holes and instead mark as "series_inconsistent_date_granularity" and
  // "series_invalid_date".
  protected static void checkDates(
      List<DataPoint> timeSeries, StatValidationResult.Builder resBuilder, LogWrapper logCtx) {
    Set<LocalDateTime> dateTimes = new TreeSet<>();
    StatValidationEntry.Builder invalidDateCounter = StatValidationEntry.newBuilder();
    String invalidDateCounterKey = "StatsCheck_Invalid_Date";
    invalidDateCounter.setCounterKey(invalidDateCounterKey);
    // To keep track of the different lengths of the date strings.
    Map<Integer, List<DataPoint>> dateLen = new HashMap<>();

    // In the first pass, get sorted dates in LocalDateTime form and check for invalid dates and
    // inconsistent date granularities.
    for (DataPoint dp : timeSeries) {
      String date = dp.getDate();
      LocalDateTime dateTime = StringUtil.getValidISO8601Date(date);
      if (dateTime == null) {
        invalidDateCounter.addProblemPoints(dp);
        logCtx.incrementWarningCounterBy(invalidDateCounterKey, 1);
        continue;
      }
      if (!dateLen.containsKey(date.length())) {
        dateLen.put(date.length(), new ArrayList<>());
      }
      dateLen.get(date.length()).add(dp);
      dateTimes.add(dateTime);
    }
    List<Integer> dateLenList = new ArrayList<>(dateLen.keySet());
    if (dateLenList.size() > 1) {
      StatValidationEntry.Builder inconsistentDateCounter = StatValidationEntry.newBuilder();
      String inconsistentDateCounterKey = "StatsCheck_Inconsistent_Date_Granularity";
      inconsistentDateCounter.setCounterKey(inconsistentDateCounterKey);
      // When there are multiple date granularity, the problem points will be those that are of a
      // different date granularity than the most common one.
      dateLenList.sort((d1, d2) -> dateLen.get(d2).size() - dateLen.get(d1).size());
      for (int i = 1; i < dateLenList.size(); i++) {
        dateLen.get(dateLenList.get(i)).forEach(inconsistentDateCounter::addProblemPoints);
      }
      // Increment counter for each series where there is an inconsistent date problem.
      logCtx.incrementWarningCounterBy(inconsistentDateCounterKey, 1);
      resBuilder.addValidationCounters(inconsistentDateCounter.build());
      return;
    }
    if (!invalidDateCounter.getProblemPointsList().isEmpty()) {
      resBuilder.addValidationCounters(invalidDateCounter.build());
      return;
    }
    long window = -1;
    LocalDateTime prev = null;
    List<LocalDateTime> dateTimesList = new ArrayList<>(dateTimes);

    // In this second pass, compute the data holes.
    // Date arithmetic is complicated by leap year considerations. For now assume
    // only month boundary.
    // TODO: Handle days granularity
    for (LocalDateTime dt : dateTimesList) {
      if (prev != null) {
        long delta = ChronoUnit.MONTHS.between(prev, dt);
        if (window >= 0 && window != delta) {
          StatValidationEntry.Builder dataHoleCounter = StatValidationEntry.newBuilder();
          String dataHoleCounterKey = "StatsCheck_Data_Holes";
          dataHoleCounter.setCounterKey(dataHoleCounterKey);
          dataHoleCounter.setAdditionalDetails(
              "Data hole found between the dates: " + prev.toString() + " and " + dt.toString());
          logCtx.incrementWarningCounterBy(dataHoleCounterKey, 1);
          resBuilder.addValidationCounters(dataHoleCounter.build());
          return;
        }
        window = delta;
      }
      prev = dt;
    }
  }

  // Return false if we have already seen the same StatVarObservation node but with a different
  // value.
  private boolean checkSvObsValueInconsistency(McfGraph.PropertyValues node) {
    String placeDcid = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT);
    String statVarDcid = McfUtil.getPropVal(node, Vocabulary.VARIABLE_MEASURED);
    String mmethod = McfUtil.getPropVal(node, Vocabulary.MEASUREMENT_METHOD);
    String obsPeriod = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_PERIOD);
    String sFactor = McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR);
    String unit = McfUtil.getPropVal(node, Vocabulary.UNIT);
    String obsDate = McfUtil.getPropVal(node, Vocabulary.OBSERVATION_DATE);
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    for (String prop :
        List.of(placeDcid, statVarDcid, mmethod, obsPeriod, sFactor, unit, obsDate)) {
      if (prop.isEmpty()) {
        hasher.putString(EMPTY_PROP_STRING, StandardCharsets.UTF_8);
      } else {
        hasher.putString(prop, StandardCharsets.UTF_8).putInt(prop.length());
      }
    }
    Long fp = hasher.hash().asLong();
    Float val = Float.parseFloat(McfUtil.getPropVal(node, Vocabulary.VALUE));
    if (this.svObValues.containsKey(fp) && !this.svObValues.get(fp).equals(val)) {
      logCtx.addEntry(
          Level.LEVEL_ERROR,
          "Sanity_InconsistentSvObsValues",
          "Found nodes with different values for the same StatVarObservation :: observationAbout: '"
              + placeDcid
              + "', variableMeasured: '"
              + statVarDcid
              + "', observationDate: '"
              + obsDate
              + "', value1: "
              + this.svObValues.get(fp)
              + ", value2: "
              + val,
          node.getLocationsList());
      return false;
    } else {
      this.svObValues.put(fp, val);
      return true;
    }
  }
}
