package org.datacommons.util;

import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.datacommons.proto.Debug.DataPoint;
import org.datacommons.proto.Debug.DataPoint.DataValue;
import org.datacommons.proto.Debug.StatValidationResult;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;

// An object to hold all the series information for all the statistical variables for a place.
public class PlaceSeriesSummary {
  public static class SeriesSummary {
    StatValidationResult.Builder validationResult;
    // Key is date of each datapoint. Use treemap here to keep the dates sorted.
    TreeMap<String, DataPoint> timeSeries;
  }

  // Key in svSeriesSummaryMap is stat var dcid & key in the Map<Long, SeriesSummary> is a hash
  // constructed using place dcid, stat var dcid, measurement method, observation period,
  // scaling factor, and unit of the stat var observations of the series summary
  private final Map<String, Map<Long, SeriesSummary>> svSeriesSummaryMap = new HashMap<>();

  // Given a statVarObservation node, extract time series info and save it into svSeriesSummaryMap.
  public synchronized void extractSeriesFromNode(McfGraph.PropertyValues node) {
    StatValidationResult.Builder vres = StatValidationResult.newBuilder();
    // Add information about the node to StatValidationResult
    vres.setPlaceDcid(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_ABOUT));
    vres.setStatVarDcid(McfUtil.getPropVal(node, Vocabulary.VARIABLE_MEASURED));
    vres.setMeasurementMethod(McfUtil.getPropVal(node, Vocabulary.MEASUREMENT_METHOD));
    vres.setObservationPeriod(McfUtil.getPropVal(node, Vocabulary.OBSERVATION_PERIOD));
    vres.setScalingFactor(McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR));
    vres.setUnit(McfUtil.getPropVal(node, Vocabulary.SCALING_FACTOR));

    // Get the series summary for this node. If the series summary for this node is not already
    // in the seriesSummaryMap, add it to the seriesSummaryMap.
    Hasher hasher = Hashing.farmHashFingerprint64().newHasher();
    hasher.putString(vres.toString(), StandardCharsets.UTF_8);
    Long hash = hasher.hash().asLong();
    svSeriesSummaryMap.computeIfAbsent(vres.getStatVarDcid(), k -> new HashMap<>());
    Map<Long, SeriesSummary> seriesSummaryMap = svSeriesSummaryMap.get(vres.getStatVarDcid());
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
    svSeriesSummaryMap.put(vres.getStatVarDcid(), seriesSummaryMap);
  }

  public Map<String, Map<Long, SeriesSummary>> getSvSeriesSummaryMap() {
    return svSeriesSummaryMap;
  }

  // Generate a map of stat var id to StatVarSummary for that stat var from the stats information
  // saved to this object. Used by SummaryReport.ftl
  public Map<String, StatVarSummary> getStatVarSummaryMap() {
    Map<String, StatVarSummary> statVarSummaryMap = new HashMap<>();
    for (Map.Entry<String, Map<Long, SeriesSummary>> svSeriesSummary :
        svSeriesSummaryMap.entrySet()) {
      StatVarSummary summary = new StatVarSummary();
      List<DataPoint> seriesDataPoints = new ArrayList<>();
      for (SeriesSummary seriesSummary : svSeriesSummary.getValue().values()) {
        summary.numObservations += seriesSummary.timeSeries.size();
        if (!seriesSummary.validationResult.getMeasurementMethod().isEmpty()) {
          summary.mMethods.add(seriesSummary.validationResult.getMeasurementMethod());
        }
        if (!seriesSummary.validationResult.getUnit().isEmpty()) {
          summary.mMethods.add(seriesSummary.validationResult.getUnit());
        }
        if (!seriesSummary.validationResult.getScalingFactor().isEmpty()) {
          summary.mMethods.add(seriesSummary.validationResult.getScalingFactor());
        }
        seriesDataPoints.addAll(seriesSummary.timeSeries.values());
      }
      seriesDataPoints.sort(Comparator.comparing(DataPoint::getDate));
      seriesDataPoints.forEach(
          dp -> {
            if (!dp.getValuesList().isEmpty()) {
              summary.seriesDates.add(dp.getDate());
              summary.seriesValues.add(dp.getValues(0).getValue());
            }
          });
      statVarSummaryMap.put(svSeriesSummary.getKey(), summary);
    }
    return statVarSummaryMap;
  }
}
