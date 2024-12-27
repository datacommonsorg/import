package org.datacommons.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.TreeSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;

/*
 * This class generates a CSV summary report of stats related to a dataset import.
 */
class CSVReportWriter {

  enum ReportHeaders {
    StatVar,
    NumPlaces,
    NumObservations,
    MinValue,
    MaxValue,
    NumObservationsDates,
    MinDate,
    MaxDate,
    MeasurementMethods,
    Units,
    ScalingFactors,
    observationPeriods
  }

  public static void writeRecords(Map<String, StatVarSummary> records, Writer sw)
      throws IOException {
    CSVFormat csvFormat = CSVFormat.DEFAULT.withHeader(ReportHeaders.class);

    try (final CSVPrinter printer = new CSVPrinter(sw, csvFormat)) {
      records.forEach(
          (sv, summary) -> {
            try {
              TreeSet<String> dates = (TreeSet<String>) (summary.getUniqueDates());
              printer.printRecord(
                  sv,
                  summary.places.size(),
                  summary.numObservations,
                  summary.getMinValue(),
                  summary.getMaxValue(),
                  summary.dates.size(),
                  !summary.dates.isEmpty() ? dates.first() : "",
                  !summary.dates.isEmpty() ? dates.last() : "",
                  summary.mMethods.toString(),
                  summary.units.toString(),
                  summary.scalingFactors.toString(),
                  summary.observationPeriods.toString());
            } catch (IOException e) {
              e.printStackTrace();
            }
          });
    }
  }
}
