package org.datacommons.util;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;
import java.util.TreeSet;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;

class CSVReportWriter {

  enum ReportHeaders {
    StatVar,
    NumPlaces,
    NumObservations,
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
              printer.printRecord(
                  sv,
                  summary.places.size(),
                  summary.numObservations,
                  summary.dates.size(),
                  !summary.dates.isEmpty()
                      ? ((TreeSet<String>) (summary.getUniqueDates())).first()
                      : "",
                  !summary.dates.isEmpty()
                      ? ((TreeSet<String>) (summary.getUniqueDates())).last()
                      : "",
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

