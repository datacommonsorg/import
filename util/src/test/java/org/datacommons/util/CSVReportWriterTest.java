package org.datacommons.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.datacommons.util.SummaryReportGenerator.StatVarSummary;
import org.junit.Before;
import org.junit.Test;

public class CSVReportWriterTest {

  public static final String[] HEADERS = {"author", "title"};
  public static final String EXPECTED_FILESTREAM =
      "StatVar,NumPlaces,NumObservations,NumObservationsDates,MinDate,MaxDate,MeasurementMethods,Units,ScalingFactors,observationPeriods\r\n"
          + "Var1,0,6,0,,,[CensusACS5YrSurvey],[],[],[]\r\n"
          + "Var2,0,2,2,2020,2025,[CensusACS5YrSurvey],[],[],[]";
  Map<String, StatVarSummary> records;

  @Before
  public void setUp() {
    StatVarSummary countPersonExpectedSummary = new StatVarSummary();
    countPersonExpectedSummary.numObservations = 6;
    countPersonExpectedSummary.mMethods = Set.of("CensusACS5YrSurvey");
    StatVarSummary countFemaleExpectedSummary = new StatVarSummary();
    countFemaleExpectedSummary.numObservations = 2;
    countFemaleExpectedSummary.mMethods = Set.of("CensusACS5YrSurvey");
    countFemaleExpectedSummary.dates = Set.of("2020", "2025");

    records =
        Collections.unmodifiableMap(
            new LinkedHashMap<String, StatVarSummary>() {
              {
                put("Var1", countPersonExpectedSummary);
                put("Var2", countFemaleExpectedSummary);
              }
            });
  }

  @Test
  public void csvWriterSuccess() throws IOException {
    StringWriter sw = new StringWriter();
    CSVReportWriter.writeRecords(records, sw);
    assertEquals(EXPECTED_FILESTREAM, sw.toString().trim());
  }
}

