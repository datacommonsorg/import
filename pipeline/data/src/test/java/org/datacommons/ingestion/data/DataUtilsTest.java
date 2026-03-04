package org.datacommons.ingestion.data;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class DataUtilsTest {

  private final String expectedId;
  private final String importName;
  private final String measurementMethod;
  private final String observationPeriod;
  private final String scalingFactor;
  private final String unit;
  private final boolean isDcAggregate;

  public DataUtilsTest(
      String expectedId,
      String importName,
      String measurementMethod,
      String observationPeriod,
      String scalingFactor,
      String unit,
      boolean isDcAggregate) {
    this.expectedId = expectedId;
    this.importName = importName;
    this.measurementMethod = measurementMethod;
    this.observationPeriod = observationPeriod;
    this.scalingFactor = scalingFactor;
    this.unit = unit;
    this.isDcAggregate = isDcAggregate;
  }

  // This method provides the data for the test below
  @Parameters(name = "Test {index}: expected {0} for {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          // Format: expectedId, importName, measurementMethod, observationPeriod, scalingFactor,
          // unit, isDcAggregate
          {"3981252704", "WorldDevelopmentIndicators", "", "P1Y", "", "", false},
          {
            "10983471",
            "CensusACS5YearSurvey_SubjectTables_S2601A",
            "CensusACS5yrSurveySubjectTable",
            "",
            "",
            "",
            false
          },
          {"2825511676", "CDC_Mortality_UnderlyingCause", "", "", "", "", false},
          {"1226172227", "CensusACS1YearSurvey", "CensusACS1yrSurvey", "", "", "", false},
          {"2176550201", "USCensusPEP_Annual_Population", "CensusPEPSurvey", "P1Y", "", "", false},
          {"2645850372", "CensusACS5YearSurvey_AggCountry", "CensusACS5yrSurvey", "", "", "", true},
          {
            "1541763368",
            "USDecennialCensus_RedistrictingRelease",
            "USDecennialCensus",
            "",
            "",
            "",
            false
          },
          {
            "4181918134",
            "OECDRegionalDemography_Population",
            "OECDRegionalStatistics",
            "P1Y",
            "",
            "",
            false
          },
          {
            "1964317807",
            "CensusACS5YearSurvey_SubjectTables_S0101",
            "CensusACS5yrSurveySubjectTable",
            "",
            "",
            "",
            false
          },
          {"2517965213", "CensusPEP", "CensusPEPSurvey", "", "", "", false}
        });
  }

  @Test
  public void testGenerateFacetId() {
    String facetId =
        DataUtils.generateFacetId(
            importName, measurementMethod, observationPeriod, scalingFactor, unit, isDcAggregate);

    assertEquals(expectedId, facetId);
  }
}
