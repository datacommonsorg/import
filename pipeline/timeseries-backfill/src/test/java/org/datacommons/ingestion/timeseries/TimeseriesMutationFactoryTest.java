package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.io.gcp.spanner.MutationGroup;
import org.datacommons.Storage.Observations;
import org.junit.Test;

public class TimeseriesMutationFactoryTest {
  @Test
  public void toTimeSeriesMutation_usesExistingSeriesDcidAndSourceProvenance() {
    SourceSeriesRow row =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "12345",
            "P1Y",
            "CensusACS5yrSurvey",
            "Person",
            "1",
            "CensusACS5YearSurvey",
            "https://example.com",
            false,
            "dc/base/CensusACS5YearSurvey");

    Mutation mutation = TimeseriesMutationFactory.toTimeSeriesMutation(row, "TimeSeries");

    assertEquals("TimeSeries", mutation.getTable());
    assertEquals("dc/os/Count_Person_geoId_06_12345", mutation.asMap().get("id").getString());
    assertEquals("dc/base/CensusACS5YearSurvey", mutation.asMap().get("provenance").getString());
  }

  @Test
  public void toTimeSeriesAttributeMutations_preservesNormalizedReadAttributes() {
    SourceSeriesRow row =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "12345",
            "P1Y",
            "CensusACS5yrSurvey",
            "Person",
            "1",
            "CensusACS5YearSurvey",
            "",
            true,
            "dc/base/CensusACS5YearSurvey");

    List<Mutation> mutations =
        TimeseriesMutationFactory.toTimeSeriesAttributeMutations(row, "TimeSeriesAttribute");

    Map<String, String> properties =
        mutations.stream()
            .collect(
                Collectors.toMap(
                    mutation -> mutation.asMap().get("property").getString(),
                    mutation -> mutation.asMap().get("value").getString()));

    assertEquals("geoId/06", properties.get("observationAbout"));
    assertEquals("12345", properties.get("facetId"));
    assertEquals("CensusACS5YearSurvey", properties.get("importName"));
    assertEquals("P1Y", properties.get("observationPeriod"));
    assertEquals("CensusACS5yrSurvey", properties.get("measurementMethod"));
    assertEquals("Person", properties.get("unit"));
    assertEquals("1", properties.get("scalingFactor"));
    assertEquals("true", properties.get("isDcAggregate"));
    assertFalse(properties.containsKey("provenanceUrl"));
  }

  @Test
  public void toStatVarObservationMutation_emitsPointMutation() {
    SourcePointRow row = new SourcePointRow("geoId/06", "Count_Person", "12345", "2024", "123.4");

    Mutation mutation =
        TimeseriesMutationFactory.toStatVarObservationMutation(row, "StatVarObservation");

    assertEquals("StatVarObservation", mutation.getTable());
    assertEquals("2024", mutation.asMap().get("date").getString());
    assertEquals("123.4", mutation.asMap().get("value").getString());
  }

  @Test
  public void toMutationGroups_groupsParentAttributesAndPointsFromSingleSourceRow() {
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "12345",
            "P1Y",
            "CensusACS5yrSurvey",
            "Person",
            "1",
            "CensusACS5YearSurvey",
            "",
            true,
            "dc/base/CensusACS5YearSurvey");
    List<SourcePointRow> pointRows =
        List.of(
            new SourcePointRow("geoId/06", "Count_Person", "12345", "2023", "1"),
            new SourcePointRow("geoId/06", "Count_Person", "12345", "2024", "2"));

    BackfillMutationGroups mutationGroups =
        TimeseriesMutationFactory.toMutationGroups(
            new SourceObservationRow(seriesRow, pointRows),
            "TimeSeries",
            "TimeSeriesAttribute",
            "StatVarObservation");

    assertEquals(1, mutationGroups.groups().size());
    assertEquals(8, mutationGroups.timeSeriesAttributeRows());
    assertEquals(2, mutationGroups.statVarObservationRows());

    MutationGroup mutationGroup = mutationGroups.groups().get(0);
    assertEquals("TimeSeries", mutationGroup.primary().getTable());
    assertEquals(10, mutationGroup.attached().size());
    assertTrue(
        mutationGroup.attached().stream()
            .anyMatch(mutation -> mutation.getTable().equals("TimeSeriesAttribute")));
    assertTrue(
        mutationGroup.attached().stream()
            .anyMatch(mutation -> mutation.getTable().equals("StatVarObservation")));
  }

  @Test
  public void toMutationGroups_splitsLargePointSetsAcrossGroups() {
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "12345",
            "",
            "",
            "",
            "",
            "",
            "",
            false,
            "dc/base/CensusACS5YearSurvey");
    List<SourcePointRow> pointRows = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      pointRows.add(
          new SourcePointRow("geoId/06", "Count_Person", "12345", Integer.toString(i), "1"));
    }

    BackfillMutationGroups mutationGroups =
        TimeseriesMutationFactory.toMutationGroups(
            new SourceObservationRow(seriesRow, pointRows),
            "TimeSeries",
            "TimeSeriesAttribute",
            "StatVarObservation");

    assertEquals(2, mutationGroups.groups().size());
    assertEquals("TimeSeries", mutationGroups.groups().get(0).primary().getTable());
    assertEquals("TimeSeries", mutationGroups.groups().get(1).primary().getTable());
  }

  @Test
  public void toMutationGroups_compactRowAvoidsExpandedPointListInput() {
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            "geoId/06",
            "Count_Person",
            "12345",
            "",
            "",
            "",
            "",
            "TestImport",
            "",
            false,
            "dc/base/TestImport");
    CompactSourceObservationRow compactRow =
        new CompactSourceObservationRow(
            seriesRow,
            Observations.newBuilder()
                .putValues("2023", "1")
                .putValues("2024", "2")
                .build()
                .toByteArray());

    BackfillMutationGroups mutationGroups =
        TimeseriesMutationFactory.toMutationGroups(
            compactRow, "TimeSeries", "TimeSeriesAttribute", "StatVarObservation");

    assertEquals(1, mutationGroups.groups().size());
    assertEquals(2, mutationGroups.statVarObservationRows());
    assertEquals("TimeSeries", mutationGroups.groups().get(0).primary().getTable());
  }

  @Test
  public void seriesIdGenerator_replacesSlashesWithUnderscores() {
    assertEquals(
        "dc/os/Count_Person_geoId_06_123",
        SeriesIdGenerator.build("Count_Person", "geoId/06", "123"));
    assertTrue(SeriesIdGenerator.build("Count/Person", "geoId/06", "123").contains("Count_Person"));
  }
}
