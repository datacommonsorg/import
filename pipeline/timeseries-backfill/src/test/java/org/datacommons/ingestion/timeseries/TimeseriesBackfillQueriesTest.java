package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.cloud.spanner.Statement;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;

public class TimeseriesBackfillQueriesTest {
  @Test
  public void buildSourceQuery_selectsMetadataObservationsAndShardFilters() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setSourceObservationTableName("Observation");
    options.setVariableMeasured("Count_Person");
    options.setStartObservationAbout("geoId/06");
    options.setEndObservationAboutExclusive("geoId/07");

    Statement query = new TimeseriesBackfillQueries(options).buildSourceQuery();

    assertTrue(query.getSql().contains("provenance"));
    assertTrue(query.getSql().contains("observations"));
    assertTrue(query.getSql().contains("FROM Observation"));
    assertTrue(query.getSql().contains("observation_about >= @startObservationAbout"));
    assertTrue(query.getSql().contains("observation_about < @endObservationAboutExclusive"));
    assertTrue(query.getSql().contains("variable_measured = @variableMeasured"));
    assertFalse(query.getSql().contains("UNNEST"));
    assertTrue(query.hasBinding("startObservationAbout"));
    assertTrue(query.hasBinding("endObservationAboutExclusive"));
    assertTrue(query.hasBinding("variableMeasured"));
  }

  @Test
  public void buildSourceQuery_supportsCommaSeparatedVariableMeasuredFilters() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setSourceObservationTableName("Observation");
    options.setVariableMeasured("Count_Person, Min_Temperature ,Max_Temperature");

    Statement query = new TimeseriesBackfillQueries(options).buildSourceQuery();

    assertTrue(query.getSql().contains("variable_measured IN UNNEST(@variableMeasuredList)"));
    assertFalse(query.hasBinding("variableMeasured"));
    assertTrue(query.hasBinding("variableMeasuredList"));
  }

  @Test
  public void buildSourceQuery_supportsBoundedValidatorRuns() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setSourceObservationTableName("Observation");

    Statement query = new TimeseriesBackfillQueries(options).buildSourceQuery(10);

    assertTrue(query.getSql().contains("provenance"));
    assertTrue(query.getSql().contains("observations"));
    assertTrue(query.getSql().contains("LIMIT 10"));
    assertTrue(query.getSql().contains("ORDER BY observation_about"));
  }

  @Test
  public void validateOptions_rejectsObservationAboutRangeWithoutVariableMeasured() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setStartObservationAbout("geoId/06");

    try {
      TimeseriesBackfillPipeline.validateOptions(options);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("variableMeasured is required"));
    }
  }

  @Test
  public void validateOptions_acceptsObservationAboutRangeWithCommaSeparatedVariableMeasured() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setVariableMeasured("Count_Person, Min_Temperature");
    options.setStartObservationAbout("geoId/06");

    TimeseriesBackfillPipeline.validateOptions(options);
  }

  @Test
  public void validateOptions_rejectsBeamRowCaps() {
    TimeseriesBackfillOptions options =
        PipelineOptionsFactory.create().as(TimeseriesBackfillOptions.class);
    options.setVariableMeasured("Count_Person");
    options.setMaxSeriesRows(10);

    try {
      TimeseriesBackfillPipeline.validateOptions(options);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) {
      assertTrue(e.getMessage().contains("Beam row caps are not supported"));
    }
  }
}
