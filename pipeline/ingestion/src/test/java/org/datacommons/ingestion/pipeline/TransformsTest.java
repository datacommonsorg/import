package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Mutation;
import java.util.List;
import org.apache.beam.sdk.values.KV;
import org.datacommons.Storage.Observations;
import org.datacommons.ingestion.data.Observation;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class TransformsTest {

  @Test
  public void testToNewSchemaMutations() {
    Observations.Builder obsBuilder = Observations.newBuilder();
    obsBuilder.putValues("2020", "10.0");
    obsBuilder.putValues("2021", "12.0");

    Observation obs =
        Observation.builder()
            .variableMeasured("count")
            .observationAbout("country/USA")
            .importName("test_import")
            .unit("Count")
            .measurementMethod("Census")
            .observationPeriod("P1Y")
            .scalingFactor("1.0")
            .observations(obsBuilder.build())
            .build();

    List<KV<String, Mutation>> mutations = Transforms.toNewSchemaMutations(obs);

    // Expected mutations:
    // 1 TimeSeries
    // 5 TimeSeriesAttribute (observationAbout, unit, scalingFactor, measurementMethod,
    // observationPeriod)
    // 2 StatVarObservation (for 2020 and 2021)
    // Total = 8

    assertEquals(8, mutations.size());

    // Verify TimeSeries mutation
    Mutation tsMutation = findMutationByTable(mutations, "TimeSeries");
    assertEquals("TimeSeries", tsMutation.getTable());
    assertEquals(
        "dc/os/count_country_USA_" + obs.getFacetId(), tsMutation.asMap().get("id").getString());
    assertEquals("count", tsMutation.asMap().get("variable_measured").getString());
    assertEquals("dc/base/test_import", tsMutation.asMap().get("provenance").getString());

    // Verify StatVarObservation mutations
    List<Mutation> svoMutations = findMutationsByTable(mutations, "StatVarObservation");
    assertEquals(2, svoMutations.size());

    Mutation m2020 = findMutationByDate(svoMutations, "2020");
    assertEquals("10.0", m2020.asMap().get("value").getString());

    Mutation m2021 = findMutationByDate(svoMutations, "2021");
    assertEquals("12.0", m2021.asMap().get("value").getString());
  }

  @Test
  public void testFilterNewSchemaMutations() {
    Transforms.LRUCache<String, Boolean> seenObs = new Transforms.LRUCache<>(100);

    Mutation m1 = Mutation.newInsertOrUpdateBuilder("TimeSeries").set("id").to("ts1").build();
    Mutation m2 =
        Mutation.newInsertOrUpdateBuilder("StatVarObservation")
            .set("id")
            .to("ts1")
            .set("date")
            .to("2020")
            .set("value")
            .to("10")
            .build();

    List<KV<String, Mutation>> kvs =
        List.of(
            KV.of("ts1", m1),
            KV.of("ts1", m2),
            KV.of("ts1", m1), // Duplicate
            KV.of("ts1", m2) // Duplicate
            );

    List<KV<String, Mutation>> filtered = Transforms.filterNewSchemaMutations(kvs, seenObs);

    assertEquals(2, filtered.size());
    assertTrue(
        filtered.stream().map(KV::getValue).anyMatch(m -> m.getTable().equals("TimeSeries")));
    assertTrue(
        filtered.stream()
            .map(KV::getValue)
            .anyMatch(m -> m.getTable().equals("StatVarObservation")));
  }

  private Mutation findMutationByTable(List<KV<String, Mutation>> kvs, String table) {
    return kvs.stream()
        .map(KV::getValue)
        .filter(m -> m.getTable().equals(table))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Mutation for table " + table + " not found"));
  }

  private List<Mutation> findMutationsByTable(List<KV<String, Mutation>> kvs, String table) {
    return kvs.stream().map(KV::getValue).filter(m -> m.getTable().equals(table)).toList();
  }

  private Mutation findMutationByDate(List<Mutation> mutations, String date) {
    return mutations.stream()
        .filter(m -> m.asMap().get("date").getString().equals(date))
        .findFirst()
        .orElseThrow(() -> new AssertionError("Mutation for date " + date + " not found"));
  }
}
