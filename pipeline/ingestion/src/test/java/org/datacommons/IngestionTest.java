package org.datacommons;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class IngestionTest {

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void testGetObservations() {
    List<String> cache = new ArrayList<>();
    cache.add(
        "d/3/geoId/sch2915390^Mean_PrecipitableWater_Atmosphere,H4sIAAAAAAAAAOPqZpHi8igtykmtVAjS9TT6w8wlysVlZGBkqmtgpGtkIMgQvilZeYOoA4qwkeBcve7Os9OVUIWNBQOzZ5XPaVJBEjbWNTAU3OHQ9Oj4DD1U1aaCIvbH7mx9ookibGguqN6xi98hSwZV2EJQ0Mxmb5CYFqohJoJMszik59WpogobCq49MOHqWjk5VJcYCRac67h074AqqrCxoOOVWoag1QqohlgIpnUkx96ZZoQqbC7IeI+Js2s+mkvMBKub9X2WrdJENdtE0N9BOPHwZkVU71gKGoOBioMUh5+/o2O8u1uwEnOAoYuWBIwbH56aWJKRWuSWX5SanFhcYsSRl5+YqJeeX+bE5ZuZk5OZm1qSWuTBGOSZUVJSUGylr19eXq6Xl5yaqQdTqF9QlJ9SmlxSrF8OMUs3GagtsSRVNzc/JTWnWD89Jz8pMUc3DWoFAAwgwZ8OAgAA");
    PCollection<String> entries = p.apply(Create.of(cache));
    PCollection<Observation> result = CacheReader.getObservations(entries);
    List<Observation> expected = new ArrayList<>();
    List<String> series =
        Arrays.asList(
            "{\"2025-02-20\" : \"5.42201\"}",
            "{\"2025-02-22\" : \"9.29649\"}",
            "{\"2025-02-23\" : \"10.2551\"}",
            "{\"2025-03-01\" : \"15.2984\"}",
            "{\"2025-02-25\" : \"12.9467\"}",
            "{\"2025-02-17\" : \"7.10376\"}",
            "{\"2025-02-18\" : \"13.0436\"}",
            "{\"2025-02-24\" : \"10.7473\"}",
            "{\"2025-02-21\" : \"7.52996\"}",
            "{\"2025-03-02\" : \"10.8767\"}",
            "{\"2025-03-03\" : \"8.33461\"}",
            "{\"2025-02-28\" : \"18.5893\"}",
            "{\"2025-02-27\" : \"13.3116\"}",
            "{\"2025-02-26\" : \"12.8333\"}",
            "{\"2025-03-04\" : \"8.8511\"}",
            "{\"2025-02-19\" : \"10.1\"}");

    expected.add(
        new Observation(
            "Mean_PrecipitableWater_Atmosphere",
            "geoId/sch2915390",
            series,
            "P1D",
            "NOAA_GFS",
            "",
            "Millimeter",
            "noaa.gov",
            "https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast",
            "NOAA_GFS_WeatherForecast"));
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testGetEntities() {
    List<String> cache = new ArrayList<>();
    cache.add(
        "d/m/Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person^measuredProperty^Property^0,H4sIAAAAAAAAAOPS5WJNzi/NK5GCUEqyKcn6SYnFqfoepbmJeUGpiSmJSTmpwSWJJWGJRcWCDGDwwR4AejAnwDgAAAA=");
    cache.add(
        "d/m/Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person^name^^0,H4sIAAAAAAAAAONqYFSSTUnWT0osTtX3KM1NzAtKTUxJTMpJDS5JLAlLLCrWig9ILUpOzStJTE9VCM8vylYISs1JLElNUQjIqCzOTE7MUXBMLsksyyyp1FHwzU9JLQJKwoUU/IsUPFITyyoRIo65+XnpCgH5BaVAYzLz8wQZwOCDPQA1JajOjAAAAA==");
    cache.add(
        "d/l/dc/d/UnitedNationsUn_SdgIndicatorsDatabase^isPartOf^Provenance^0,H4sIAAAAAAAAAOPS4GIL9YsPdnGX4ktJ1k9KLE7Vh/CV0PiCDGDwwR4AhMbiaDMAAAA=");
    PCollection<String> entries = p.apply(Create.of(cache));
    PCollection<Entity> result = CacheReader.getEntities(entries);
    List<Entity> expected =  Arrays.asList(
        new Entity(
            "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
            "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
            "measuredProperty",
            "count",
            "",
            "dc/base/HumanReadableStatVars",
            "count",
            "count",
            List.of("Property")),
        new Entity(
            "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
            "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
            "name",
            "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
            "Percentage Work Related Physical Activity, Moderate Activity Or Heavy Activity Among Population",
            "dc/base/HumanReadableStatVars",
            "",
            "",
            new ArrayList<String>()),
        new Entity(
            "dc/base/UN_SDG",
            "dc/base/UN_SDG",
            "isPartOf",
            "dc/d/UnitedNationsUn_SdgIndicatorsDatabase",
            "",
            "dc/base/UN_SDG",
            "dc/base/UN_SDG",
            "UN_SDG",
            List.of("Provenance")));
    PAssert.that(result).containsInAnyOrder(expected);
    p.run();
  }

  @Test
  public void testToNode() {
    Entity out = new Entity(
        "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
        "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person",
        "measuredProperty",
        "count",
        "",
        "dc/base/HumanReadableStatVars",
        "count",
        "count",
        List.of("Property"));
    Mutation outExpected = Mutation.newInsertOrUpdateBuilder("Node")
        .set("subject_id")
        .to("count")
        .set("name")
        .to("count")
        .set("types")
        .to(Value.stringArray(List.of("Property")))
        .build();
    Assert.assertTrue(out.toNode("Node").equals(outExpected));
  }
}
