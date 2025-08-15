package org.datacommons.ingestion.data;

import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import java.util.List;
import org.apache.beam.sdk.metrics.Counter;
import org.datacommons.Storage.Observations;
import org.datacommons.pipeline.util.PipelineUtils;
import org.junit.Test;
import org.mockito.Mockito;

public class CacheReaderTest {
  @Test
  public void testParseArcRowForOutArcWithReferenceNode() {
    CacheReader reader = newCacheReader();
    Counter mockMcfNodesWithoutTypeCounter = Mockito.mock(Counter.class);
    String row =
        "d/m/Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person^measuredProperty^Property^0,H4sIAAAAAAAAAOPS5WJNzi/NK5GCUEqyKcn6SYnFqfoepbmJeUGpiSmJSTmpwSWJJWGJRcWCDGDwwR4AejAnwDgAAAA=";

    NodesEdges expected =
        new NodesEdges()
            .addNode(
                Node.builder()
                    .subjectId("count")
                    .value("count")
                    .name("count")
                    .types(List.of("Property"))
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId(
                        "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person")
                    .predicate("measuredProperty")
                    .objectId("count")
                    .provenance("dc/base/HumanReadableStatVars")
                    .build());

    NodesEdges actual = reader.parseArcRow(row, mockMcfNodesWithoutTypeCounter);

    assertEquals(expected, actual);
    Mockito.verify(mockMcfNodesWithoutTypeCounter, Mockito.times(0)).inc();
  }

  @Test
  public void testParseArcRowForOutArcWithValueNode() {
    CacheReader reader = newCacheReader();
    Counter mockMcfNodesWithoutTypeCounter = Mockito.mock(Counter.class);
    String row =
        "d/m/Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person^name^^0,H4sIAAAAAAAAAONqYFSSTUnWT0osTtX3KM1NzAtKTUxJTMpJDS5JLAlLLCrWig9ILUpOzStJTE9VCM8vylYISs1JLElNUQjIqCzOTE7MUXBMLsksyyyp1FHwzU9JLQJKwoUU/IsUPFITyyoRIo65+XnpCgH5BaVAYzLz8wQZwOCDPQA1JajOjAAAAA==";

    NodesEdges expected =
        new NodesEdges()
            .addNode(
                Node.builder()
                    .subjectId("c6CV18sK/njghkqgkS/mMaTkKP+oWup0pgYkS6iFpvY=")
                    .value(
                        "Percentage Work Related Physical Activity, Moderate Activity Or Heavy Activity Among Population")
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId(
                        "Percent_WorkRelatedPhysicalActivity_ModerateActivityOrHeavyActivity_In_Count_Person")
                    .predicate("name")
                    .objectId("c6CV18sK/njghkqgkS/mMaTkKP+oWup0pgYkS6iFpvY=")
                    .provenance("dc/base/HumanReadableStatVars")
                    .build());

    NodesEdges actual = reader.parseArcRow(row, mockMcfNodesWithoutTypeCounter);

    assertEquals(expected, actual);
    Mockito.verify(mockMcfNodesWithoutTypeCounter, Mockito.times(0)).inc();
  }

  @Test
  public void testParseArcRowForOutArcWithBytesNode() {
    CacheReader reader = newCacheReader();
    Counter mockMcfNodesWithoutTypeCounter = Mockito.mock(Counter.class);
    String row =
        "d/m/ipcc_50/6.75_9.25_NGA^geoJsonCoordinates^^0,H4sIAAAAAAAAAONawKgklJKsn5RYnKrvGeDsHJCTmJxarNXJWK2goKBUUlmQqmSloBSQn1OZnp+npAMSTM7PL0rJzEssSS0GykUrgACEBNKWOgrmsTpIPDM9UyS+nik2EVQd5rFgZiwY1yoIMoDBB3sAUJT1uKwAAAA=";

    NodesEdges expected =
        new NodesEdges()
            .addNode(
                Node.builder()
                    .subjectId("G8RZr2tV3+cSSDVRj8Q4KnMpxDhZyZr438T3Fvq1Zkk=")
                    .bytes(
                        ByteArray.copyFrom(
                            PipelineUtils.compressString(
                                "{   \"type\": \"Polygon\",   \"coordinates\": [     [       [9, 7],       [9, 6.5],       [9.5, 6.5],       [9.5, 7],       [9, 7]     ]   ] } ")))
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("ipcc_50/6.75_9.25_NGA")
                    .predicate("geoJsonCoordinates")
                    .objectId("G8RZr2tV3+cSSDVRj8Q4KnMpxDhZyZr438T3Fvq1Zkk=")
                    .provenance("dc/base/IPCCPlaces")
                    .build());

    NodesEdges actual = reader.parseArcRow(row, mockMcfNodesWithoutTypeCounter);

    assertEquals(expected, actual);
    Mockito.verify(mockMcfNodesWithoutTypeCounter, Mockito.times(0)).inc();
  }

  @Test
  public void testParseArcRowForInArc() {
    CacheReader reader = newCacheReader();
    Counter mockMcfNodesWithoutTypeCounter = Mockito.mock(Counter.class);
    String row =
        "d/l/dc/d/UnitedNationsUn_SdgIndicatorsDatabase^isPartOf^Provenance^0,H4sIAAAAAAAAAOPS4GIL9YsPdnGX4ktJ1k9KLE7Vh/CV0PiCDGDwwR4AhMbiaDMAAAA=";

    NodesEdges expected =
        new NodesEdges()
            .addNode(
                Node.builder()
                    .subjectId("dc/base/UN_SDG")
                    .value("dc/base/UN_SDG")
                    .name("UN_SDG")
                    .types(List.of("Provenance"))
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("dc/base/UN_SDG")
                    .predicate("isPartOf")
                    .objectId("dc/d/UnitedNationsUn_SdgIndicatorsDatabase")
                    .provenance("dc/base/UN_SDG")
                    .build());

    NodesEdges actual = reader.parseArcRow(row, mockMcfNodesWithoutTypeCounter);

    assertEquals(expected, actual);
    Mockito.verify(mockMcfNodesWithoutTypeCounter, Mockito.times(0)).inc();
  }

  @Test
  public void testParseTimeSeriesRow() {
    CacheReader reader = newCacheReader();
    String row =
        "d/3/geoId/sch2915390^Mean_PrecipitableWater_Atmosphere,H4sIAAAAAAAAAOPqZpHi8igtykmtVAjS9TT6w8wlysVlZGBkqmtgpGtkIMgQvilZeYOoA4qwkeBcve7Os9OVUIWNBQOzZ5XPaVJBEjbWNTAU3OHQ9Oj4DD1U1aaCIvbH7mx9ookibGguqN6xi98hSwZV2EJQ0Mxmb5CYFqohJoJMszik59WpogobCq49MOHqWjk5VJcYCRac67h074AqqrCxoOOVWoag1QqohlgIpnUkx96ZZoQqbC7IeI+Js2s+mkvMBKub9X2WrdJENdtE0N9BOPHwZkVU71gKGoOBioMUh5+/o2O8u1uwEnOAoYuWBIwbH56aWJKRWuSWX5SanFhcYsSRl5+YqJeeX+bE5ZuZk5OZm1qSWuTBGOSZUVJSUGylr19eXq6Xl5yaqQdTqF9QlJ9SmlxSrF8OMUs3GagtsSRVNzc/JTWnWD89Jz8pMUc3DWoFAAwgwZ8OAgAA";

    Observations series =
        Observations.newBuilder()
            .putValues("2025-02-20", "5.42201")
            .putValues("2025-02-22", "9.29649")
            .putValues("2025-02-23", "10.2551")
            .putValues("2025-03-01", "15.2984")
            .putValues("2025-02-25", "12.9467")
            .putValues("2025-02-17", "7.10376")
            .putValues("2025-02-18", "13.0436")
            .putValues("2025-02-24", "10.7473")
            .putValues("2025-02-21", "7.52996")
            .putValues("2025-03-02", "10.8767")
            .putValues("2025-03-03", "8.33461")
            .putValues("2025-02-28", "18.5893")
            .putValues("2025-02-27", "13.3116")
            .putValues("2025-02-26", "12.8333")
            .putValues("2025-03-04", "8.8511")
            .putValues("2025-02-19", "10.1")
            .build();

    List<Observation> expected =
        List.of(
            Observation.builder()
                .variableMeasured("Mean_PrecipitableWater_Atmosphere")
                .observationAbout("geoId/sch2915390")
                .observations(series)
                .observationPeriod("P1D")
                .measurementMethod("NOAA_GFS")
                .scalingFactor("")
                .unit("Millimeter")
                .isDcAggregate(true)
                .provenanceUrl(
                    "https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast")
                .importName("NOAA_GFS_WeatherForecast")
                .build());

    NodesEdges expectedGraph =
        new NodesEdges()
            .addNode(
                Node.builder()
                    .subjectId("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .value("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .name("Mean_PrecipitableWater_Atmosphere | geoId/sch2915390 | 870755137")
                    .types(List.of("StatVarObsSeries"))
                    .build())
            .addNode(
                Node.builder()
                    .subjectId("jVWNIHt73yOspqKD0fnvTCH8GCW7m38F3gW+JB+aWms=")
                    .value("Mean_PrecipitableWater_Atmosphere | geoId/sch2915390 | 870755137")
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .predicate("variableMeasured")
                    .objectId("Mean_PrecipitableWater_Atmosphere")
                    .provenance("dc/base/NOAA_GFS_WeatherForecast")
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .predicate("observationAbout")
                    .objectId("geoId/sch2915390")
                    .provenance("dc/base/NOAA_GFS_WeatherForecast")
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .predicate("name")
                    .objectId("jVWNIHt73yOspqKD0fnvTCH8GCW7m38F3gW+JB+aWms=")
                    .provenance("dc/base/NOAA_GFS_WeatherForecast")
                    .build())
            .addEdge(
                Edge.builder()
                    .subjectId("dc/os/Mean_PrecipitableWater_Atmosphere_geoId_sch2915390_870755137")
                    .predicate("typeOf")
                    .objectId("StatVarObsSeries")
                    .provenance("dc/base/NOAA_GFS_WeatherForecast")
                    .build());

    List<Observation> actual = reader.parseTimeSeriesRow(row);

    assertEquals(expected, actual);

    assertEquals(expectedGraph, actual.get(0).getObsGraph());
  }

  private static CacheReader newCacheReader() {
    return new CacheReader("datcom-store");
  }
}
