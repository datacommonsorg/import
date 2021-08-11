package org.datacommons.util;

import static org.junit.Assert.*;

import java.nio.file.Path;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

public class ComplexValueParserTest {
  @Test
  public void testQuantitySuccess() {
    assertEquals("Years10To20", toComplexValueSuccess("[dcs:Years 10 20]"));
    assertEquals("Years10To20", toComplexValueSuccess("[10 20 Years]"));

    assertEquals("Years10Onwards", toComplexValueSuccess("[dcs:Years 10 -]"));
    assertEquals("Years10Onwards", toComplexValueSuccess("[10 - dcs:Years]"));

    assertEquals("YearsUpto20", toComplexValueSuccess("[Years - 20]"));
    assertEquals("YearsUpto20", toComplexValueSuccess("[- 20 dcs:Years]"));

    assertEquals("Years10", toComplexValueSuccess("[dcs:Years 10]"));
    assertEquals("Years10", toComplexValueSuccess("[10 Years]"));
  }

  @Test
  public void testQuantityFailure() {
    Debug.Log log = toComplexValueFailure("[]");
    assertTrue(log.getCounterSet().containsCounters("MCF_MalformedComplexValueString"));
    assertTrue(log.getEntries(0).getUserMessage().contains("malformed"));

    log = toComplexValueFailure("dcs:Years 10]");
    assertTrue(log.getCounterSet().containsCounters("MCF_UnenclosedComplexValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("not enclosed in brackets"));

    log = toComplexValueFailure("[Years 10");
    assertTrue(log.getCounterSet().containsCounters("MCF_UnenclosedComplexValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("not enclosed in brackets"));

    log = toComplexValueFailure("[Years 1 2 3]");
    assertTrue(log.getCounterSet().containsCounters("MCF_MalformedComplexValueParts"));
    assertTrue(log.getEntries(0).getUserMessage().contains("2 or 3"));

    log = toComplexValueFailure("[dcs:Years -]");
    assertTrue(log.getCounterSet().containsCounters("MCF_QuantityMalformedValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));

    log = toComplexValueFailure("[dcs:Years - -]");
    assertTrue(log.getCounterSet().containsCounters("MCF_QuantityRangeMalformedValues"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));

    log = toComplexValueFailure("[a b Years]");
    assertTrue(log.getCounterSet().containsCounters("MCF_QuantityRangeMalformedValues"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));
  }

  @Test
  public void testLatLngSuccess() {
    assertEquals(
        "latLong/3738848_-12208344", toComplexValueSuccess("[LatLong 37.3884812 -122.0834373]"));
    assertEquals(
        "latLong/3738848_-12208344", toComplexValueSuccess("[37.3884812 -122.0834373 latlong]"));

    assertEquals(
        "latLong/3738848_-12208344", toComplexValueSuccess("[LatLong 37.3884812N 122.0834373W]"));
    assertEquals(
        "latLong/-3738848_12208344", toComplexValueSuccess("[LatLong 37.3884812s 122.0834373e]"));

    // Clarify that 12.21 and 122.1 values packed in DCID are different.
    assertEquals("latLong/1221000_12210000", toComplexValueSuccess("[LatLong 12.21 122.1]"));
  }

  @Test
  public void testLatLngFailure() {
    Debug.Log log = toComplexValueFailure("[LatLong a b]");
    assertTrue(log.getCounterSet().containsCounters("MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 91 -10.0]");
    assertTrue(log.getCounterSet().containsCounters("MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 90.0 -181.0]");
    assertTrue(log.getCounterSet().containsCounters("MCF_InvalidLongitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid longitude value"));

    log = toComplexValueFailure("[LatLong 37.3884812w 122.0834373]");
    assertTrue(log.getCounterSet().containsCounters("MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 37.3884812 122.0834373n]");
    assertTrue(log.getCounterSet().containsCounters("MCF_InvalidLongitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid longitude value"));
  }

  private static String toComplexValueSuccess(String value) {
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), Path.of("/tmp"));
    Mcf.McfGraph.PropertyValues dummyNode = Mcf.McfGraph.PropertyValues.newBuilder().build();
    ComplexValueParser parser = new ComplexValueParser("n1", dummyNode, "p1", value, null, logCtx);
    assertTrue(parser.parse());
    return parser.getDcid();
  }

  private static Debug.Log toComplexValueFailure(String value) {
    Debug.Log.Builder log = Debug.Log.newBuilder();
    LogWrapper logCtx = new LogWrapper(log, Path.of("/tmp"));
    Mcf.McfGraph.PropertyValues dummyNode = Mcf.McfGraph.PropertyValues.newBuilder().build();
    ComplexValueParser parser = new ComplexValueParser("n1", dummyNode, "p1", value, null, logCtx);
    assertFalse(parser.parse());
    return log.build();
  }
}
