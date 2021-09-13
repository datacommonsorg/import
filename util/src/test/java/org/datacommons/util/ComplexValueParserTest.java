// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.util;

import static org.junit.Assert.*;

import java.nio.file.Path;
import org.datacommons.proto.Debug;
import org.datacommons.proto.Mcf;
import org.junit.Test;

public class ComplexValueParserTest {
  @Test
  public void testQuantityDcid() {
    assertEquals("Years10To20", toComplexValueDcid("[dcs:Years 10 20]"));
    assertEquals("Years10To20", toComplexValueDcid("[10 20 Years]"));
    // Spaces within [] are okay
    assertEquals("Years10To20", toComplexValueDcid("[ Years 10 20 ]"));

    assertEquals("Years10Onwards", toComplexValueDcid("[dcs:Years 10 -]"));
    assertEquals("Years10Onwards", toComplexValueDcid("[10 - dcs:Years]"));

    assertEquals("YearsUpto20", toComplexValueDcid("[Years - 20]"));
    assertEquals("YearsUpto20", toComplexValueDcid("[- 20 dcs:Years]"));

    assertEquals("Years10", toComplexValueDcid("[dcs:Years 10]"));
    assertEquals("Years10", toComplexValueDcid("[10 Years]"));
  }

  @Test
  public void testQuantityNode() {
    String exp =
        "Node: Years10To20\n"
            + "dcid: \"Years10To20\"\n"
            + "endValue: 20\n"
            + "name: \"Years 10 To 20\"\n"
            + "startValue: 10\n"
            + "typeOf: dcid:QuantityRange\n"
            + "unit: dcid:Years\n\n";
    assertEquals(toComplexValueMcf("[10 20 Years]"), exp);

    exp =
        "Node: Years10Onwards\n"
            + "dcid: \"Years10Onwards\"\n"
            + "endValue: \"-\"\n"
            + "name: \"Years 10 Onwards\"\n"
            + "startValue: 10\n"
            + "typeOf: dcid:QuantityRange\n"
            + "unit: dcid:Years\n\n";
    assertEquals(toComplexValueMcf("[10 - dcs:Years]"), exp);

    exp =
        "Node: Years10\n"
            + "dcid: \"Years10\"\n"
            + "name: \"Years 10\"\n"
            + "typeOf: dcid:Quantity\n"
            + "unit: dcid:Years\n"
            + "value: 10\n\n";
    assertEquals(toComplexValueMcf("[10 Years]"), exp);
  }

  @Test
  public void testLatLongNode() {
    String exp =
        "Node: latLong/3738848_-12208344\n"
            + "dcid: \"latLong/3738848_-12208344\"\n"
            + "latitude: \"37.3884812\"\n"
            + "longitude: \"-122.0834373\"\n"
            + "name: \"37.38848,-122.08344\"\n"
            + "typeOf: dcid:GeoCoordinates\n\n";
    assertEquals(toComplexValueMcf("[LatLong 37.3884812 -122.0834373]"), exp);
  }

  @Test
  public void testQuantityFailure() {
    Debug.Log log = toComplexValueFailure("[]");
    assertEquals(1, TestUtil.getCounter(log, "StrSplit_EmptyToken_p1"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Empty value found"));
    assertEquals(1, TestUtil.getCounter(log, "MCF_MalformedComplexValueParts"));
    assertTrue(log.getEntries(1).getUserMessage().contains("value must have 2"));

    log = toComplexValueFailure("dcs:Years 10]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_UnenclosedComplexValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("not enclosed in [] brackets"));

    log = toComplexValueFailure("[Years 10");
    assertEquals(1, TestUtil.getCounter(log, "MCF_UnenclosedComplexValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("not enclosed in [] brackets"));

    log = toComplexValueFailure("[Years 1 2 3]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_MalformedComplexValueParts"));
    assertTrue(log.getEntries(0).getUserMessage().contains("value must have 2"));

    log = toComplexValueFailure("[dcs:Years -]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_QuantityMalformedValue"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));

    log = toComplexValueFailure("[dcs:Years - -]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_QuantityRangeMalformedValues"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));

    log = toComplexValueFailure("[a b Years]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_QuantityRangeMalformedValues"));
    assertTrue(log.getEntries(0).getUserMessage().contains("must be a number"));
  }

  @Test
  public void testLatLngDcid() {
    assertEquals(
        "latLong/3738848_-12208344", toComplexValueDcid("[LatLong 37.3884812 -122.0834373]"));
    assertEquals(
        "latLong/3738848_-12208344", toComplexValueDcid("[37.3884812 -122.0834373 latlong]"));

    assertEquals(
        "latLong/3738848_-12208344", toComplexValueDcid("[LatLong 37.3884812N 122.0834373W]"));
    assertEquals(
        "latLong/-3738848_12208344", toComplexValueDcid("[LatLong 37.3884812s 122.0834373e]"));

    // Clarify that 12.21 and 122.1 values packed in DCID are different.
    assertEquals("latLong/1221000_12210000", toComplexValueDcid("[LatLong 12.21 122.1]"));
  }

  @Test
  public void testLatLngFailure() {
    Debug.Log log = toComplexValueFailure("[LatLong a b]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 91 -10.0]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 90.0 -181.0]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_InvalidLongitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid longitude value"));

    log = toComplexValueFailure("[LatLong 37.3884812w 122.0834373]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_InvalidLatitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid latitude value"));

    log = toComplexValueFailure("[LatLong 37.3884812 122.0834373n]");
    assertEquals(1, TestUtil.getCounter(log, "MCF_InvalidLongitude"));
    assertTrue(log.getEntries(0).getUserMessage().contains("Invalid longitude value"));
  }

  private static String toComplexValueDcid(String value) {
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), Path.of("/tmp"));
    Mcf.McfGraph.PropertyValues dummyNode = Mcf.McfGraph.PropertyValues.newBuilder().build();
    ComplexValueParser parser = new ComplexValueParser("n1", dummyNode, "p1", value, null, logCtx);
    assertTrue(parser.parse());
    return parser.getDcid();
  }

  private static String toComplexValueMcf(String value) {
    LogWrapper logCtx = new LogWrapper(Debug.Log.newBuilder(), Path.of("/tmp"));
    Mcf.McfGraph.PropertyValues dummyNode = Mcf.McfGraph.PropertyValues.newBuilder().build();
    Mcf.McfGraph.PropertyValues.Builder newNode = Mcf.McfGraph.PropertyValues.newBuilder();
    ComplexValueParser parser =
        new ComplexValueParser("n1", dummyNode, "p1", value, newNode, logCtx);
    assertTrue(parser.parse());
    return McfUtil.serializeMcfNode(parser.getDcid(), newNode.build(), true);
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
