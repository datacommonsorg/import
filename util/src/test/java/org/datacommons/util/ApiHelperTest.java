package org.datacommons.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class ApiHelperTest {

  @Test
  public void convertsNodesWithDcid() throws Exception {
    V2NodeResponse response = new V2NodeResponse();
    response.data = Map.of("geoId/06", nodeWith("typeOf", List.of(nodeWithDcid("Class"))));

    JsonObject legacy = ApiHelper.convertToLegacyFormat(response, List.of("geoId/06"), "typeOf");

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertEquals(1, out.size());
    assertEquals("Class", out.get(0).getAsJsonObject().get("dcid").getAsString());
  }

  @Test
  public void convertsNodesWithValue() throws Exception {
    V2NodeResponse response = new V2NodeResponse();
    response.data = Map.of("geoId/06", nodeWith("name", List.of(nodeWithValue("California"))));

    JsonObject legacy = ApiHelper.convertToLegacyFormat(response, List.of("geoId/06"), "name");

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertEquals(1, out.size());
    assertEquals("California", out.get(0).getAsJsonObject().get("value").getAsString());
  }

  @Test
  public void populatesPlaceholdersWhenNoDataReturned() throws Exception {
    V2NodeResponse response = new V2NodeResponse();
    response.data = Map.of();

    JsonObject legacy = ApiHelper.convertToLegacyFormat(response, List.of("geoId/06"), "name");

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertTrue(out.isEmpty());
  }

  @Test
  public void returnsEmptyWhenPropertyMissing() throws Exception {
    V2NodeResponse.NodeData nodeWithoutProperty = new V2NodeResponse.NodeData();
    nodeWithoutProperty.arcs = Map.of();

    V2NodeResponse response = new V2NodeResponse();
    response.data = Map.of("geoId/06", nodeWithoutProperty);

    JsonObject legacy = ApiHelper.convertToLegacyFormat(response, List.of("geoId/06"), "name");

    JsonArray out = legacy.getAsJsonObject("geoId/06").getAsJsonArray("out");
    assertTrue(out.isEmpty());
  }

  private static V2NodeResponse.NodeData nodeWith(
      String property, List<V2NodeResponse.NodeInfo> nodes) {
    V2NodeResponse.NodeData nodeData = new V2NodeResponse.NodeData();
    nodeData.arcs = Map.of(property, arcWith(nodes));
    return nodeData;
  }

  private static V2NodeResponse.ArcData arcWith(List<V2NodeResponse.NodeInfo> nodes) {
    V2NodeResponse.ArcData arcData = new V2NodeResponse.ArcData();
    arcData.nodes = nodes;
    return arcData;
  }

  private static V2NodeResponse.NodeInfo nodeWithDcid(String dcid) {
    V2NodeResponse.NodeInfo nodeInfo = new V2NodeResponse.NodeInfo();
    nodeInfo.dcid = dcid;
    return nodeInfo;
  }

  private static V2NodeResponse.NodeInfo nodeWithValue(String value) {
    V2NodeResponse.NodeInfo nodeInfo = new V2NodeResponse.NodeInfo();
    nodeInfo.value = value;
    return nodeInfo;
  }
}
