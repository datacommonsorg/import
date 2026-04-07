package org.datacommons.util.jsonld;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;

/**
 * Parser for JSON-LD input files. Converts JSON-LD constructs into Data Commons McfGraph protobuf
 * representation.
 */
public class JsonLdParser {

  /**
   * Parses a JSON-LD input stream and returns an McfGraph.
   *
   * @param inputStream The input stream containing JSON-LD data.
   * @return The parsed McfGraph.
   * @throws IOException If there is an error reading the input.
   */
  public static McfGraph parse(InputStream inputStream) throws IOException {
    // TODO: Optimize memory footprint by replacing full-graph buffering with a streaming Jackson
    // JsonParser
    Object jsonObject = JsonUtils.fromInputStream(inputStream);
    JsonLdOptions options = new JsonLdOptions();

    // Enforce JSON-LD Canonical Expansion mapping per W3C 1.1 rules
    try {
      jsonObject = com.github.jsonldjava.core.JsonLdProcessor.expand(jsonObject, options);
    } catch (com.github.jsonldjava.core.JsonLdError e) {
      throw new IOException("JSON-LD expansion failed: " + e.getMessage(), e);
    }

    McfGraph.Builder graphBuilder = McfGraph.newBuilder();
    graphBuilder.setType(Mcf.McfType.INSTANCE_MCF);

    if (jsonObject instanceof java.util.List) {
      java.util.List<Object> list = (java.util.List<Object>) jsonObject;
      for (Object obj : list) {
        if (obj instanceof Map) {
          Map<String, Object> nodeMap = (Map<String, Object>) obj;
          parseNode(nodeMap, graphBuilder);
        }
      }
    }

    return graphBuilder.build();
  }

  private static void parseNode(Map<String, Object> nodeMap, McfGraph.Builder graphBuilder) {
    String id = (String) nodeMap.get("@id");
    if (id == null) {
      // Skip nodes without ID for now. Data Commons requires IDs for resolution.
      return;
    }

    McfGraph.PropertyValues.Builder nodeBuilder = McfGraph.PropertyValues.newBuilder();
    addProperty(nodeBuilder, "dcid", id, org.datacommons.proto.Mcf.ValueType.TEXT);

    for (Map.Entry<String, Object> entry : nodeMap.entrySet()) {
      String key = entry.getKey();
      if (key.contains("/")) {
        key = key.substring(key.lastIndexOf('/') + 1);
      }
      if (key.contains("#")) {
        key = key.substring(key.lastIndexOf('#') + 1);
      }
      Object value = entry.getValue();
      System.out.println("JSONLD_PARSER node " + id + ": " + key + " = " + value);

      if ("@id".equals(key)) {
        continue;
      }

      if ("@type".equals(key)) {
        // Map @type to typeOf
        if (value instanceof List) {
          for (Object typeObj : (List<?>) value) {
            addProperty(nodeBuilder, "typeOf", typeObj.toString(), Mcf.ValueType.RESOLVED_REF);
          }
        } else if (value != null) {
          addProperty(nodeBuilder, "typeOf", value.toString(), Mcf.ValueType.RESOLVED_REF);
        }
        continue;
      }

      // Process other properties
      if (value instanceof List) {
        for (Object item : (List<?>) value) {
          processValueItem(nodeBuilder, key, item);
        }
      } else if (value != null) {
        processValueItem(nodeBuilder, key, value);
      }
    }

    graphBuilder.putNodes(id, nodeBuilder.build());
  }

  private static void processValueItem(
      McfGraph.PropertyValues.Builder nodeBuilder, String property, Object item) {
    if (item instanceof Map) {
      Map<?, ?> map = (Map<?, ?>) item;
      if (map.containsKey("@value")) {
        Object val = map.get("@value");
        addProperty(nodeBuilder, property, val.toString(), Mcf.ValueType.TEXT);
      } else if (map.containsKey("@id")) {
        Object idVal = map.get("@id");
        String idStr = idVal.toString();
        Mcf.ValueType type =
            idStr.startsWith("l:") ? Mcf.ValueType.UNRESOLVED_REF : Mcf.ValueType.RESOLVED_REF;
        addProperty(nodeBuilder, property, idStr, type);
      } else {
        System.err.println(
            "WARNING: Ignoring unsupported JSON-LD object for property " + property + ": " + map);
      }
    } else if (item != null) {
      // Fallback for simple values if JSON-LD processor didn't fully expand/flatten as expected
      addProperty(nodeBuilder, property, item.toString(), Mcf.ValueType.TEXT);
    }
  }

  private static void addProperty(
      McfGraph.PropertyValues.Builder nodeBuilder,
      String property,
      String value,
      Mcf.ValueType type) {
    McfGraph.Values.Builder valuesBuilder =
        nodeBuilder.getPvsOrDefault(property, McfGraph.Values.getDefaultInstance()).toBuilder();

    McfGraph.TypedValue.Builder typedValueBuilder =
        McfGraph.TypedValue.newBuilder().setValue(value).setType(type);

    valuesBuilder.addTypedValues(typedValueBuilder.build());
    nodeBuilder.putPvs(property, valuesBuilder.build());
  }
}
