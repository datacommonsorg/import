package org.datacommons.util.jsonld;

import com.github.jsonldjava.core.JsonLdOptions;
import com.github.jsonldjava.core.JsonLdProcessor;
import com.github.jsonldjava.utils.JsonUtils;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.datacommons.proto.Mcf;
import org.datacommons.proto.Mcf.McfGraph;

/**
 * Parser for JSON-LD Templates mapped to CSV files. Reads a .tmpl.jsonld template and a CSV file,
 * generating McfGraph protos.
 */
public class JsonLdTemplateParser {

  private static final String MAPPING_COLUMN_SOURCE = "http://datacommons.org/mapping/ColumnSource";
  private static final String MAPPING_COLUMN_HEADER = "http://datacommons.org/mapping/columnHeader";

  private final List<Map<String, Object>> flattenedTemplate;
  private CSVParser csvParser;
  private java.util.Iterator<CSVRecord> csvIterator;
  private int rowNum = 0;

  /**
   * Initializes the parser with a JSON-LD template.
   *
   * @param templateStream The input stream containing the JSON-LD template.
   * @throws IOException If there is an error reading the template.
   */
  public JsonLdTemplateParser(InputStream templateStream) throws IOException {
    Object templateJson = JsonUtils.fromInputStream(templateStream);
    JsonLdOptions options = new JsonLdOptions();
    Object flattened = JsonLdProcessor.expand(templateJson);

    this.flattenedTemplate = new ArrayList<>();
    if (flattened instanceof List) {
      for (Object obj : (List<?>) flattened) {
        if (obj instanceof Map) {
          @SuppressWarnings("unchecked")
          Map<String, Object> map = (Map<String, Object>) obj;
          flattenedTemplate.add(map);
        }
      }
    }
  }

  /**
   * Initializes the CSV parser with a reader.
   *
   * @param csvReader Reader for the CSV file.
   * @throws IOException If there is an error reading the CSV.
   */
  public void initCsv(Reader csvReader) throws IOException {
    this.csvParser = CSVFormat.DEFAULT.withFirstRecordAsHeader().parse(csvReader);
    this.csvIterator = csvParser.iterator();
    this.rowNum = 0;
  }

  /**
   * Parses the next row of the CSV and returns the generated McfGraph.
   *
   * @return The generated McfGraph, or null if no more rows.
   */
  public McfGraph parseNextRow() {
    if (csvIterator != null && csvIterator.hasNext()) {
      CSVRecord record = csvIterator.next();
      McfGraph.Builder graphBuilder = McfGraph.newBuilder();
      graphBuilder.setType(Mcf.McfType.INSTANCE_MCF);
      processRow(record, graphBuilder, rowNum++);
      return graphBuilder.build();
    }
    return null;
  }

  private void processRow(CSVRecord record, McfGraph.Builder graphBuilder, int rowNum) {
    for (Map<String, Object> nodeTemplate : flattenedTemplate) {
      String idTemplate = (String) nodeTemplate.get("@id");
      if (idTemplate == null) continue;

      // Generate a unique ID for this row if it's a local ID template
      String actualId = resolveId(idTemplate, record, rowNum);

      McfGraph.PropertyValues.Builder nodeBuilder = McfGraph.PropertyValues.newBuilder();

      for (Map.Entry<String, Object> entry : nodeTemplate.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        if ("@id".equals(key)) continue;

        if ("@type".equals(key)) {
          if (value instanceof List) {
            for (Object typeObj : (List<?>) value) {
              addProperty(nodeBuilder, "typeOf", typeObj.toString(), Mcf.ValueType.RESOLVED_REF);
            }
          }
          continue;
        }

        processProperty(nodeBuilder, key, value, record);
      }

      graphBuilder.putNodes(actualId, nodeBuilder.build());
    }
  }

  private String resolveId(String idTemplate, CSVRecord record, int rowNum) {
    // Simplified: append row number to make it unique if it looks like a local ID
    if (idTemplate.startsWith("l:")) {
      return idTemplate + "_" + rowNum;
    }
    return idTemplate;
  }

  private void processProperty(
      McfGraph.PropertyValues.Builder nodeBuilder,
      String property,
      Object valueTemplate,
      CSVRecord record) {
    if (valueTemplate instanceof List) {
      for (Object item : (List<?>) valueTemplate) {
        if (item instanceof Map) {
          Map<?, ?> map = (Map<?, ?>) item;

          // Check if it is a ColumnSource mapping
          if (map.containsKey("@type")) {
            Object type = map.get("@type");
            if (MAPPING_COLUMN_SOURCE.equals(type)
                || (type instanceof List && ((List<?>) type).contains(MAPPING_COLUMN_SOURCE))) {

              Object headerObj = map.get(MAPPING_COLUMN_HEADER);
              if (headerObj instanceof List && !((List<?>) headerObj).isEmpty()) {
                Object firstHeader = ((List<?>) headerObj).get(0);
                if (firstHeader instanceof Map) {
                  String columnName = (String) ((Map<?, ?>) firstHeader).get("@value");
                  if (columnName != null && record.isMapped(columnName)) {
                    String csvValue = record.get(columnName);
                    if (csvValue != null && !csvValue.isEmpty()) {
                      addProperty(nodeBuilder, property, csvValue, Mcf.ValueType.TEXT);
                    }
                  }
                }
              }
              continue;
            }
          }

          // Fallback to static value if not a ColumnSource
          if (map.containsKey("@value")) {
            addProperty(nodeBuilder, property, map.get("@value").toString(), Mcf.ValueType.TEXT);
          } else if (map.containsKey("@id")) {
            String idStr = map.get("@id").toString();
            Mcf.ValueType type =
                idStr.startsWith("l:") ? Mcf.ValueType.UNRESOLVED_REF : Mcf.ValueType.RESOLVED_REF;
            addProperty(nodeBuilder, property, idStr, type);
          }
        }
      }
    }
  }

  private void addProperty(
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
