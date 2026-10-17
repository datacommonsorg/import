package org.datacommons.tool;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.datacommons.proto.Mcf;
import org.datacommons.util.GraphUtils;
import org.datacommons.util.McfUtil;

/**
 * Writes resolved {@code StatVarObservation} MCF nodes to an Apache Avro file alongside the
 * generated MCF file in {@code genmcf}.
 */
public class AvroObservationWriter implements Closeable {
  public static final String SCHEMA_JSON =
      "{\n"
          + "  \"namespace\": \"org.datacommons.avro\",\n"
          + "  \"type\": \"record\",\n"
          + "  \"name\": \"StatVarObservationRecord\",\n"
          + "  \"fields\": [\n"
          + "    {\"name\": \"dcid\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"variableMeasured\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"observationAbout\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"observationDate\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"value\", \"type\": [\"null\", \"double\"], \"default\": null},\n"
          + "    {\"name\": \"valueStr\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"unit\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"measurementMethod\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"observationPeriod\", \"type\": \"string\", \"default\": \"\"},\n"
          + "    {\"name\": \"scalingFactor\", \"type\": \"string\", \"default\": \"\"}\n"
          + "  ]\n"
          + "}";

  public static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

  private final File outputFile;
  private DataFileWriter<GenericRecord> dataFileWriter = null;

  public AvroObservationWriter(File outputFile) {
    this.outputFile = outputFile;
  }

  public void writeGraph(Mcf.McfGraph graph) throws IOException {
    for (Map.Entry<String, Mcf.McfGraph.PropertyValues> entry : graph.getNodesMap().entrySet()) {
      Mcf.McfGraph.PropertyValues pv = entry.getValue();
      if (!GraphUtils.isObservation(pv)) {
        continue;
      }
      if (dataFileWriter == null) {
        GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(SCHEMA);
        dataFileWriter = new DataFileWriter<>(datumWriter);
        dataFileWriter.setCodec(CodecFactory.deflateCodec(6));
        dataFileWriter.create(SCHEMA, outputFile);
      }

      String dcid = GraphUtils.getPropVal(pv, "dcid");
      if (dcid.isEmpty()) {
        dcid = McfUtil.stripNamespace(entry.getKey());
      }
      String valueStr = GraphUtils.getPropVal(pv, "value");
      Double numericVal = null;
      if (!valueStr.isEmpty()) {
        try {
          numericVal = Double.parseDouble(valueStr);
        } catch (NumberFormatException ignored) {
          // Keep numericVal as null if non-numeric observation is allowed.
        }
      }

      GenericRecord record = new GenericData.Record(SCHEMA);
      record.put("dcid", dcid);
      record.put("variableMeasured", GraphUtils.getPropVal(pv, "variableMeasured"));
      record.put("observationAbout", GraphUtils.getPropVal(pv, "observationAbout"));
      record.put("observationDate", GraphUtils.getPropVal(pv, "observationDate"));
      record.put("value", numericVal);
      record.put("valueStr", valueStr);
      record.put("unit", GraphUtils.getPropVal(pv, "unit"));
      record.put("measurementMethod", GraphUtils.getPropVal(pv, "measurementMethod"));
      record.put("observationPeriod", GraphUtils.getPropVal(pv, "observationPeriod"));
      record.put("scalingFactor", GraphUtils.getPropVal(pv, "scalingFactor"));
      dataFileWriter.append(record);
    }
  }

  @Override
  public void close() throws IOException {
    if (dataFileWriter != null) {
      dataFileWriter.close();
    }
  }
}
