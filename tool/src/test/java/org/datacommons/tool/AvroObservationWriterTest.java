package org.datacommons.tool;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.datacommons.proto.Mcf;
import org.datacommons.util.McfParser;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class AvroObservationWriterTest {
  @Rule public TemporaryFolder testFolder = new TemporaryFolder();

  @Test
  public void testWriteObservationGraphToAvro() throws IOException {
    String mcfStr =
        "Node: dcid:obs1\n"
            + "typeOf: dcs:StatVarObservation\n"
            + "variableMeasured: dcs:Count_Person_Male\n"
            + "observationAbout: dcid:geoId/06\n"
            + "observationDate: \"2020\"\n"
            + "value: 100.5\n"
            + "unit: dcs:Person\n"
            + "measurementMethod: dcs:CensusACS5yrSurvey\n"
            + "observationPeriod: \"P1Y\"\n"
            + "scalingFactor: \"1\"\n"
            + "\n"
            + "Node: dcid:nonObsPlace\n"
            + "typeOf: dcs:State\n"
            + "name: \"California\"\n";

    Mcf.McfGraph graph = McfParser.parseInstanceMcfString(mcfStr, false, null);
    File avroFile = new File(testFolder.getRoot(), "table_observations_test.avro");
    try (AvroObservationWriter writer = new AvroObservationWriter(avroFile)) {
      writer.writeGraph(graph);
    }

    assertTrue(avroFile.exists());
    List<GenericRecord> records = new ArrayList<>();
    GenericDatumReader<GenericRecord> datumReader =
        new GenericDatumReader<>(AvroObservationWriter.SCHEMA);
    try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(avroFile, datumReader)) {
      while (fileReader.hasNext()) {
        records.add(fileReader.next());
      }
    }

    assertEquals(1, records.size());
    GenericRecord rec = records.get(0);
    assertEquals("obs1", rec.get("dcid").toString());
    assertEquals("Count_Person_Male", rec.get("variableMeasured").toString());
    assertEquals("geoId/06", rec.get("observationAbout").toString());
    assertEquals("2020", rec.get("observationDate").toString());
    assertEquals(100.5, (Double) rec.get("value"), 1e-6);
    assertEquals("100.5", rec.get("valueStr").toString());
    assertEquals("Person", rec.get("unit").toString());
    assertEquals("CensusACS5yrSurvey", rec.get("measurementMethod").toString());
    assertEquals("P1Y", rec.get("observationPeriod").toString());
    assertEquals("1", rec.get("scalingFactor").toString());
  }
}
