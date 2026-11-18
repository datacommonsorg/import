package org.datacommons.ingestion.timeseries;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.datacommons.Storage.Observations;
import org.junit.Test;

public class SourceObservationRowsTest {
  @Test
  public void toObservationRow_expandsObservationProtoFromStruct() {
    Struct row =
        Struct.newBuilder()
            .set("observation_about")
            .to("geoId/06")
            .set("variable_measured")
            .to("Count_Person")
            .set("facet_id")
            .to("123")
            .set("observation_period")
            .to((String) null)
            .set("measurement_method")
            .to((String) null)
            .set("unit")
            .to((String) null)
            .set("scaling_factor")
            .to((String) null)
            .set("import_name")
            .to("TestImport")
            .set("provenance_url")
            .to((String) null)
            .set("is_dc_aggregate")
            .to(false)
            .set("provenance")
            .to("dc/base/WrongImport")
            .set("observations")
            .to(
                ByteArray.copyFrom(
                    Observations.newBuilder()
                        .putValues("2023", "1")
                        .putValues("2024", "2")
                        .build()
                        .toByteArray()))
            .build();

    SourceObservationRow observationRow = SourceObservationRows.toObservationRow(row);
    List<SourcePointRow> rows = observationRow.pointRows();
    rows.sort(Comparator.comparing(SourcePointRow::date));

    assertEquals("geoId/06", observationRow.seriesRow().observationAbout());
    assertEquals("Count_Person", observationRow.seriesRow().variableMeasured());
    assertEquals("dc/base/TestImport", observationRow.seriesRow().provenance());
    assertEquals(2, rows.size());
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2023", "1"), rows.get(0));
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2024", "2"), rows.get(1));
  }

  @Test
  public void toObservationRow_expandsObservationProtoFromGenericRecord() {
    GenericRecord row = new GenericData.Record(schemaWithoutGeneratedProvenance());
    row.put("observation_about", "geoId/06");
    row.put("variable_measured", "Count_Person");
    row.put("facet_id", "123");
    row.put("observation_period", null);
    row.put("measurement_method", null);
    row.put("unit", null);
    row.put("scaling_factor", null);
    row.put("import_name", "TestImport");
    row.put("provenance_url", "https://example.com");
    row.put("is_dc_aggregate", true);
    row.put(
        "observations",
        ByteBuffer.wrap(
            Observations.newBuilder()
                .putValues("2023", "1")
                .putValues("2024", "2")
                .build()
                .toByteArray()));

    SourceObservationRow observationRow = SourceObservationRows.toObservationRow(row);
    List<SourcePointRow> rows = observationRow.pointRows();
    rows.sort(Comparator.comparing(SourcePointRow::date));

    assertEquals("geoId/06", observationRow.seriesRow().observationAbout());
    assertEquals("Count_Person", observationRow.seriesRow().variableMeasured());
    assertEquals("dc/base/TestImport", observationRow.seriesRow().provenance());
    assertEquals("https://example.com", observationRow.seriesRow().provenanceUrl());
    assertEquals(2, rows.size());
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2023", "1"), rows.get(0));
    assertEquals(new SourcePointRow("geoId/06", "Count_Person", "123", "2024", "2"), rows.get(1));
  }

  @Test
  public void toCompactObservationRow_keepsSeriesMetadataAndRawProtoBytes() {
    byte[] observationBytes =
        Observations.newBuilder()
            .putValues("2023", "1")
            .putValues("2024", "2")
            .build()
            .toByteArray();
    GenericRecord row = new GenericData.Record(schemaWithoutGeneratedProvenance());
    row.put("observation_about", "geoId/06");
    row.put("variable_measured", "Count_Person");
    row.put("facet_id", "123");
    row.put("observation_period", null);
    row.put("measurement_method", null);
    row.put("unit", null);
    row.put("scaling_factor", null);
    row.put("import_name", "TestImport");
    row.put("provenance_url", "https://example.com");
    row.put("is_dc_aggregate", true);
    row.put("observations", ByteBuffer.wrap(observationBytes));

    CompactSourceObservationRow observationRow = SourceObservationRows.toCompactObservationRow(row);

    assertEquals("geoId/06", observationRow.seriesRow().observationAbout());
    assertEquals("Count_Person", observationRow.seriesRow().variableMeasured());
    assertEquals("dc/base/TestImport", observationRow.seriesRow().provenance());
    assertEquals("https://example.com", observationRow.seriesRow().provenanceUrl());
    assertArrayEquals(observationBytes, observationRow.observationsProtoBytes());
  }

  private static Schema schemaWithoutGeneratedProvenance() {
    return new Schema.Parser()
        .parse(
            """
            {
              "type": "record",
              "name": "Observation",
              "fields": [
                {"name": "observation_about", "type": "string"},
                {"name": "variable_measured", "type": "string"},
                {"name": "facet_id", "type": "string"},
                {"name": "observation_period", "type": ["null", "string"], "default": null},
                {"name": "measurement_method", "type": ["null", "string"], "default": null},
                {"name": "unit", "type": ["null", "string"], "default": null},
                {"name": "scaling_factor", "type": ["null", "string"], "default": null},
                {"name": "import_name", "type": "string"},
                {"name": "provenance_url", "type": ["null", "string"], "default": null},
                {"name": "is_dc_aggregate", "type": ["null", "boolean"], "default": null},
                {"name": "observations", "type": "bytes"}
              ]
            }
            """);
  }
}
