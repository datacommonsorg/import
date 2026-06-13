package org.datacommons.ingestion.timeseries;

import com.google.cloud.ByteArray;
import com.google.cloud.spanner.Struct;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.datacommons.Storage.Observations;

/** Row shapes used by the timeseries backfill. */
final class SourceObservationRows {
  private SourceObservationRows() {}

  static SourceObservationRow toObservationRow(Struct row) {
    return buildObservationRow(
        row.getString("observation_about"),
        row.getString("variable_measured"),
        row.getString("facet_id"),
        getNullableString(row, "observation_period"),
        getNullableString(row, "measurement_method"),
        getNullableString(row, "unit"),
        getNullableString(row, "scaling_factor"),
        getNullableString(row, "import_name"),
        getNullableString(row, "provenance_url"),
        !row.isNull("is_dc_aggregate") && row.getBoolean("is_dc_aggregate"),
        parseObservations(row));
  }

  static SourceObservationRow toObservationRow(GenericRecord row) {
    return buildObservationRow(
        getNullableString(row, "observation_about"),
        getNullableString(row, "variable_measured"),
        getNullableString(row, "facet_id"),
        getNullableString(row, "observation_period"),
        getNullableString(row, "measurement_method"),
        getNullableString(row, "unit"),
        getNullableString(row, "scaling_factor"),
        getNullableString(row, "import_name"),
        getNullableString(row, "provenance_url"),
        getNullableBoolean(row, "is_dc_aggregate"),
        parseObservations(row));
  }

  static CompactSourceObservationRow toCompactObservationRow(GenericRecord row) {
    return new CompactSourceObservationRow(
        new SourceSeriesRow(
            getNullableString(row, "observation_about"),
            getNullableString(row, "variable_measured"),
            getNullableString(row, "facet_id"),
            getNullableString(row, "observation_period"),
            getNullableString(row, "measurement_method"),
            getNullableString(row, "unit"),
            getNullableString(row, "scaling_factor"),
            getNullableString(row, "import_name"),
            getNullableString(row, "provenance_url"),
            getNullableBoolean(row, "is_dc_aggregate"),
            deriveProvenance(getNullableString(row, "import_name"))),
        getObservationBytes(row));
  }

  private static SourceObservationRow buildObservationRow(
      String observationAbout,
      String variableMeasured,
      String facetId,
      String observationPeriod,
      String measurementMethod,
      String unit,
      String scalingFactor,
      String importName,
      String provenanceUrl,
      boolean isDcAggregate,
      Observations observations) {
    SourceSeriesRow seriesRow =
        new SourceSeriesRow(
            observationAbout,
            variableMeasured,
            facetId,
            observationPeriod,
            measurementMethod,
            unit,
            scalingFactor,
            importName,
            provenanceUrl,
            isDcAggregate,
            deriveProvenance(importName));
    List<SourcePointRow> pointRows = new ArrayList<>();
    for (Map.Entry<String, String> entry : observations.getValuesMap().entrySet()) {
      pointRows.add(
          new SourcePointRow(
              observationAbout, variableMeasured, facetId, entry.getKey(), entry.getValue()));
    }
    return new SourceObservationRow(seriesRow, pointRows);
  }

  private static String getNullableString(Struct row, String columnName) {
    return row.isNull(columnName) ? "" : row.getString(columnName);
  }

  private static String getNullableString(GenericRecord row, String fieldName) {
    Object value = getField(row, fieldName);
    return value == null ? "" : value.toString();
  }

  private static boolean getNullableBoolean(GenericRecord row, String fieldName) {
    Object value = getField(row, fieldName);
    return value instanceof Boolean && (Boolean) value;
  }

  private static Observations parseObservations(Struct row) {
    if (row.isNull("observations")) {
      return Observations.getDefaultInstance();
    }
    ByteArray protoBytes = row.getBytes("observations");
    return parseObservations(protoBytes.toByteArray());
  }

  private static Observations parseObservations(GenericRecord row) {
    return parseObservations(getObservationBytes(row));
  }

  static Observations parseObservations(byte[] protoBytes) {
    if (protoBytes == null || protoBytes.length == 0) {
      return Observations.getDefaultInstance();
    }
    try {
      return Observations.parseFrom(protoBytes);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse observations proto", e);
    }
  }

  private static byte[] getObservationBytes(GenericRecord row) {
    Object value = getField(row, "observations");
    if (value == null) {
      return new byte[0];
    }
    return toByteArray(value);
  }

  private static byte[] toByteArray(Object value) {
    if (value instanceof ByteBuffer byteBuffer) {
      ByteBuffer duplicate = byteBuffer.duplicate();
      byte[] bytes = new byte[duplicate.remaining()];
      duplicate.get(bytes);
      return bytes;
    }
    if (value instanceof byte[] bytes) {
      return bytes;
    }
    if (value instanceof GenericData.Fixed fixed) {
      return fixed.bytes();
    }
    throw new IllegalArgumentException("Unsupported observations Avro type: " + value.getClass());
  }

  private static Object getField(GenericRecord row, String fieldName) {
    if (row.getSchema().getField(fieldName) == null) {
      return null;
    }
    return row.get(fieldName);
  }

  private static String deriveProvenance(String importName) {
    return importName == null || importName.isEmpty() ? "" : "dc/base/" + importName;
  }
}

record SourceObservationRow(SourceSeriesRow seriesRow, List<SourcePointRow> pointRows)
    implements Serializable {}

record CompactSourceObservationRow(SourceSeriesRow seriesRow, byte[] observationsProtoBytes)
    implements Serializable {}

record SourceSeriesRow(
    String observationAbout,
    String variableMeasured,
    String facetId,
    String observationPeriod,
    String measurementMethod,
    String unit,
    String scalingFactor,
    String importName,
    String provenanceUrl,
    boolean isDcAggregate,
    String provenance)
    implements Serializable {}

record SourcePointRow(
    String observationAbout, String variableMeasured, String facetId, String date, String value)
    implements Serializable {}
