package org.datacommons.ingestion.data;

import java.io.Serializable;
import java.util.Objects;

import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;

/**
 * Models an observation value at a specific date.
 */
@DefaultCoder(AvroCoder.class)
public class DateValue implements Serializable {
    private String date;
    private String value;

    public DateValue(String date, String value) {
        this.date = date;
        this.value = value;
    }

    public String getDate() {
        return date;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DateValue dateValue = (DateValue) o;
        return Objects.equals(date, dateValue.date) && Objects.equals(value, dateValue.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(date, value);
    }

    @Override
    public String toString() {
        return String.format("DateValue{date='%s', value='%s'}", date, value);
    }
}