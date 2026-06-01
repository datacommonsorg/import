package org.datacommons.ingestion.data;

import java.io.Serializable;

/** Util functions for managing provenance prefixes. */
public class ProvenanceUtils implements Serializable {
  public static final String BASE_DC_PREFIX = "dc/base/";

  /**
   * Generates the provenance DCID based on import name and whether it is base DC.
   *
   * @param importName The name of the import.
   * @param isBaseDc Whether the run is base DC.
   * @return The constructed provenance DCID.
   */
  public static String getProvenanceDcid(String importName, boolean isBaseDc) {
    String prefix = isBaseDc ? BASE_DC_PREFIX : "";
    return prefix + importName;
  }

  /**
   * Strips the 'dc/base/' prefix from the given ID if the run is not base DC.
   *
   * @param value The value to strip.
   * @param isBaseDc Whether the run is base DC.
   * @return The stripped value if not base DC and starts with prefix; otherwise the original value.
   */
  public static String stripPrefix(String value, boolean isBaseDc) {
    if (!isBaseDc && value != null && value.startsWith(BASE_DC_PREFIX)) {
      return value.substring(BASE_DC_PREFIX.length());
    }
    return value;
  }
}
