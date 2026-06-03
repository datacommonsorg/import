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
}
