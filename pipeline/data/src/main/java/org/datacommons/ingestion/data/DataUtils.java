package org.datacommons.ingestion.data;

import com.google.common.base.Joiner;
import java.nio.charset.StandardCharsets;

/** Util functions for the pipeline data model. */
public class DataUtils {

  // Standard FNV-1a 32-bit constants
  private static final int FNV_32_INIT = 0x811c9dc5;
  private static final int FNV_32_PRIME = 0x01000193;

  /**
   * Generates a consistent facet ID using the FNV-1a 32-bit hash algorithm.
   *
   * <p>This is designed to replicate the legacy Go facet ID generation implementation in Mixer's
   * GetFacetID function. See
   * https://github.com/datacommonsorg/mixer/blob/0618c1f3ef80703c98fc97f6c6c6e5cd3d7c13d3/internal/util/util.go#L497-L515
   *
   * @param builder The Observation builder containing the fields to hash.
   * @return A consistent facet ID string.
   */
  public static String generateFacetId(Observation.Builder builder) {
    // Only include fields that are set in hash.
    // This is so the hashes stay consistent if more fields are added.
    String s =
        Joiner.on("-")
            .join(
                builder.getImportName(),
                builder.getMeasurementMethod(),
                builder.getObservationPeriod(),
                builder.getScalingFactor(),
                builder.getUnit());
    if (builder.getIsDcAggregate()) {
      s += "-IsDcAggregate";
    }

    int hash = fnv1a32(s);

    // Go's fmt.Sprint on a uint32 treats it as unsigned.
    // We must do the same in Java to avoid negative string values.
    return Integer.toUnsignedString(hash);
  }

  /**
   * Computes the 32-bit FNV-1a hash of a string.
   *
   * <p>Note: Java does not provide a built-in FNV-1a implementation, so we implement it manually
   * here.
   *
   * @param data The input string to hash.
   * @return The FNV-1a 32-bit hash as an integer.
   */
  private static int fnv1a32(String data) {
    int hash = FNV_32_INIT;
    for (byte b : data.getBytes(StandardCharsets.UTF_8)) {
      hash ^= (b & 0xff); // Bitwise XOR with the unsigned byte value
      hash *= FNV_32_PRIME;
    }
    return hash;
  }
}
