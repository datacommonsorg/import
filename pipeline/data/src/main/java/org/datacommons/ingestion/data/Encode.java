package org.datacommons.ingestion.data;

import com.google.common.hash.Hashing;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class Encode {

  /**
   * Generates Base64-encoded SHA256 of input.
   *
   * @param input The input string to encode.
   * @return The encoded string.
   */
  public static String generateSha256(String input) {
    if (input == null || input.isEmpty()) {
      return "";
    }
    return Base64.getEncoder()
        .encodeToString(Hashing.sha256().hashString(input, StandardCharsets.UTF_8).asBytes());
  }
}
