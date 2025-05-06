package org.datacommons.ingestion.data;

import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/** Utility functions related to protobuf messages. */
public final class ProtoUtil {
  /** Parses a Base64-encoded, GZIP-compressed BT cache protobuf message from a cache row string. */
  public static <T extends Message> T parseCacheProto(String value, Parser<T> parser) {
    try (GZIPInputStream gzipInputStream =
        new GZIPInputStream(new ByteArrayInputStream(Base64.getDecoder().decode(value)))) {
      return parser.parseFrom(gzipInputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error parsing protobuf message: " + e.getMessage(), e);
    }
  }

  /**
   * Compresses the specified proto to be stored in spanner as gzipped bytes.
   *
   * <p>Note that we don't Base64 encode them like we do in BT since base64 encoding almost triples
   * the size.
   */
  public static byte[] compressProto(Message proto) {
    try {
      var out = new ByteArrayOutputStream();
      try (GZIPOutputStream gout = new GZIPOutputStream(out)) {
        gout.write(proto.toByteArray());
      }
      return out.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Error serializing proto: " + e.getMessage(), e);
    }
  }
}
