package org.datacommons.ingestion.timeseries;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.io.Reader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;

/** Resolves Avro export files from either an export directory or an explicit file list. */
final class ObservationExportFiles {
  private ObservationExportFiles() {}

  static void validateOptions(TimeseriesBackfillOptions options) {
    boolean hasInputExportDir = !options.getInputExportDir().isEmpty();
    boolean hasInputFiles = !options.getInputFiles().isEmpty();
    if (hasInputExportDir == hasInputFiles) {
      throw new IllegalArgumentException(
          "Exactly one of inputExportDir or inputFiles must be provided for the Avro pipeline.");
    }
  }

  static List<String> resolveInputFiles(TimeseriesBackfillOptions options) {
    if (!options.getInputFiles().isEmpty()) {
      return parseCsv(options.getInputFiles());
    }
    return readManifest(
        options, options.getInputExportDir(), options.getSourceObservationTableName());
  }

  private static List<String> readManifest(
      TimeseriesBackfillOptions options, String exportDir, String sourceTableName) {
    FileSystems.setDefaultPipelineOptions(options);
    String normalizedExportDir = trimTrailingSlash(exportDir);
    String manifestPath = normalizedExportDir + "/" + sourceTableName + "-manifest.json";
    ResourceId manifestResource;
    try {
      manifestResource = FileSystems.matchSingleFileSpec(manifestPath).resourceId();
    } catch (IOException e) {
      throw new RuntimeException("Failed to find export manifest at " + manifestPath, e);
    }

    try (Reader reader =
        Channels.newReader(FileSystems.open(manifestResource), StandardCharsets.UTF_8.name())) {
      JsonElement root = JsonParser.parseReader(reader);
      List<String> files = parseManifest(root, normalizedExportDir, sourceTableName);
      if (!files.isEmpty()) {
        return files;
      }
      throw new IllegalArgumentException(
          "No Avro files for " + sourceTableName + " were found in " + manifestPath);
    } catch (IOException e) {
      throw new RuntimeException("Failed to read export manifest at " + manifestPath, e);
    }
  }

  static List<String> parseManifest(JsonElement root, String exportDir, String sourceTableName) {
    Pattern filePattern =
        Pattern.compile("(^|.*/)" + Pattern.quote(sourceTableName) + "\\.avro-\\d{5}-of-\\d{5}$");
    Set<String> files = new LinkedHashSet<>();
    collectFiles(root, exportDir, filePattern, files);
    return new ArrayList<>(files);
  }

  private static void collectFiles(
      JsonElement element, String exportDir, Pattern filePattern, Set<String> files) {
    if (element == null || element.isJsonNull()) {
      return;
    }
    if (element.isJsonPrimitive() && element.getAsJsonPrimitive().isString()) {
      String value = element.getAsString();
      if (filePattern.matcher(value).find()) {
        files.add(toAbsolutePath(exportDir, value));
      }
      return;
    }
    if (element.isJsonArray()) {
      for (JsonElement child : element.getAsJsonArray()) {
        collectFiles(child, exportDir, filePattern, files);
      }
      return;
    }
    JsonObject object = element.getAsJsonObject();
    for (String key : object.keySet()) {
      collectFiles(object.get(key), exportDir, filePattern, files);
    }
  }

  private static List<String> parseCsv(String csv) {
    List<String> fileSpecs = new ArrayList<>();
    for (String part : csv.split(",")) {
      String trimmed = part.trim();
      if (!trimmed.isEmpty()) {
        fileSpecs.add(trimmed);
      }
    }
    if (!fileSpecs.isEmpty()) {
      return fileSpecs;
    }
    throw new IllegalArgumentException("inputFiles must contain at least one Avro file path.");
  }

  private static String toAbsolutePath(String exportDir, String value) {
    if (value.contains("://") || value.startsWith("/")) {
      return value;
    }
    String relativePath = value.startsWith("./") ? value.substring(2) : value;
    while (relativePath.startsWith("/")) {
      relativePath = relativePath.substring(1);
    }
    return exportDir + "/" + relativePath;
  }

  private static String trimTrailingSlash(String value) {
    if (value.endsWith("/")) {
      return value.substring(0, value.length() - 1);
    }
    return value;
  }
}
