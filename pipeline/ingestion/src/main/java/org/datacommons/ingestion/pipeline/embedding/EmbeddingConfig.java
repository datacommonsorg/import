// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.datacommons.ingestion.pipeline.embedding;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/** Global configuration for embedding ingestion, parsed from a JSON configuration file. */
public record EmbeddingConfig(List<EmbeddingSpec> specs) implements Serializable {

  private static final Gson GSON =
      new GsonBuilder()
          .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
          .create();

  public EmbeddingConfig {
    if (specs == null || specs.isEmpty()) {
      throw new IllegalArgumentException("specs must not be empty");
    }
    if (specs.contains(null)) {
      throw new IllegalArgumentException("specs must not contain null elements");
    }
  }

  /**
   * Loads {@link EmbeddingConfig} from a packaged classpath resource (e.g., {@code
   * embedding/default.json} in the bundled Dataflow JAR).
   */
  public static EmbeddingConfig load(String configPath) {
    if (configPath == null || configPath.trim().isEmpty()) {
      throw new IllegalArgumentException("embeddingConfigPath must not be empty");
    }
    String trimmed = configPath.trim();
    String resourceName = trimmed.startsWith("/") ? trimmed.substring(1) : trimmed;
    try (InputStream is =
        EmbeddingConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
      if (is == null) {
        throw new IllegalArgumentException("Embedding configuration file not found: " + configPath);
      }
      return parseJson(new String(is.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read embedding configuration from: " + configPath, e);
    }
  }

  /** Parses JSON content into an {@link EmbeddingConfig}. */
  public static EmbeddingConfig parseJson(String jsonContent) {
    if (jsonContent == null || jsonContent.trim().isEmpty()) {
      throw new IllegalArgumentException("Embedding JSON configuration must not be empty");
    }
    EmbeddingConfig config = GSON.fromJson(jsonContent, EmbeddingConfig.class);
    if (config == null) {
      throw new IllegalArgumentException("Embedding JSON configuration must not be empty");
    }
    return config;
  }
}
