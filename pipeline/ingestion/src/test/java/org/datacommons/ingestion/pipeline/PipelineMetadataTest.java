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

package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Guardrail tests ensuring that Dataflow Flex Template JSON metadata files (metadata.json and
 * rollback-metadata.json) stay strictly synchronized with Java PipelineOptions interfaces and with
 * each other on shared parameters.
 */
@RunWith(JUnit4.class)
public class PipelineMetadataTest {

  private static Path resolveMetadataFile(String filename) {
    Path localPath = Paths.get(filename);
    if (Files.exists(localPath)) {
      return localPath;
    }
    Path fromRootPath = Paths.get("pipeline", "ingestion", filename);
    if (Files.exists(fromRootPath)) {
      return fromRootPath;
    }
    throw new IllegalStateException("Could not locate metadata file: " + filename);
  }

  private static Map<String, JsonObject> parseMetadataParameters(String filename)
      throws IOException {
    Path path = resolveMetadataFile(filename);
    String content = Files.readString(path);
    JsonObject root = JsonParser.parseString(content).getAsJsonObject();
    JsonArray params = root.getAsJsonArray("parameters");

    Map<String, JsonObject> paramMap = new HashMap<>();
    for (JsonElement element : params) {
      JsonObject obj = element.getAsJsonObject();
      String name = obj.get("name").getAsString();
      paramMap.put(name, obj);
    }
    return paramMap;
  }

  private static Set<String> extractDeclaredOptionNames(Class<?>... optionInterfaces) {
    Set<String> optionNames = new TreeSet<>();
    for (Class<?> iface : optionInterfaces) {
      for (Method method : iface.getDeclaredMethods()) {
        String methodName = method.getName();
        if (methodName.startsWith("get")
            && methodName.length() > 3
            && method.getParameterCount() == 0
            && !Void.TYPE.equals(method.getReturnType())) {
          String prop = methodName.substring(3);
          String optionName = Character.toLowerCase(prop.charAt(0)) + prop.substring(1);
          optionNames.add(optionName);
        }
      }
    }
    return optionNames;
  }

  private static boolean isOptional(JsonObject paramObj) {
    if (!paramObj.has("isOptional")) {
      return false;
    }
    return paramObj.get("isOptional").getAsBoolean();
  }

  @Test
  public void testIngestionMetadataMatchesJavaOptions() throws IOException {
    Set<String> expectedJavaOptions =
        extractDeclaredOptionNames(SpannerPipelineOptions.class, IngestionPipelineOptions.class);
    Map<String, JsonObject> metadataParams = parseMetadataParameters("metadata.json");
    Set<String> actualJsonParams = new TreeSet<>(metadataParams.keySet());

    assertEquals(
        "metadata.json parameters must strictly match getters in SpannerPipelineOptions + IngestionPipelineOptions",
        expectedJavaOptions,
        actualJsonParams);
  }

  @Test
  public void testRollbackMetadataMatchesJavaOptions() throws IOException {
    Set<String> expectedJavaOptions =
        extractDeclaredOptionNames(SpannerPipelineOptions.class, RollbackPipelineOptions.class);
    Map<String, JsonObject> rollbackMetadataParams =
        parseMetadataParameters("rollback-metadata.json");
    Set<String> actualJsonParams = new TreeSet<>(rollbackMetadataParams.keySet());

    assertEquals(
        "rollback-metadata.json parameters must strictly match getters in SpannerPipelineOptions + RollbackPipelineOptions",
        expectedJavaOptions,
        actualJsonParams);
  }

  @Test
  public void testSharedParametersAreConsistentAcrossMetadataFiles() throws IOException {
    Set<String> sharedJavaOptions = extractDeclaredOptionNames(SpannerPipelineOptions.class);
    Map<String, JsonObject> ingestionParams = parseMetadataParameters("metadata.json");
    Map<String, JsonObject> rollbackParams = parseMetadataParameters("rollback-metadata.json");

    for (String sharedOption : sharedJavaOptions) {
      JsonObject ingObj = ingestionParams.get(sharedOption);
      JsonObject rollObj = rollbackParams.get(sharedOption);

      assertNotNull("Missing shared option in metadata.json: " + sharedOption, ingObj);
      assertNotNull("Missing shared option in rollback-metadata.json: " + sharedOption, rollObj);

      assertEquals(
          "Mismatched paramType for shared option: " + sharedOption,
          ingObj.get("paramType").getAsString(),
          rollObj.get("paramType").getAsString());

      assertEquals(
          "Mismatched isOptional flag for shared option: " + sharedOption,
          isOptional(ingObj),
          isOptional(rollObj));
    }
  }

  @Test
  public void testRequiredParametersEnforced() throws IOException {
    Map<String, JsonObject> ingestionParams = parseMetadataParameters("metadata.json");
    Map<String, JsonObject> rollbackParams = parseMetadataParameters("rollback-metadata.json");

    assertFalse(
        "importList must be required in metadata.json",
        isOptional(ingestionParams.get("importList")));
    assertFalse(
        "importList must be required in rollback-metadata.json",
        isOptional(rollbackParams.get("importList")));
    assertFalse(
        "rollbackTimestamp must be required in rollback-metadata.json",
        isOptional(rollbackParams.get("rollbackTimestamp")));
  }
}
