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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.google.common.hash.Hashing;
import com.google.gson.JsonObject;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.McfGraph.TypedValue;
import org.datacommons.proto.Mcf.McfGraph.Values;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.util.GraphUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenerateEmbeddingContentFn}. */
@RunWith(JUnit4.class)
public class GenerateEmbeddingContentFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static McfGraph buildMcfNode(
      String subjectId, String name, String type, List<String> descriptions) {
    PropertyValues.Builder pvs = PropertyValues.newBuilder();
    GraphUtils.setPropVal("dcid", ValueType.TEXT, subjectId, pvs);
    if (name != null) {
      GraphUtils.setPropVal("name", ValueType.TEXT, name, pvs);
    }
    if (type != null) {
      GraphUtils.setPropVal("typeOf", ValueType.RESOLVED_REF, type, pvs);
    }
    if (descriptions != null && !descriptions.isEmpty()) {
      Values.Builder valuesBuilder = Values.newBuilder();
      for (String desc : descriptions) {
        valuesBuilder.addTypedValues(
            TypedValue.newBuilder().setType(ValueType.TEXT).setValue(desc).build());
      }
      pvs.putPvs("description", valuesBuilder.build());
    }
    return McfGraph.newBuilder().putNodes(subjectId, pvs.build()).build();
  }

  @Test
  public void testGetAllowedFieldsForNode() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "test_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of(
                "StatisticalVariable",
                List.of("subject_id", "name"),
                "Class",
                List.of("name", "description")),
            NodeFilterType.NO_FILTER);

    assertEquals(
        Set.of("subject_id", "name"),
        GenerateEmbeddingContentFn.getAllowedFieldsForNode(List.of("StatisticalVariable"), spec));
    assertEquals(
        Set.of("subject_id", "name", "description"),
        GenerateEmbeddingContentFn.getAllowedFieldsForNode(
            List.of("StatisticalVariable", "Class"), spec));
    assertTrue(
        GenerateEmbeddingContentFn.getAllowedFieldsForNode(List.of("UnknownType"), spec).isEmpty());
    assertTrue(GenerateEmbeddingContentFn.getAllowedFieldsForNode(List.of(), spec).isEmpty());
    assertTrue(GenerateEmbeddingContentFn.getAllowedFieldsForNode(null, spec).isEmpty());
  }

  @Test
  public void testGetAllowedPredicatesForNode() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "test_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of(
                "StatisticalVariable",
                List.of("subject_id", "name"),
                "Class",
                List.of("name", "description", "alternateName")),
            NodeFilterType.NO_FILTER);

    assertTrue(
        GenerateEmbeddingContentFn.getAllowedPredicatesForNode(List.of("StatisticalVariable"), spec)
            .isEmpty());
    assertEquals(
        Set.of("description", "alternateName"),
        GenerateEmbeddingContentFn.getAllowedPredicatesForNode(List.of("Class"), spec));
  }

  @Test
  public void testBuildBaseJsonObject() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "test_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of(
                "StatisticalVariable",
                List.of("subject_id", "name"),
                "Class",
                List.of("name", "description"),
                "PredicateOnly",
                List.of("description")),
            NodeFilterType.NO_FILTER);

    JsonObject statVarJson =
        GenerateEmbeddingContentFn.buildBaseJsonObject(
            "Count_Person", "Total Population", List.of("StatisticalVariable"), spec);
    assertEquals("Count_Person", statVarJson.get("subject_id").getAsString());
    assertEquals("Total Population", statVarJson.get("name").getAsString());

    JsonObject classJson =
        GenerateEmbeddingContentFn.buildBaseJsonObject("Farm", "Farm", List.of("Class"), spec);
    assertFalse(classJson.has("subject_id"));
    assertEquals("Farm", classJson.get("name").getAsString());

    JsonObject predOnlyJson =
        GenerateEmbeddingContentFn.buildBaseJsonObject(
            "dc/1", "Pred Only", List.of("PredicateOnly"), spec);
    assertEquals(0, predOnlyJson.size());
  }

  @Test
  public void testExtractPredicateProperties() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "type_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Class", List.of("name", "description")),
            NodeFilterType.NO_FILTER);

    McfGraph farmGraph =
        buildMcfNode("Farm", "Farm", "Class", List.of("Second sentence", "First sentence"));
    PropertyValues pvs = farmGraph.getNodesMap().get("Farm");

    Map<String, String> props =
        GenerateEmbeddingContentFn.extractPredicateProperties(pvs, List.of("Class"), spec);
    assertEquals(Map.of("description", "First sentence. Second sentence"), props);
  }

  @Test
  public void testBuildEmbeddingContent_capsTextToEmbedAtMaxCharLength() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "type_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Class", List.of("name", "description")),
            NodeFilterType.NO_FILTER);

    String longDesc = "a".repeat(10000);
    McfGraph farmGraph = buildMcfNode("Farm", "Farm", "Class", List.of(longDesc));

    EmbeddingContent content = GenerateEmbeddingContentFn.buildEmbeddingContent(farmGraph, spec);
    assertNotNull(content);
    assertEquals(GenerateEmbeddingContentFn.MAX_CHAR_LENGTH, content.textToEmbed().length());
    assertTrue(
        content.embeddingContentJson().length() > GenerateEmbeddingContentFn.MAX_CHAR_LENGTH);
  }

  @Test
  public void testComputeFarmFingerprintKey() {
    String input = "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}";
    long expected =
        Hashing.farmHashFingerprint64().hashString(input, StandardCharsets.UTF_8).asLong();
    assertEquals(
        Long.toString(expected), GenerateEmbeddingContentFn.computeFarmFingerprintKey(input));

    long expectedEmpty =
        Hashing.farmHashFingerprint64().hashString("", StandardCharsets.UTF_8).asLong();
    assertEquals(
        Long.toString(expectedEmpty), GenerateEmbeddingContentFn.computeFarmFingerprintKey(null));
  }

  @Test
  public void testBuildEmbeddingContent_noPredicates() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "VERTEX_AI",
            "gemini-embedding-001",
            "RETRIEVAL_DOCUMENT",
            Map.of(
                "StatisticalVariable",
                List.of("subject_id", "name"),
                "Entity",
                List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);

    McfGraph candidate =
        buildMcfNode("Count_Person", "Total Population", "StatisticalVariable", List.of());

    EmbeddingContent content = GenerateEmbeddingContentFn.buildEmbeddingContent(candidate, spec);

    assertNotNull(content);
    assertEquals("Count_Person", content.subjectId());
    assertEquals("base_text_embedding", content.embeddingLabel());
    assertEquals("gemini-embedding-001", content.modelEndpoint());
    assertEquals("RETRIEVAL_DOCUMENT", content.taskType());
    assertEquals(List.of("StatisticalVariable"), content.nodeTypes());

    String expectedJson = "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}";
    assertEquals(expectedJson, content.embeddingContentJson());
    assertEquals(expectedJson, content.textToEmbed());

    long expectedFingerprint =
        Hashing.farmHashFingerprint64().hashString(expectedJson, StandardCharsets.UTF_8).asLong();
    assertEquals(String.valueOf(expectedFingerprint), content.embeddingContentKey());
  }

  @Test
  public void testBuildEmbeddingContent_withPredicatesAndDeterministicOrder() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "type_embedding",
            "VERTEX_AI",
            "gemini-embedding-001",
            "RETRIEVAL_DOCUMENT",
            Map.of("Class", List.of("name", "description")),
            NodeFilterType.NO_FILTER);

    McfGraph farmGraph =
        buildMcfNode("Farm", "Farm", "Class", List.of("Second sentence", "First sentence"));

    EmbeddingContent content = GenerateEmbeddingContentFn.buildEmbeddingContent(farmGraph, spec);

    assertNotNull(content);
    String expectedJson = "{\"name\":\"Farm\",\"description\":\"First sentence. Second sentence\"}";
    assertEquals(expectedJson, content.embeddingContentJson());
    assertEquals(expectedJson, content.textToEmbed());

    long expectedFingerprint =
        Hashing.farmHashFingerprint64().hashString(expectedJson, StandardCharsets.UTF_8).asLong();
    assertEquals(String.valueOf(expectedFingerprint), content.embeddingContentKey());
  }

  @Test
  public void testBuildEmbeddingContent_blankNameOrUnmatchedTypeReturnsNull() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "VERTEX_AI",
            "gemini-embedding-001",
            "RETRIEVAL_DOCUMENT",
            Map.of("Entity", List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);

    McfGraph blankNameCandidate = buildMcfNode("dc/123", "   ", "Entity", List.of());
    assertNull(GenerateEmbeddingContentFn.buildEmbeddingContent(blankNameCandidate, spec));

    McfGraph unmatchedTypeCandidate =
        buildMcfNode("dc/456", "Valid Name", "UnconfiguredType", List.of());
    assertNull(GenerateEmbeddingContentFn.buildEmbeddingContent(unmatchedTypeCandidate, spec));
  }

  @Test
  public void testGenerateEmbeddingContent_pipeline() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "VERTEX_AI",
            "gemini-embedding-001",
            "RETRIEVAL_DOCUMENT",
            Map.of(
                "StatisticalVariable",
                List.of("subject_id", "name"),
                "Topic",
                List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);

    McfGraph matchingNode =
        buildMcfNode("Count_Person", "Total Population", "StatisticalVariable", List.of());
    McfGraph emptyNameNode = buildMcfNode("dc/empty", "", "StatisticalVariable", List.of());

    PCollection<McfGraph> candidates = pipeline.apply(Create.of(matchingNode, emptyNameNode));
    PCollection<EmbeddingContent> contents =
        GenerateEmbeddingContentFn.generateEmbeddingContent(candidates, spec);

    String expectedJson = "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}";
    long expectedFp =
        Hashing.farmHashFingerprint64().hashString(expectedJson, StandardCharsets.UTF_8).asLong();

    PAssert.that(contents)
        .containsInAnyOrder(
            new EmbeddingContent(
                "Count_Person",
                "base_text_embedding",
                String.valueOf(expectedFp),
                expectedJson,
                expectedJson,
                List.of("StatisticalVariable"),
                "gemini-embedding-001",
                "RETRIEVAL_DOCUMENT"));
    pipeline.run();
  }
}
