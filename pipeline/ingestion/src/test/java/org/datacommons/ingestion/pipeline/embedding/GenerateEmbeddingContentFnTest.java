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
import org.datacommons.util.McfUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenerateEmbeddingContentFn}. */
@RunWith(JUnit4.class)
public class GenerateEmbeddingContentFnTest {

  private static final McfGraph STAT_VAR_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "Count_Person",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "Count_Person"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Total Population"))
                  .putPvs(
                      "typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "StatisticalVariable"))
                  .build())
          .build();

  private static final McfGraph FARM_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "Farm",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "Farm"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Farm"))
                  .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "Class"))
                  .putPvs(
                      "description",
                      Values.newBuilder()
                          .addTypedValues(
                              TypedValue.newBuilder()
                                  .setType(ValueType.TEXT)
                                  .setValue("Second sentence")
                                  .build())
                          .addTypedValues(
                              TypedValue.newBuilder()
                                  .setType(ValueType.TEXT)
                                  .setValue("First sentence")
                                  .build())
                          .build())
                  .build())
          .build();

  private static final McfGraph BLANK_NAME_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "dc/123",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "dc/123"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "   "))
                  .putPvs(
                      "typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "StatisticalVariable"))
                  .build())
          .build();

  private static final McfGraph UNMATCHED_TYPE_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "dc/456",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "dc/456"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Valid Name"))
                  .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "UnconfiguredType"))
                  .build())
          .build();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testGetEmbeddingNodeFields() {
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
        GenerateEmbeddingContentFn.getEmbeddingNodeFields(List.of("StatisticalVariable"), spec));
    assertEquals(
        Set.of("subject_id", "name", "description"),
        GenerateEmbeddingContentFn.getEmbeddingNodeFields(
            List.of("StatisticalVariable", "Class"), spec));
    assertTrue(
        GenerateEmbeddingContentFn.getEmbeddingNodeFields(List.of("UnknownType"), spec).isEmpty());
    assertTrue(GenerateEmbeddingContentFn.getEmbeddingNodeFields(List.of(), spec).isEmpty());
    assertTrue(GenerateEmbeddingContentFn.getEmbeddingNodeFields(null, spec).isEmpty());
  }

  @Test
  public void testGetEmbeddingPredicates() {
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
        GenerateEmbeddingContentFn.getEmbeddingPredicates(List.of("StatisticalVariable"), spec)
            .isEmpty());
    assertEquals(
        Set.of("description", "alternateName"),
        GenerateEmbeddingContentFn.getEmbeddingPredicates(List.of("Class"), spec));
  }

  @Test
  public void testBuildBaseContentMap() {
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

    Map<String, String> statVarMap =
        GenerateEmbeddingContentFn.buildBaseContentMap(
            "Count_Person", "Total Population", List.of("StatisticalVariable"), spec);
    assertEquals("Count_Person", statVarMap.get("subject_id"));
    assertEquals("Total Population", statVarMap.get("name"));

    Map<String, String> classMap =
        GenerateEmbeddingContentFn.buildBaseContentMap("Farm", "Farm", List.of("Class"), spec);
    assertFalse(classMap.containsKey("subject_id"));
    assertEquals("Farm", classMap.get("name"));

    Map<String, String> predOnlyMap =
        GenerateEmbeddingContentFn.buildBaseContentMap(
            "dc/1", "Pred Only", List.of("PredicateOnly"), spec);
    assertEquals(0, predOnlyMap.size());
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

    PropertyValues pvs = FARM_GRAPH.getNodesMap().get("Farm");

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

    McfGraph longFarmGraph =
        McfGraph.newBuilder()
            .putNodes(
                "Farm",
                PropertyValues.newBuilder()
                    .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "Farm"))
                    .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Farm"))
                    .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "Class"))
                    .putPvs("description", McfUtil.newValues(ValueType.TEXT, "a".repeat(10000)))
                    .build())
            .build();

    EmbeddingContent content =
        GenerateEmbeddingContentFn.buildEmbeddingContent(longFarmGraph, spec);
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

    EmbeddingContent content =
        GenerateEmbeddingContentFn.buildEmbeddingContent(STAT_VAR_GRAPH, spec);

    assertNotNull(content);
    assertEquals("Count_Person", content.subjectId());
    assertEquals("base_text_embedding", content.embeddingLabel());
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

    EmbeddingContent content = GenerateEmbeddingContentFn.buildEmbeddingContent(FARM_GRAPH, spec);

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
            Map.of("StatisticalVariable", List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);

    assertNull(GenerateEmbeddingContentFn.buildEmbeddingContent(BLANK_NAME_GRAPH, spec));
    assertNull(GenerateEmbeddingContentFn.buildEmbeddingContent(UNMATCHED_TYPE_GRAPH, spec));
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

    PCollection<McfGraph> candidates = pipeline.apply(Create.of(STAT_VAR_GRAPH, BLANK_NAME_GRAPH));
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
                List.of("StatisticalVariable")));
    pipeline.run();
  }
}
