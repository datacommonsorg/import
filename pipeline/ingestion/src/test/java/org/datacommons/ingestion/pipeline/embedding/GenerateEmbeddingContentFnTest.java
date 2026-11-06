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

import com.google.cloud.spanner.Statement;
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
import org.datacommons.ingestion.pipeline.embedding.GenerateEmbeddingContentFn.PredicateObjectValue;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenerateEmbeddingContentFn}. */
@RunWith(JUnit4.class)
public class GenerateEmbeddingContentFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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

    NodeRecord statVarNode =
        new NodeRecord(
            "Count_Person", "", null, "Total Population", List.of("StatisticalVariable"));
    JsonObject statVarJson = GenerateEmbeddingContentFn.buildBaseJsonObject(statVarNode, spec);
    assertEquals("Count_Person", statVarJson.get("subject_id").getAsString());
    assertEquals("Total Population", statVarJson.get("name").getAsString());

    NodeRecord classNode = new NodeRecord("Farm", "", null, "Farm", List.of("Class"));
    JsonObject classJson = GenerateEmbeddingContentFn.buildBaseJsonObject(classNode, spec);
    assertFalse(classJson.has("subject_id"));
    assertEquals("Farm", classJson.get("name").getAsString());

    NodeRecord predOnlyNode =
        new NodeRecord("dc/1", "", null, "Pred Only", List.of("PredicateOnly"));
    JsonObject predOnlyJson = GenerateEmbeddingContentFn.buildBaseJsonObject(predOnlyNode, spec);
    assertEquals(0, predOnlyJson.size());
  }

  @Test
  public void testAggregatePropertiesBySubject() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "type_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Class", List.of("name", "description")),
            NodeFilterType.ENTITY_TYPES);

    NodeRecord farmNode = new NodeRecord("Farm", "", null, "Farm", List.of("Class"));
    NodeRecord countryNode = new NodeRecord("Country", "", null, "Country", List.of("Class"));

    List<PredicateObjectValue> predicateValues =
        List.of(
            new PredicateObjectValue("Farm", "description", "First sentence"),
            new PredicateObjectValue("Farm", "description", "Second sentence"),
            new PredicateObjectValue("Farm", "unconfiguredPredicate", "Ignored value"),
            new PredicateObjectValue("UnknownSubject", "description", "Ignored subject"));

    Map<String, Map<String, String>> propsBySubject =
        GenerateEmbeddingContentFn.aggregatePropertiesBySubject(
            List.of(farmNode, countryNode), predicateValues, spec);

    assertEquals(1, propsBySubject.size());
    assertEquals(
        Map.of("description", "First sentence. Second sentence"), propsBySubject.get("Farm"));
    assertNull(propsBySubject.get("Country"));
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

    NodeRecord candidate =
        new NodeRecord(
            "Count_Person", "", null, "Total Population", List.of("StatisticalVariable"));

    EmbeddingContent content =
        GenerateEmbeddingContentFn.buildEmbeddingContent(candidate, Map.of(), spec);

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
            NodeFilterType.ENTITY_TYPES);

    NodeRecord farmNode = new NodeRecord("Farm", "", null, "Farm", List.of("Class"));

    List<PredicateObjectValue> predicateValues =
        List.of(
            new PredicateObjectValue("Farm", "description", "First sentence"),
            new PredicateObjectValue("Farm", "description", "Second sentence"));

    Map<String, Map<String, String>> propsBySubject =
        GenerateEmbeddingContentFn.aggregatePropertiesBySubject(
            List.of(farmNode), predicateValues, spec);

    EmbeddingContent content =
        GenerateEmbeddingContentFn.buildEmbeddingContent(
            farmNode, propsBySubject.getOrDefault("Farm", Map.of()), spec);

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

    NodeRecord blankNameCandidate = new NodeRecord("dc/123", "", null, "   ", List.of("Entity"));
    assertNull(
        GenerateEmbeddingContentFn.buildEmbeddingContent(blankNameCandidate, Map.of(), spec));

    NodeRecord unmatchedTypeCandidate =
        new NodeRecord("dc/456", "", null, "Valid Name", List.of("UnconfiguredType"));
    assertNull(
        GenerateEmbeddingContentFn.buildEmbeddingContent(unmatchedTypeCandidate, Map.of(), spec));
  }

  @Test
  public void testBuildPredicateQueryStatement() {
    Statement stmt =
        GenerateEmbeddingContentFn.buildPredicateQueryStatement(
            "Edge", "Node", Set.of("Farm", "Country"), Set.of("description"));
    String sql = stmt.getSql();
    assertTrue(sql.contains("SELECT e.subject_id, e.predicate, o.value"));
    assertTrue(sql.contains("FROM Edge e JOIN Node o ON e.object_id = o.subject_id"));
    assertTrue(sql.contains("e.subject_id IN UNNEST(@subject_ids)"));
    assertTrue(sql.contains("e.predicate IN UNNEST(@predicates)"));
    assertTrue(sql.contains("ORDER BY e.subject_id, e.predicate, o.value"));
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

    NodeRecord matchingNode =
        new NodeRecord(
            "Count_Person", "", null, "Total Population", List.of("StatisticalVariable"));
    NodeRecord emptyNameNode =
        new NodeRecord("dc/empty", "", null, "", List.of("StatisticalVariable"));

    PCollection<NodeRecord> candidates = pipeline.apply(Create.of(matchingNode, emptyNameNode));
    PCollection<EmbeddingContent> contents =
        GenerateEmbeddingContentFn.generateEmbeddingContent(candidates, null, spec);

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
