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
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.Key;
import com.google.cloud.spanner.KeyRange;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.pipeline.GraphIngestionUtil;
import org.datacommons.ingestion.pipeline.MockSpannerClient;
import org.datacommons.ingestion.pipeline.MockVertexAIClient;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.ingestion.spanner.SpannerClient;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link SpannerEmbeddingPipeline}. */
@RunWith(JUnit4.class)
public class SpannerEmbeddingPipelineTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    MockSpannerClient.clearCapturedMutations();
    MockVertexAIClient.clearCapturedCalls();
  }

  @Test
  public void testAttachTransforms_runsDeleteBeforeContentAndWritesMutations() {
    EmbeddingConfig config = EmbeddingConfig.load("embedding/default.json");

    NodeSubGraph statVar =
        GraphIngestionUtil.subjectNodeSubGraph(
            "Count_Person", "Total Population", List.of("StatisticalVariable"));
    NodeSubGraph farmClass =
        GraphIngestionUtil.subjectNodeSubGraph(
            "Farm",
            "Farm",
            List.of("Class"),
            List.of(
                Edge.builder()
                    .subjectId("Farm")
                    .predicate("description")
                    .objectId("Agricultural land.")
                    .provenance("dc/base/Schema")
                    .build()));

    PCollection<NodeSubGraph> graphs =
        pipeline.apply("NodeSubGraphs", Create.of(statVar, farmClass));
    SpannerClient spannerClient =
        new MockSpannerClient(Map.of("dc/base/Schema", List.of("OldNode", "Count_Person")));

    SpannerEmbeddingPipeline.attachTransforms(
        pipeline,
        spannerClient,
        "testImport",
        "dc/base/Schema",
        config,
        new MockVertexAIClient(),
        "NodeEmbedding",
        10,
        graphs,
        null,
        false);

    pipeline.run().waitUntilFinish();

    List<Mutation> deletes =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(m -> m.getOperation() == Mutation.Op.DELETE)
            .collect(Collectors.toList());
    Set<KeyRange> deletedRanges =
        deletes.stream()
            .flatMap(m -> Lists.newArrayList(m.getKeySet().getRanges()).stream())
            .collect(Collectors.toSet());
    assertEquals(
        Set.of(
            KeyRange.prefix(Key.of("OldNode", "base_text_embedding")),
            KeyRange.prefix(Key.of("Count_Person", "base_text_embedding")),
            KeyRange.prefix(Key.of("OldNode", "type_embedding")),
            KeyRange.prefix(Key.of("Count_Person", "type_embedding"))),
        deletedRanges);

    List<Mutation> upserts =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(m -> m.getOperation() == Mutation.Op.INSERT_OR_UPDATE)
            .collect(Collectors.toList());
    assertEquals(2, upserts.size());
    Map<String, Mutation> byLabel =
        upserts.stream()
            .collect(Collectors.toMap(m -> m.asMap().get("embedding_label").getString(), m -> m));
    assertEquals(
        "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
        byLabel.get("base_text_embedding").asMap().get("embedding_content").getString());
    assertEquals(
        "{\"name\":\"Farm\",\"description\":\"Agricultural land.\"}",
        byLabel.get("type_embedding").asMap().get("embedding_content").getString());

    Set<String> predictedTexts =
        MockVertexAIClient.getCapturedCalls().stream()
            .flatMap(List::stream)
            .collect(Collectors.toSet());
    assertEquals(
        Set.of(
            "{\"subject_id\":\"Count_Person\",\"name\":\"Total Population\"}",
            "{\"name\":\"Farm\",\"description\":\"Agricultural land.\"}"),
        predictedTexts);

    // Confirm Wait.on ran delete stages strictly before write stages per spec.
    List<String> stageEvents = MockSpannerClient.getCapturedStageEvents();
    int baseDelIdx = stageEvents.indexOf("WriteDeletedEmbeddings_base_text_embedding-testImport");
    int baseWriteIdx = stageEvents.indexOf("WriteNodeEmbeddings_base_text_embedding-testImport");
    assertTrue(baseDelIdx >= 0 && baseDelIdx < baseWriteIdx);

    int typeDelIdx = stageEvents.indexOf("WriteDeletedEmbeddings_type_embedding-testImport");
    int typeWriteIdx = stageEvents.indexOf("WriteNodeEmbeddings_type_embedding-testImport");
    assertTrue(typeDelIdx >= 0 && typeDelIdx < typeWriteIdx);
  }

  @Test
  public void testAttachTransforms_skipsDeleteWhenRequested() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Topic", List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);
    EmbeddingConfig config = new EmbeddingConfig(List.of(spec));

    NodeSubGraph topic =
        GraphIngestionUtil.subjectNodeSubGraph("dc/topic/Health", "Health", List.of("Topic"));
    SpannerClient spannerClient =
        new MockSpannerClient(Map.of("dc/base/Test", List.of("LegacyNode")));

    SpannerEmbeddingPipeline.attachTransforms(
        pipeline,
        spannerClient,
        "testImport",
        "dc/base/Test",
        config,
        new MockVertexAIClient(),
        "NodeEmbedding",
        10,
        pipeline.apply("Graphs", Create.of(topic)),
        null,
        true);

    pipeline.run().waitUntilFinish();

    List<Mutation> mutations = MockSpannerClient.getCapturedMutations();
    assertEquals(1, mutations.size());
    assertEquals(Mutation.Op.INSERT_OR_UPDATE, mutations.get(0).getOperation());
    assertTrue(
        MockSpannerClient.getCapturedStageEvents().stream()
            .noneMatch(e -> e.startsWith("WriteDeletedEmbeddings_")));
  }

  @Test
  public void testAttachTransforms_multipleImportsUseUniqueTransformNames() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of("Topic", List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);
    EmbeddingConfig config = new EmbeddingConfig(List.of(spec));

    NodeSubGraph topicA =
        GraphIngestionUtil.subjectNodeSubGraph("dc/topic/Health", "Health", List.of("Topic"));
    NodeSubGraph topicB =
        GraphIngestionUtil.subjectNodeSubGraph("dc/topic/Economy", "Economy", List.of("Topic"));

    SpannerClient spannerClient =
        new MockSpannerClient(
            Map.of("dc/base/ImportA", List.of("OldA"), "dc/base/ImportB", List.of("OldB")));
    VertexAiEmbeddingClient embeddingClient = new MockVertexAIClient();

    SpannerEmbeddingPipeline.attachTransforms(
        pipeline,
        spannerClient,
        "importA",
        "dc/base/ImportA",
        config,
        embeddingClient,
        "NodeEmbedding",
        10,
        pipeline.apply("GraphsA", Create.of(topicA)),
        null,
        false);
    SpannerEmbeddingPipeline.attachTransforms(
        pipeline,
        spannerClient,
        "importB",
        "dc/base/ImportB",
        config,
        embeddingClient,
        "NodeEmbedding",
        10,
        pipeline.apply("GraphsB", Create.of(topicB)),
        null,
        false);

    pipeline.run().waitUntilFinish();

    List<String> stageEvents = MockSpannerClient.getCapturedStageEvents();
    assertTrue(stageEvents.contains("WriteDeletedEmbeddings_base_text_embedding-importA"));
    assertTrue(stageEvents.contains("WriteDeletedEmbeddings_base_text_embedding-importB"));
    assertTrue(stageEvents.contains("WriteNodeEmbeddings_base_text_embedding-importA"));
    assertTrue(stageEvents.contains("WriteNodeEmbeddings_base_text_embedding-importB"));

    Set<String> upsertedSubjects =
        MockSpannerClient.getCapturedMutations().stream()
            .filter(m -> m.getOperation() == Mutation.Op.INSERT_OR_UPDATE)
            .map(m -> m.asMap().get("subject_id").getString())
            .collect(Collectors.toSet());
    assertEquals(Set.of("dc/topic/Health", "dc/topic/Economy"), upsertedSubjects);
  }
}
