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

import com.google.cloud.spanner.Mutation;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.pipeline.GraphTestUtil;
import org.datacommons.ingestion.pipeline.MockVertexAIClient;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GenerateEmbeddingMutations}. */
@RunWith(JUnit4.class)
public class GenerateEmbeddingMutationsTest implements Serializable {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Before
  public void setUp() {
    MockVertexAIClient.clearCapturedCalls();
  }

  @Test
  public void testBuildEmbeddingMutations_buildsPerSpecMutations() {
    EmbeddingConfig config = EmbeddingConfig.load("embedding/default.json");

    NodeSubGraph statVar =
        GraphTestUtil.subjectNodeSubGraph(
            "Count_Person", "Total Population", List.of("StatisticalVariable"));
    NodeSubGraph farmClass =
        GraphTestUtil.subjectNodeSubGraph(
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

    List<PCollection<Mutation>> perSpecMutations =
        GenerateEmbeddingMutations.buildEmbeddingMutations(
            "testImport", config, new MockVertexAIClient(), "NodeEmbedding", 10, graphs);

    assertEquals(2, perSpecMutations.size());
    PAssert.thatSingleton(perSpecMutations.get(0))
        .satisfies(
            m -> {
              assertEquals(Mutation.Op.INSERT_OR_UPDATE, m.getOperation());
              assertEquals("NodeEmbedding", m.getTable());
              assertEquals("base_text_embedding", m.asMap().get("embedding_label").getString());
              assertEquals("Count_Person", m.asMap().get("subject_id").getString());
              assertEquals(
                  "{\"name\":\"Total Population\",\"title\":\"Count_Person\"}",
                  m.asMap().get("embedding_content").getString());
              return null;
            });
    PAssert.thatSingleton(perSpecMutations.get(1))
        .satisfies(
            m -> {
              assertEquals(Mutation.Op.INSERT_OR_UPDATE, m.getOperation());
              assertEquals("NodeEmbedding", m.getTable());
              assertEquals("type_embedding", m.asMap().get("embedding_label").getString());
              assertEquals("Farm", m.asMap().get("subject_id").getString());
              assertEquals(
                  "{\"name\":\"Farm\",\"description\":\"Agricultural land.\"}",
                  m.asMap().get("embedding_content").getString());
              return null;
            });

    pipeline.run().waitUntilFinish();

    Set<String> predictedTexts =
        MockVertexAIClient.getCapturedCalls().stream()
            .flatMap(List::stream)
            .collect(Collectors.toSet());
    assertEquals(
        Set.of(
            "{\"name\":\"Total Population\",\"title\":\"Count_Person\"}",
            "{\"name\":\"Farm\",\"description\":\"Agricultural land.\"}"),
        predictedTexts);
  }

  @Test
  public void testBuildEmbeddingMutations_multipleImportsUseUniqueTransformNames() {
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
        GraphTestUtil.subjectNodeSubGraph("dc/topic/Health", "Health", List.of("Topic"));
    NodeSubGraph topicB =
        GraphTestUtil.subjectNodeSubGraph("dc/topic/Economy", "Economy", List.of("Topic"));

    VertexAiEmbeddingClient embeddingClient = new MockVertexAIClient();

    List<PCollection<Mutation>> mutationsA =
        GenerateEmbeddingMutations.buildEmbeddingMutations(
            "importA",
            config,
            embeddingClient,
            "NodeEmbedding",
            10,
            pipeline.apply("GraphsA", Create.of(topicA)));
    List<PCollection<Mutation>> mutationsB =
        GenerateEmbeddingMutations.buildEmbeddingMutations(
            "importB",
            config,
            embeddingClient,
            "NodeEmbedding",
            10,
            pipeline.apply("GraphsB", Create.of(topicB)));

    assertEquals(1, mutationsA.size());
    assertEquals(1, mutationsB.size());
    PAssert.thatSingleton(mutationsA.get(0))
        .satisfies(
            m -> {
              assertEquals("dc/topic/Health", m.asMap().get("subject_id").getString());
              return null;
            });
    PAssert.thatSingleton(mutationsB.get(0))
        .satisfies(
            m -> {
              assertEquals("dc/topic/Economy", m.asMap().get("subject_id").getString());
              return null;
            });

    pipeline.run().waitUntilFinish();
  }
}
