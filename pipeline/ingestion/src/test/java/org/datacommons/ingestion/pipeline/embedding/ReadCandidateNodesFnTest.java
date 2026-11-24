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
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.pipeline.GraphIngestionUtil;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadCandidateNodesFn}. */
@RunWith(JUnit4.class)
public class ReadCandidateNodesFnTest {

  private static final NodeSubGraph STAT_VAR_NODE =
      GraphIngestionUtil.subjectNodeSubGraph(
          "Count_Person",
          "Total Population",
          List.of("StatisticalVariable"),
          List.of(
              Edge.builder()
                  .subjectId("Count_Person")
                  .predicate("name")
                  .objectId("Total Population:hash")
                  .provenance("dc/base/ImportA")
                  .build()));

  private static final NodeSubGraph TOPIC_NODE =
      GraphIngestionUtil.subjectNodeSubGraph("dc/topic/Health", "Health", List.of("Topic"));

  private static final NodeSubGraph CLASS_NODE =
      GraphIngestionUtil.subjectNodeSubGraph("Farm", "Farm", List.of("Class"));

  private static final NodeSubGraph PLACE_NODE =
      GraphIngestionUtil.subjectNodeSubGraph("country/USA", "United States", List.of("Country"));

  private static final NodeSubGraph EMPTY_NAME_NODE =
      GraphIngestionUtil.subjectNodeSubGraph("Count_Empty", "   ", List.of("StatisticalVariable"));

  private static final NodeSubGraph LEAF_VALUE_NODE =
      NodeSubGraph.builder()
          .node(Node.builder().subjectId("Total Population:hash").value("Total Population").build())
          .build();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReadCandidateNodes_filtersNodeSubGraphs() {
    EmbeddingSpec spec =
        new EmbeddingSpec(
            "base_text_embedding",
            "NodeEmbeddingModel",
            "text-embedding-005",
            "RETRIEVAL_QUERY",
            Map.of(
                "StatisticalVariable", List.of("subject_id", "name"),
                "Topic", List.of("subject_id", "name")),
            NodeFilterType.NO_FILTER);

    PCollection<NodeSubGraph> nodeSubGraphs =
        pipeline.apply(
            "CreateNodeSubGraphs",
            Create.of(STAT_VAR_NODE, TOPIC_NODE, PLACE_NODE, EMPTY_NAME_NODE, LEAF_VALUE_NODE));

    PCollection<String> candidateIds =
        ReadCandidateNodesFn.readCandidateNodes("testImport", nodeSubGraphs, spec)
            .apply(
                "ExtractIds",
                MapElements.into(TypeDescriptors.strings())
                    .via(ReadCandidateNodesFn::getSubjectId));

    PAssert.that(candidateIds).containsInAnyOrder("Count_Person", "dc/topic/Health");
    pipeline.run();
  }

  @Test
  public void testIsCandidate_handlesValidAndInvalidNodes() {
    Set<String> targetTypes = Set.of("StatisticalVariable", "Topic", "Class");

    assertTrue(ReadCandidateNodesFn.isCandidate(STAT_VAR_NODE, targetTypes));
    assertTrue(ReadCandidateNodesFn.isCandidate(CLASS_NODE, targetTypes));
    assertEquals("Count_Person", ReadCandidateNodesFn.getSubjectId(STAT_VAR_NODE));

    // Non-matching type, empty name, and leaf value nodes are skipped.
    assertFalse(ReadCandidateNodesFn.isCandidate(PLACE_NODE, targetTypes));
    assertFalse(ReadCandidateNodesFn.isCandidate(EMPTY_NAME_NODE, targetTypes));
    assertFalse(ReadCandidateNodesFn.isCandidate(LEAF_VALUE_NODE, targetTypes));
    assertFalse(ReadCandidateNodesFn.isCandidate(null, targetTypes));
  }
}
