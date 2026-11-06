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
import static org.junit.Assert.assertNull;

import com.google.cloud.spanner.Mutation;
import com.google.cloud.spanner.Value;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.ingestion.spanner.model.NodeRecord;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadCandidateNodesFn}. */
@RunWith(JUnit4.class)
public class ReadCandidateNodesFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  @Test
  public void testReadCandidateNodes_filtersAndDeduplicatesMutations() {
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

    NodeRecord statVarNode =
        new NodeRecord(
            "Count_Person",
            "Count_Person",
            null,
            "Total Population",
            List.of("StatisticalVariable"));
    NodeRecord topicNode =
        new NodeRecord("dc/topic/Health", "dc/topic/Health", null, "Health", List.of("Topic"));
    NodeRecord nonMatchingTypeNode =
        new NodeRecord("country/USA", "country/USA", null, "United States", List.of("Country"));
    NodeRecord emptyNameNode =
        new NodeRecord(
            "Count_EmptyName", "Count_EmptyName", null, "", List.of("StatisticalVariable"));
    Mutation provisionalMutation =
        Mutation.newInsertOrUpdateBuilder("Node")
            .set(NodeRecord.COL_SUBJECT_ID)
            .to("Provisional_1")
            .set(NodeRecord.COL_LAST_UPDATE_TIMESTAMP)
            .to(Value.COMMIT_TIMESTAMP)
            .build();

    PCollection<Mutation> nodeMutations =
        pipeline.apply(
            "CreateNodeMutations",
            Create.of(
                statVarNode.toMutation("Node"),
                statVarNode.toMutation("Node"), // duplicate across imports
                topicNode.toMutation("Node"),
                nonMatchingTypeNode.toMutation("Node"),
                emptyNameNode.toMutation("Node"),
                provisionalMutation));

    PCollection<NodeRecord> candidates =
        ReadCandidateNodesFn.readCandidateNodes(nodeMutations, spec);

    PAssert.that(candidates).containsInAnyOrder(statVarNode, topicNode);
    pipeline.run();
  }

  @Test
  public void testExtractCandidateNode_handlesValidAndInvalidMutations() {
    Set<String> targetTypes = Set.of("StatisticalVariable", "Topic", "Class");

    NodeRecord validNode =
        new NodeRecord(
            "Count_Person",
            "Count_Person",
            null,
            "Total Population",
            List.of("StatisticalVariable"));
    assertEquals(
        validNode,
        ReadCandidateNodesFn.extractCandidateNode(validNode.toMutation("Node"), targetTypes));

    NodeRecord classNode = new NodeRecord("Farm", "Farm", null, "Farm", List.of("Class"));
    assertEquals(
        classNode,
        ReadCandidateNodesFn.extractCandidateNode(classNode.toMutation("Node"), targetTypes));

    // Non-matching type returns null
    NodeRecord placeNode =
        new NodeRecord("country/USA", "country/USA", null, "United States", List.of("Country"));
    assertNull(
        ReadCandidateNodesFn.extractCandidateNode(placeNode.toMutation("Node"), targetTypes));

    // Empty name returns null
    NodeRecord emptyNameNode =
        new NodeRecord("Count_Empty", "Count_Empty", null, "   ", List.of("StatisticalVariable"));
    assertNull(
        ReadCandidateNodesFn.extractCandidateNode(emptyNameNode.toMutation("Node"), targetTypes));

    // Provisional node mutation (missing name and types) returns null
    Mutation provisionalMutation =
        Mutation.newInsertOrUpdateBuilder("Node")
            .set(NodeRecord.COL_SUBJECT_ID)
            .to("Provisional_1")
            .set(NodeRecord.COL_LAST_UPDATE_TIMESTAMP)
            .to(Value.COMMIT_TIMESTAMP)
            .build();
    assertNull(ReadCandidateNodesFn.extractCandidateNode(provisionalMutation, targetTypes));
  }
}
