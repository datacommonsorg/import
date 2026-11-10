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

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.datacommons.ingestion.pipeline.embedding.EmbeddingSpec.NodeFilterType;
import org.datacommons.proto.Mcf.McfGraph;
import org.datacommons.proto.Mcf.McfGraph.PropertyValues;
import org.datacommons.proto.Mcf.ValueType;
import org.datacommons.util.GraphUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadCandidateNodesFn}. */
@RunWith(JUnit4.class)
public class ReadCandidateNodesFnTest {

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

  private static McfGraph buildMcfNode(String subjectId, String name, String type) {
    PropertyValues.Builder pvs = PropertyValues.newBuilder();
    GraphUtils.setPropVal("dcid", ValueType.TEXT, subjectId, pvs);
    if (name != null) {
      GraphUtils.setPropVal("name", ValueType.TEXT, name, pvs);
    }
    if (type != null) {
      GraphUtils.setPropVal("typeOf", ValueType.RESOLVED_REF, type, pvs);
    }
    return McfGraph.newBuilder().putNodes(subjectId, pvs.build()).build();
  }

  @Test
  public void testReadCandidateNodes_filtersAndCombinesGraphs() {
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

    McfGraph statVarGraph = buildMcfNode("Count_Person", "Total Population", "StatisticalVariable");
    McfGraph topicGraph = buildMcfNode("dc/topic/Health", "Health", "Topic");
    McfGraph nonMatchingTypeGraph = buildMcfNode("country/USA", "United States", "Country");
    McfGraph emptyNameGraph = buildMcfNode("Count_EmptyName", "", "StatisticalVariable");

    PCollection<McfGraph> graphs =
        pipeline.apply(
            "CreateGraphs",
            Create.of(
                statVarGraph,
                statVarGraph, // duplicate across imports
                topicGraph,
                nonMatchingTypeGraph,
                emptyNameGraph));

    PCollection<String> candidateIds =
        ReadCandidateNodesFn.readCandidateNodes(graphs, spec)
            .apply(
                "ExtractIds",
                MapElements.into(TypeDescriptors.strings())
                    .via(ReadCandidateNodesFn::getSubjectId));

    PAssert.that(candidateIds).containsInAnyOrder("Count_Person", "dc/topic/Health");
    pipeline.run();
  }

  @Test
  public void testExtractCandidateGraphs_handlesValidAndInvalidNodes() {
    Set<String> targetTypes = Set.of("StatisticalVariable", "Topic", "Class");

    McfGraph validGraph = buildMcfNode("Count_Person", "Total Population", "StatisticalVariable");
    List<McfGraph> extracted = ReadCandidateNodesFn.extractCandidateGraphs(validGraph, targetTypes);
    assertEquals(1, extracted.size());
    assertEquals("Count_Person", ReadCandidateNodesFn.getSubjectId(extracted.get(0)));

    McfGraph classGraph = buildMcfNode("Farm", "Farm", "Class");
    assertEquals(1, ReadCandidateNodesFn.extractCandidateGraphs(classGraph, targetTypes).size());

    // Non-matching type returns empty
    McfGraph placeGraph = buildMcfNode("country/USA", "United States", "Country");
    assertTrue(ReadCandidateNodesFn.extractCandidateGraphs(placeGraph, targetTypes).isEmpty());

    // Empty name returns empty
    McfGraph emptyNameGraph = buildMcfNode("Count_Empty", "   ", "StatisticalVariable");
    assertTrue(ReadCandidateNodesFn.extractCandidateGraphs(emptyNameGraph, targetTypes).isEmpty());
  }
}
