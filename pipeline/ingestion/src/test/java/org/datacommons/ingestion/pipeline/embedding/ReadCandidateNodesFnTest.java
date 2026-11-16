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
import org.datacommons.util.McfUtil;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link ReadCandidateNodesFn}. */
@RunWith(JUnit4.class)
public class ReadCandidateNodesFnTest {

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

  private static final McfGraph TOPIC_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "dc/topic/Health",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "dc/topic/Health"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Health"))
                  .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "Topic"))
                  .build())
          .build();

  private static final McfGraph CLASS_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "Farm",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "Farm"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "Farm"))
                  .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "Class"))
                  .build())
          .build();

  private static final McfGraph PLACE_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "country/USA",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "country/USA"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "United States"))
                  .putPvs("typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "Country"))
                  .build())
          .build();

  private static final McfGraph EMPTY_NAME_GRAPH =
      McfGraph.newBuilder()
          .putNodes(
              "Count_Empty",
              PropertyValues.newBuilder()
                  .putPvs("dcid", McfUtil.newValues(ValueType.TEXT, "Count_Empty"))
                  .putPvs("name", McfUtil.newValues(ValueType.TEXT, "   "))
                  .putPvs(
                      "typeOf", McfUtil.newValues(ValueType.RESOLVED_REF, "StatisticalVariable"))
                  .build())
          .build();

  @Rule public final transient TestPipeline pipeline = TestPipeline.create();

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

    PCollection<McfGraph> graphs =
        pipeline.apply(
            "CreateGraphs",
            Create.of(
                STAT_VAR_GRAPH,
                STAT_VAR_GRAPH, // duplicate across imports
                TOPIC_GRAPH,
                PLACE_GRAPH,
                EMPTY_NAME_GRAPH));

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

    List<McfGraph> extracted =
        ReadCandidateNodesFn.extractCandidateGraphs(STAT_VAR_GRAPH, targetTypes);
    assertEquals(1, extracted.size());
    assertEquals("Count_Person", ReadCandidateNodesFn.getSubjectId(extracted.get(0)));

    assertEquals(1, ReadCandidateNodesFn.extractCandidateGraphs(CLASS_GRAPH, targetTypes).size());

    // Non-matching type returns empty
    assertTrue(ReadCandidateNodesFn.extractCandidateGraphs(PLACE_GRAPH, targetTypes).isEmpty());

    // Empty name returns empty
    assertTrue(
        ReadCandidateNodesFn.extractCandidateGraphs(EMPTY_NAME_GRAPH, targetTypes).isEmpty());
  }
}
