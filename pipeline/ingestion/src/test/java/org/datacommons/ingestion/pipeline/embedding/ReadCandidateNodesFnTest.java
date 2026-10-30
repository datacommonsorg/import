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

import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
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
  public void testFilterByEntityTypes_withPlaceTypeOrder() {
    // Candidate Class nodes:
    // 1. "Farm": returned by the entity_type Spanner query
    // 2. "Country": included via PLACE_TYPE_ORDER even if not returned by entity_type query
    // 3. "UnusedClass": neither in entityTypes nor in PLACE_TYPE_ORDER -> filtered out
    NodeRecord farmClassNode = new NodeRecord("Farm", "", null, "Farm", List.of("Class"));
    NodeRecord countryClassNode = new NodeRecord("Country", "", null, "Country", List.of("Class"));
    NodeRecord unusedClassNode =
        new NodeRecord("UnusedClass", "", null, "Unused Class", List.of("Class"));

    PCollection<NodeRecord> candidateNodes =
        pipeline.apply("CreateNodes", Create.of(farmClassNode, countryClassNode, unusedClassNode));
    PCollection<String> entityTypes = pipeline.apply("CreateEntityTypes", Create.of("Farm"));

    PCollection<NodeRecord> filtered =
        ReadCandidateNodesFn.filterByEntityTypes(candidateNodes, entityTypes, "type_embedding");

    PAssert.that(filtered).containsInAnyOrder(farmClassNode, countryClassNode);
    pipeline.run();
  }

  @Test
  public void testFilterByEntityTypes_deduplicatesAndIgnoresEmptyTypes() {
    NodeRecord farmClassNode = new NodeRecord("Farm", "", null, "Farm", List.of("Class"));
    NodeRecord countryClassNode = new NodeRecord("Country", "", null, "Country", List.of("Class"));

    PCollection<NodeRecord> candidateNodes =
        pipeline.apply("CreateNodes", Create.of(farmClassNode, countryClassNode));
    // Contains duplicate "Farm", "Country" (which overlaps PLACE_TYPE_ORDER), and empty string ""
    PCollection<String> entityTypes =
        pipeline.apply("CreateEntityTypes", Create.of("Farm", "Farm", "Country", ""));

    PCollection<NodeRecord> filtered =
        ReadCandidateNodesFn.filterByEntityTypes(candidateNodes, entityTypes, "type_embedding");

    PAssert.that(filtered).containsInAnyOrder(farmClassNode, countryClassNode);
    pipeline.run();
  }

  @Test
  public void testEntityTypesQueryContainsExpectedClauses() {
    String query = ReadCandidateNodesFn.ENTITY_TYPES_QUERY;
    assertTrue(query.contains("WITH all_type_table AS"));
    assertTrue(query.contains("Edge@{FORCE_INDEX=InEdge}"));
    assertTrue(query.contains("TimeSeries@{FORCE_INDEX=TimeSeriesByEntity1}"));
    assertTrue(query.contains("ORDER BY"));
  }

  @Test
  public void testPlaceTypeOrderContainsCorePlaceTypes() {
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("Continent"));
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("Country"));
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("State"));
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("County"));
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("City"));
    assertTrue(ReadCandidateNodesFn.PLACE_TYPE_ORDER.contains("Place"));
  }
}
