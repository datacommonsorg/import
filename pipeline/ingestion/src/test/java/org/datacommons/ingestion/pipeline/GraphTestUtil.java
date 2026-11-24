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

package org.datacommons.ingestion.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.gson.JsonParser;
import java.util.List;
import org.datacommons.ingestion.data.Edge;
import org.datacommons.ingestion.data.Node;
import org.datacommons.ingestion.data.NodeSubGraph;
import org.datacommons.ingestion.pipeline.embedding.GenerateEmbeddingContentFn;
import org.datacommons.ingestion.util.PipelineUtils;

/** Test helpers for building fixtures and verifying Spanner outputs in ingestion pipeline tests. */
public final class GraphTestUtil {
  private GraphTestUtil() {}

  public static NodeSubGraph subjectNodeSubGraph(
      String subjectId, String name, List<String> types) {
    return subjectNodeSubGraph(subjectId, name, types, List.of());
  }

  public static NodeSubGraph subjectNodeSubGraph(
      String subjectId, String name, List<String> types, List<Edge> edges) {
    return NodeSubGraph.builder()
        .node(Node.builder().subjectId(subjectId).value(subjectId).name(name).types(types).build())
        .edges(edges)
        .build();
  }

  public static void verifyNodeEmbedding(
      DatabaseClient dbClient,
      String subjectId,
      String expectedLabel,
      String expectedContentJson,
      List<String> expectedNodeTypes,
      List<Double> expectedEmbeddings) {
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT embedding_content_key, embedding_content, node_types, embeddings"
                            + " FROM NodeEmbedding WHERE subject_id = @subject_id"
                            + " AND embedding_label = @embedding_label")
                    .bind("subject_id")
                    .to(subjectId)
                    .bind("embedding_label")
                    .to(expectedLabel)
                    .build())) {
      assertTrue(
          String.format("NodeEmbedding (%s, %s) should exist", subjectId, expectedLabel),
          resultSet.next());
      assertEquals(
          GenerateEmbeddingContentFn.computeFarmFingerprintKey(expectedContentJson),
          resultSet.getString("embedding_content_key"));
      assertEquals(
          JsonParser.parseString(expectedContentJson),
          JsonParser.parseString(resultSet.getJson("embedding_content")));
      assertEquals(expectedNodeTypes, resultSet.getStringList("node_types"));
      assertEquals(expectedEmbeddings, resultSet.getDoubleList("embeddings"));
      assertFalse(
          String.format(
              "Should only have one NodeEmbedding row for (%s, %s)", subjectId, expectedLabel),
          resultSet.next());
    }
  }

  public static void verifySpannerOutputs(
      DatabaseClient dbClient,
      String importName,
      String expectedNodeId,
      String expectedNodeName,
      String expectedNodeType,
      String expectedEntity1,
      String expectedExtraEntitiesId) {
    // Verify Node
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder("SELECT * FROM Node WHERE subject_id = @subject_id")
                    .bind("subject_id")
                    .to(expectedNodeId)
                    .build())) {
      boolean found = false;
      while (resultSet.next()) {
        String subjectId = resultSet.getString("subject_id");
        String name = resultSet.getString("name");
        if (expectedNodeId.equals(subjectId) || ("dcid:" + expectedNodeId).equals(subjectId)) {
          found = true;
          assertTrue("Name should match", expectedNodeName.equals(name));
        }
      }
      assertTrue("Node should exist in Spanner", found);
    }

    // Verify Edges
    String expectedObjectId = PipelineUtils.generateObjectValueKey(expectedNodeName);

    // Verify 'name' edge
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Edge WHERE subject_id = @subject_id AND predicate = @predicate")
                    .bind("subject_id")
                    .to(expectedNodeId)
                    .bind("predicate")
                    .to("name")
                    .build())) {
      assertTrue("Name edge should exist", resultSet.next());
      assertEquals(
          "Name edge object_id should match", expectedObjectId, resultSet.getString("object_id"));
      assertEquals(
          "Name edge provenance should match",
          "dc/base/" + importName,
          resultSet.getString("provenance"));
      assertFalse("Should only have one name edge", resultSet.next());
    }

    // Verify 'typeOf' edge
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Edge WHERE subject_id = @subject_id AND predicate = @predicate")
                    .bind("subject_id")
                    .to(expectedNodeId)
                    .bind("predicate")
                    .to("typeOf")
                    .build())) {
      assertTrue("TypeOf edge should exist", resultSet.next());
      assertEquals(
          "TypeOf edge object_id should match", expectedNodeType, resultSet.getString("object_id"));
      assertEquals(
          "TypeOf edge provenance should match",
          "dc/base/" + importName,
          resultSet.getString("provenance"));
      assertFalse("Should only have one typeOf edge", resultSet.next());
    }

    // Verify TimeSeries
    String variableMeasured = null;
    String entity1 = null;
    String extraEntitiesId = null;
    String facetId = null;
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM TimeSeries WHERE variable_measured LIKE '%TestVariable' AND provenance = @provenance")
                    .bind("provenance")
                    .to("dc/base/" + importName)
                    .build())) {
      assertTrue("TimeSeries should exist", resultSet.next());
      variableMeasured = resultSet.getString("variable_measured");
      entity1 = resultSet.getString("entity1");
      extraEntitiesId = resultSet.getString("extra_entities_id");
      facetId = resultSet.getString("facet_id");
      assertEquals("entity1 should match", expectedEntity1, entity1);
      assertEquals("extraEntitiesId should match", expectedExtraEntitiesId, extraEntitiesId);

      // Verify facet JSON contains unit and measurement method
      String facet = resultSet.getJson("facet");
      assertTrue("Unit should be found in attributes: " + facet, facet.contains("TestUnit"));
      assertTrue(
          "Measurement method should be found in attributes: " + facet,
          facet.contains("TestMethod"));

      assertFalse("Should only have ONE TimeSeries row (deduplication check)", resultSet.next());
    }

    // Verify Observation
    try (ResultSet resultSet =
        dbClient
            .singleUse()
            .executeQuery(
                Statement.newBuilder(
                        "SELECT * FROM Observation WHERE variable_measured = @variable_measured AND entity1 = @entity1 AND extra_entities_id = @extra_entities_id AND facet_id = @facet_id ORDER BY date ASC")
                    .bind("variable_measured")
                    .to(variableMeasured)
                    .bind("entity1")
                    .to(entity1)
                    .bind("extra_entities_id")
                    .to(extraEntitiesId)
                    .bind("facet_id")
                    .to(facetId)
                    .build())) {
      // First Observation (2023)
      assertTrue("First Observation should exist", resultSet.next());
      assertEquals("First Observation date should match", "2023", resultSet.getString("date"));
      String val1 = resultSet.getString("value");
      assertTrue("First Observation value should match", "10".equals(val1) || "10.0".equals(val1));

      // Second Observation (2024)
      assertTrue("Second Observation should exist", resultSet.next());
      assertEquals("Second Observation date should match", "2024", resultSet.getString("date"));
      String val2 = resultSet.getString("value");
      assertTrue("Second Observation value should match", "20".equals(val2) || "20.0".equals(val2));

      assertFalse("Should only have TWO Observation rows", resultSet.next());
    }
  }
}
